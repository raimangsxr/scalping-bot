from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TypedDict

import pyarrow as pa
import pyarrow.parquet as pq

from scalping_bot.data.storage import ObjectStore

KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",
)


class ArchiveIntegrityError(ValueError):
    pass


class KlineRow(TypedDict):
    symbol: str
    interval: str
    open_time_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time_ms: int
    quote_volume: str
    trade_count: int
    taker_buy_base: str
    taker_buy_quote: str


@dataclass(frozen=True)
class DatasetManifest:
    schema_version: int
    source_uri: str
    source_sha256: str
    raw_key: str
    parquet_keys: tuple[str, ...]
    row_count: int
    start_time_ms: int
    end_time_ms: int
    timestamp_unit: str
    transformations: tuple[str, ...]
    manifest_key: str

    def canonical_bytes(self, *, include_key: bool = True) -> bytes:
        values = asdict(self)
        if not include_key:
            values.pop("manifest_key")
        return (json.dumps(values, sort_keys=True, separators=(",", ":")) + "\n").encode()


class BinanceArchivePipeline:
    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def ingest_file(
        self, archive_path: Path, *, expected_sha256: str, source_uri: str
    ) -> DatasetManifest:
        archive = archive_path.read_bytes()
        actual_sha256 = hashlib.sha256(archive).hexdigest()
        if actual_sha256.lower() != expected_sha256.lower():
            raise ArchiveIntegrityError(
                f"checksum mismatch: expected {expected_sha256}, got {actual_sha256}"
            )
        raw_key = f"raw/binance/klines/BTCUSDT/1m/{actual_sha256}.zip"
        self.store.put_immutable(raw_key, archive)
        rows, source_unit = self._validated_rows(archive)
        first = datetime.fromtimestamp(rows[0]["open_time_ms"] / 1000, tz=UTC)
        partition = f"year={first.year}/month={first.month:02d}"
        parquet_key = (
            f"derived/klines/symbol=BTCUSDT/interval=1m/{partition}/{actual_sha256}.parquet"
        )
        sink = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows), sink, compression="zstd")
        self.store.put_immutable(parquet_key, sink.getvalue())
        provisional = DatasetManifest(
            schema_version=1,
            source_uri=source_uri,
            source_sha256=actual_sha256,
            raw_key=raw_key,
            parquet_keys=(parquet_key,),
            row_count=len(rows),
            start_time_ms=int(rows[0]["open_time_ms"]),
            end_time_ms=int(rows[-1]["close_time_ms"]),
            timestamp_unit=source_unit,
            transformations=(
                "verified published SHA-256",
                "validated 12-column Binance kline schema",
                "normalized source timestamps to milliseconds",
                "validated chronological one-minute continuity",
                "wrote partitioned Parquet with Zstandard compression",
            ),
            manifest_key="",
        )
        identity = hashlib.sha256(provisional.canonical_bytes(include_key=False)).hexdigest()
        manifest = DatasetManifest(
            **{**asdict(provisional), "manifest_key": f"manifests/{identity}.json"}
        )
        self.store.put_immutable(manifest.manifest_key, manifest.canonical_bytes())
        return manifest

    @staticmethod
    def _validated_rows(archive: bytes) -> tuple[list[KlineRow], str]:
        try:
            with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
                names = [name for name in bundle.namelist() if name.endswith(".csv")]
                if len(names) != 1:
                    raise ArchiveIntegrityError("archive must contain exactly one CSV")
                text = io.TextIOWrapper(bundle.open(names[0]), encoding="utf-8")
                raw_rows = list(csv.reader(text))
        except zipfile.BadZipFile as error:
            raise ArchiveIntegrityError("source is not a valid ZIP archive") from error
        if not raw_rows:
            raise ArchiveIntegrityError("archive contains no rows")

        parsed: list[KlineRow] = []
        source_units: set[str] = set()
        previous_open: int | None = None
        seen: set[int] = set()
        for index, raw in enumerate(raw_rows, start=1):
            if len(raw) != len(KLINE_COLUMNS):
                raise ArchiveIntegrityError(f"schema mismatch on row {index}")
            try:
                open_time, open_unit = _timestamp_ms(raw[0])
                close_time, close_unit = _timestamp_ms(raw[6])
                prices = [Decimal(value) for value in raw[1:6]]
                quote_volume = Decimal(raw[7])
                trade_count = int(raw[8])
                taker_base = Decimal(raw[9])
                taker_quote = Decimal(raw[10])
            except ArchiveIntegrityError:
                raise
            except (InvalidOperation, ValueError) as error:
                raise ArchiveIntegrityError(f"schema value invalid on row {index}") from error
            source_units.update((open_unit, close_unit))
            if open_time in seen:
                raise ArchiveIntegrityError(f"duplicate open timestamp on row {index}")
            if previous_open is not None:
                if open_time < previous_open:
                    raise ArchiveIntegrityError(f"rows are not chronological at row {index}")
                if open_time - previous_open != 60_000:
                    raise ArchiveIntegrityError(f"one-minute gap before row {index}")
            if close_time < open_time or close_time - open_time >= 60_000:
                raise ArchiveIntegrityError(f"timestamp interval invalid on row {index}")
            seen.add(open_time)
            previous_open = open_time
            parsed.append(
                {
                    "symbol": "BTCUSDT",
                    "interval": "1m",
                    "open_time_ms": open_time,
                    "open": str(prices[0]),
                    "high": str(prices[1]),
                    "low": str(prices[2]),
                    "close": str(prices[3]),
                    "volume": str(prices[4]),
                    "close_time_ms": close_time,
                    "quote_volume": str(quote_volume),
                    "trade_count": trade_count,
                    "taker_buy_base": str(taker_base),
                    "taker_buy_quote": str(taker_quote),
                }
            )
        unit = source_units.pop() if len(source_units) == 1 else "mixed"
        return parsed, unit


def _timestamp_ms(raw: str) -> tuple[int, str]:
    value = int(raw)
    if 1_000_000_000_000 <= value < 10_000_000_000_000:
        return value, "milliseconds"
    if 1_000_000_000_000_000 <= value < 10_000_000_000_000_000:
        return value // 1000, "microseconds"
    raise ArchiveIntegrityError(f"timestamp has unsupported unit: {raw}")
