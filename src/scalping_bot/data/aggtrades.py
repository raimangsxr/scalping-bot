from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from scalping_bot.data.archive import ArchiveIntegrityError
from scalping_bot.data.storage import ObjectStore


@dataclass(frozen=True)
class AggTradeManifest:
    source_uri: str
    source_sha256: str
    raw_key: str
    parquet_key: str
    row_count: int
    first_aggregate_trade_id: int
    last_aggregate_trade_id: int
    start_time_ms: int
    end_time_ms: int
    schema_version: int
    manifest_key: str


class AggTradeArchivePipeline:
    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def ingest_file(
        self, archive_path: Path, *, expected_sha256: str, source_uri: str
    ) -> AggTradeManifest:
        return self.ingest_bytes(
            archive_path.read_bytes(), expected_sha256=expected_sha256, source_uri=source_uri
        )

    def ingest_bytes(
        self, archive: bytes, *, expected_sha256: str, source_uri: str
    ) -> AggTradeManifest:
        checksum = hashlib.sha256(archive).hexdigest()
        if checksum.lower() != expected_sha256.lower():
            raise ArchiveIntegrityError("aggregate-trade checksum mismatch")
        raw_key = f"raw/binance/aggTrades/BTCUSDT/{checksum}.zip"
        self.store.put_immutable(raw_key, archive)
        rows = self._rows(archive)
        parquet_key = f"derived/aggTrades/symbol=BTCUSDT/{checksum}.parquet"
        sink = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows), sink, compression="zstd")
        self.store.put_immutable(parquet_key, sink.getvalue())
        first_id = cast(int, rows[0]["aggregate_trade_id"])
        last_id = cast(int, rows[-1]["aggregate_trade_id"])
        start_time = cast(int, rows[0]["time_ms"])
        end_time = cast(int, rows[-1]["time_ms"])
        provisional = {
            "source_uri": source_uri,
            "source_sha256": checksum,
            "raw_key": raw_key,
            "parquet_key": parquet_key,
            "row_count": len(rows),
            "first_aggregate_trade_id": first_id,
            "last_aggregate_trade_id": last_id,
            "start_time_ms": start_time,
            "end_time_ms": end_time,
            "schema_version": 1,
        }
        identity = hashlib.sha256(
            json.dumps(provisional, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        manifest = AggTradeManifest(
            source_uri=source_uri,
            source_sha256=checksum,
            raw_key=raw_key,
            parquet_key=parquet_key,
            row_count=len(rows),
            first_aggregate_trade_id=first_id,
            last_aggregate_trade_id=last_id,
            start_time_ms=start_time,
            end_time_ms=end_time,
            schema_version=1,
            manifest_key=f"manifests/{identity}.json",
        )
        self.store.put_immutable(
            manifest.manifest_key,
            (json.dumps(asdict(manifest), sort_keys=True, separators=(",", ":")) + "\n").encode(),
        )
        return manifest

    @staticmethod
    def _rows(archive: bytes) -> list[dict[str, object]]:
        try:
            with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
                names = [name for name in bundle.namelist() if name.endswith(".csv")]
                if len(names) != 1:
                    raise ArchiveIntegrityError("archive must contain one aggregate-trade CSV")
                raw_rows = list(
                    csv.reader(io.TextIOWrapper(bundle.open(names[0]), encoding="utf-8"))
                )
        except zipfile.BadZipFile as error:
            raise ArchiveIntegrityError("source is not a valid ZIP archive") from error
        if not raw_rows:
            raise ArchiveIntegrityError("aggregate-trade archive contains no rows")
        rows: list[dict[str, object]] = []
        previous_id: int | None = None
        previous_time: int | None = None
        for index, raw in enumerate(raw_rows, start=1):
            if len(raw) != 8:
                raise ArchiveIntegrityError(f"aggregate-trade schema mismatch on row {index}")
            try:
                aggregate_id = int(raw[0])
                price = Decimal(raw[1])
                quantity = Decimal(raw[2])
                first_trade_id, last_trade_id = int(raw[3]), int(raw[4])
                timestamp = int(raw[5])
                time_ms = timestamp // 1000 if timestamp >= 1_000_000_000_000_000 else timestamp
                buyer_maker = _boolean(raw[6])
                best_match = _boolean(raw[7])
            except (InvalidOperation, ValueError) as error:
                raise ArchiveIntegrityError(
                    f"aggregate-trade value invalid on row {index}"
                ) from error
            if timestamp < 1_000_000_000_000:
                raise ArchiveIntegrityError("aggregate-trade timestamp unit is unsupported")
            if previous_id is not None and aggregate_id <= previous_id:
                raise ArchiveIntegrityError("aggregate-trade IDs are duplicate or out of order")
            if previous_time is not None and time_ms < previous_time:
                raise ArchiveIntegrityError("aggregate trades are not chronological")
            rows.append(
                {
                    "symbol": "BTCUSDT",
                    "aggregate_trade_id": aggregate_id,
                    "price": str(price),
                    "quantity": str(quantity),
                    "first_trade_id": first_trade_id,
                    "last_trade_id": last_trade_id,
                    "time_ms": time_ms,
                    "buyer_maker": buyer_maker,
                    "best_match": best_match,
                }
            )
            previous_id, previous_time = aggregate_id, time_ms
        return rows


def _boolean(value: str) -> bool:
    if value.lower() not in {"true", "false"}:
        raise ValueError(f"invalid boolean: {value}")
    return value.lower() == "true"
