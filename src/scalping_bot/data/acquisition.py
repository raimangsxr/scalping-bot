"""Reproducible acquisition and cataloguing of the Research Market archive."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from itertools import pairwise
from urllib.parse import urlparse

import httpx

from scalping_bot.data.aggtrades import AggTradeArchivePipeline, AggTradeManifest
from scalping_bot.data.archive import (
    ArchiveIntegrityError,
    BinanceArchivePipeline,
    DatasetManifest,
)
from scalping_bot.data.storage import ObjectStore


class ArchiveKind(StrEnum):
    KLINES = "klines"
    AGG_TRADES = "aggTrades"


@dataclass(frozen=True)
class ResearchPeriod:
    name: str
    start_month: str
    end_month: str

    @property
    def months(self) -> tuple[str, ...]:
        start_year, start_number = _parse_month(self.start_month)
        end_year, end_number = _parse_month(self.end_month)
        output: list[str] = []
        year, number = start_year, start_number
        while (year, number) <= (end_year, end_number):
            output.append(f"{year:04d}-{number:02d}")
            number += 1
            if number == 13:
                year, number = year + 1, 1
        return tuple(output)


HISTORICAL_STRESS = ResearchPeriod("historical-stress", "2017-08", "2019-12")
PRIMARY_RESEARCH = ResearchPeriod("primary-research", "2020-01", "2025-09")
FINAL_HOLDOUT = ResearchPeriod("final-holdout", "2025-10", "2026-09")
RESEARCH_PERIODS = (HISTORICAL_STRESS, PRIMARY_RESEARCH, FINAL_HOLDOUT)


@dataclass(frozen=True)
class ArchiveRequest:
    kind: ArchiveKind
    period: str
    month: str
    source_uri: str

    def __post_init__(self) -> None:
        _parse_month(self.month)
        if not self.period or not self.source_uri.startswith("https://"):
            raise ValueError("archive requests require a named period and HTTPS source")
        parsed = urlparse(self.source_uri)
        if parsed.hostname != "data.binance.vision":
            raise ValueError("Research Market archives must come from Binance data storage")


@dataclass(frozen=True)
class ArchiveCatalogEntry:
    kind: ArchiveKind
    period: str
    month: str
    source_uri: str
    source_sha256: str
    manifest_key: str
    row_count: int
    start_time_ms: int
    end_time_ms: int
    derived_keys: tuple[str, ...] = ()
    derived_sha256: tuple[str, ...] = ()
    schema_version: int = 1
    transformations: tuple[str, ...] = ()
    first_aggregate_trade_id: int | None = None
    last_aggregate_trade_id: int | None = None


class ArchiveIncompleteError(ValueError):
    pass


@dataclass(frozen=True)
class ResearchArchiveCatalog:
    entries: tuple[ArchiveCatalogEntry, ...] = ()
    missing: tuple[ArchiveRequest, ...] = ()
    gaps: tuple[str, ...] = ()
    catalog_key: str | None = None
    schema_version: int = 1

    def canonical_bytes(self) -> bytes:
        value = {
            "schema_version": self.schema_version,
            "entries": [asdict(item) for item in self.entries],
            "missing": [asdict(item) for item in self.missing],
            "gaps": list(self.gaps),
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def persist(self, store: ObjectStore) -> ResearchArchiveCatalog:
        key = f"catalogs/research-market/{self.sha256}.json"
        store.put_immutable(key, self.canonical_bytes())
        return replace(self, catalog_key=key)

    def require_complete(self) -> None:
        problems = [*self.gaps]
        if self.missing:
            problems.append(f"missing archives: {len(self.missing)}")
        if problems:
            raise ArchiveIncompleteError(
                "Research Market archive is not continuous: " + "; ".join(problems)
            )

    def require_periods(self, periods: Iterable[ResearchPeriod]) -> None:
        """Require that every requested period is represented by this catalog.

        A partial catalog is useful while an acquisition is running, but it must
        never be accidentally consumed as the complete Research Market.  This
        explicit check keeps resumable work separate from a finalized catalog.
        """
        requests = research_archive_requests(periods)
        expected = {(item.kind, item.period, item.month) for item in requests}
        actual = {(item.kind, item.period, item.month) for item in self.entries}
        missing = sorted(expected - actual)
        if missing:
            preview = ", ".join(f"{kind}/{period}/{month}" for kind, period, month in missing[:3])
            suffix = "..." if len(missing) > 3 else ""
            raise ArchiveIncompleteError(
                f"Research Market catalog is missing {len(missing)} requested archives: "
                f"{preview}{suffix}"
            )
        self.require_complete()

    def verify(self, store: ObjectStore) -> None:
        """Verify all raw, manifest, and derived artifacts bound by the catalog."""
        for entry in self.entries:
            try:
                raw = store.read(entry.manifest_key)
            except (KeyError, OSError) as error:
                raise ArchiveIncompleteError(
                    f"catalog manifest is unavailable: {entry.manifest_key}"
                ) from error
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ArchiveIncompleteError(f"catalog manifest is malformed: {entry.manifest_key}")
            if str(value.get("source_sha256")) != entry.source_sha256:
                raise ArchiveIncompleteError(
                    f"catalog manifest source checksum differs: {entry.manifest_key}"
                )
            raw_key = value.get("raw_key")
            if not isinstance(raw_key, str):
                raise ArchiveIncompleteError(
                    f"catalog manifest has no raw object: {entry.manifest_key}"
                )
            try:
                raw_content = store.read(raw_key)
            except (KeyError, OSError) as error:
                raise ArchiveIncompleteError(f"raw archive is unavailable: {raw_key}") from error
            if hashlib.sha256(raw_content).hexdigest() != entry.source_sha256:
                raise ArchiveIncompleteError(f"raw archive checksum differs: {raw_key}")
            if len(entry.derived_sha256) not in {0, len(entry.derived_keys)}:
                raise ArchiveIncompleteError(
                    f"derived checksum count differs: {entry.manifest_key}"
                )
            for index, key in enumerate(entry.derived_keys):
                try:
                    derived = store.read(key)
                except (KeyError, OSError) as error:
                    raise ArchiveIncompleteError(
                        f"derived artifact is unavailable: {key}"
                    ) from error
                if entry.derived_sha256 and (
                    hashlib.sha256(derived).hexdigest() != entry.derived_sha256[index]
                ):
                    raise ArchiveIncompleteError(
                        f"derived artifact checksum differs: {key}"
                    )

    @classmethod
    def load(cls, store: ObjectStore, key: str) -> ResearchArchiveCatalog:
        raw = json.loads(store.read(key))
        if not isinstance(raw, dict):
            raise ValueError("research archive catalog must be a JSON object")
        entries = tuple(
            ArchiveCatalogEntry(
                kind=ArchiveKind(str(item["kind"])),
                period=str(item["period"]),
                month=str(item["month"]),
                source_uri=str(item["source_uri"]),
                source_sha256=str(item["source_sha256"]),
                manifest_key=str(item["manifest_key"]),
                row_count=int(str(item["row_count"])),
                start_time_ms=int(str(item["start_time_ms"])),
                end_time_ms=int(str(item["end_time_ms"])),
                derived_keys=tuple(
                    str(value) for value in _list_of_strings(item.get("derived_keys", []))
                ),
                derived_sha256=tuple(
                    str(value) for value in _list_of_strings(item.get("derived_sha256", []))
                ),
                schema_version=int(str(item.get("schema_version", 1))),
                transformations=tuple(
                    str(value) for value in _list_of_strings(item.get("transformations", []))
                ),
                first_aggregate_trade_id=(
                    int(str(item["first_aggregate_trade_id"]))
                    if item.get("first_aggregate_trade_id") is not None
                    else None
                ),
                last_aggregate_trade_id=(
                    int(str(item["last_aggregate_trade_id"]))
                    if item.get("last_aggregate_trade_id") is not None
                    else None
                ),
            )
            for item in _list_of_dicts(raw.get("entries"))
        )
        missing = tuple(
            ArchiveRequest(
                kind=ArchiveKind(str(item["kind"])),
                period=str(item["period"]),
                month=str(item["month"]),
                source_uri=str(item["source_uri"]),
            )
            for item in _list_of_dicts(raw.get("missing"))
        )
        catalog = cls(
            entries=entries,
            missing=missing,
            gaps=tuple(str(item) for item in _list_of_strings(raw.get("gaps"))),
            catalog_key=key,
            schema_version=int(raw.get("schema_version", 1)),
        )
        if catalog.sha256 != key.rsplit("/", 1)[-1].removesuffix(".json"):
            raise ValueError("research archive catalog checksum does not match its key")
        return catalog


class BinanceArchiveDownloader:
    """Download an official archive and its published SHA-256 sidecar."""

    def __init__(self, *, timeout_seconds: float = 30.0) -> None:
        self.timeout_seconds = timeout_seconds

    def __call__(self, request: ArchiveRequest) -> tuple[bytes, str]:
        with httpx.Client(timeout=self.timeout_seconds, follow_redirects=True) as client:
            response = client.get(request.source_uri)
            response.raise_for_status()
            checksum_response = client.get(request.source_uri + ".CHECKSUM")
            checksum_response.raise_for_status()
        expected = _parse_checksum(checksum_response.text)
        return response.content, expected


class ResearchArchiveMaterializer:
    def __init__(
        self,
        store: ObjectStore,
        *,
        downloader: Callable[[ArchiveRequest], tuple[bytes, str]] | None = None,
    ) -> None:
        self.store = store
        self.downloader = downloader or BinanceArchiveDownloader()

    def materialize(
        self,
        requests: Iterable[ArchiveRequest],
        *,
        existing: ResearchArchiveCatalog | None = None,
        require_complete: bool = False,
    ) -> ResearchArchiveCatalog:
        requested = tuple(requests)
        entries = list(existing.entries if existing is not None else ())
        missing: list[ArchiveRequest] = []
        known = {(item.kind, item.period, item.month) for item in entries}
        for request in requested:
            identity = (request.kind, request.period, request.month)
            if identity in known:
                continue
            try:
                archive, expected_sha256 = self.downloader(request)
            except ArchiveIntegrityError:
                raise
            except Exception:
                missing.append(request)
                continue
            if hashlib.sha256(archive).hexdigest().lower() != expected_sha256.lower():
                raise ArchiveIntegrityError(
                    f"published checksum mismatch for {request.kind}/{request.month}"
                )
            manifest = self._ingest(request, archive, expected_sha256)
            entries.append(_catalog_entry(request, manifest, store=self.store))
            known.add(identity)
        catalog = ResearchArchiveCatalog(
            entries=tuple(sorted(entries, key=_entry_sort_key)),
            missing=tuple(sorted(missing, key=_request_sort_key)),
            gaps=_find_gaps(requested, entries),
        ).persist(self.store)
        if require_complete:
            catalog.require_complete()
            catalog.verify(self.store)
        return catalog

    def _ingest(
        self, request: ArchiveRequest, archive: bytes, expected_sha256: str
    ) -> DatasetManifest | AggTradeManifest:
        if request.kind is ArchiveKind.KLINES:
            return BinanceArchivePipeline(self.store).ingest_bytes(
                archive, expected_sha256=expected_sha256, source_uri=request.source_uri
            )
        return AggTradeArchivePipeline(self.store).ingest_bytes(
            archive, expected_sha256=expected_sha256, source_uri=request.source_uri
        )


def research_archive_requests(
    periods: Iterable[ResearchPeriod] = RESEARCH_PERIODS,
    kinds: Iterable[ArchiveKind] | None = None,
) -> tuple[ArchiveRequest, ...]:
    selected_kinds = tuple(kinds) if kinds is not None else tuple(ArchiveKind)
    if not selected_kinds or any(not isinstance(kind, ArchiveKind) for kind in selected_kinds):
        raise ValueError("archive request kinds must contain at least one known kind")
    requests: list[ArchiveRequest] = []
    for period in periods:
        for month in period.months:
            for kind in selected_kinds:
                requests.append(
                    ArchiveRequest(
                        kind=kind,
                        period=period.name,
                        month=month,
                        source_uri=_source_uri(kind, month),
                    )
                )
    return tuple(requests)


def select_research_periods(names: Iterable[str] | None = None) -> tuple[ResearchPeriod, ...]:
    """Resolve CLI period names to the canonical, ordered protocol periods."""
    if names is None:
        return RESEARCH_PERIODS
    requested = tuple(str(name).strip().lower() for name in names)
    if not requested or any(not name for name in requested):
        raise ValueError("at least one non-empty archive period is required")
    by_name = {period.name: period for period in RESEARCH_PERIODS}
    unknown = sorted(set(requested) - set(by_name))
    if unknown:
        raise ValueError(
            "unknown archive period(s): " + ", ".join(unknown)
        )
    selected = set(requested)
    return tuple(period for period in RESEARCH_PERIODS if period.name in selected)


def select_archive_kinds(names: Iterable[str] | None = None) -> tuple[ArchiveKind, ...]:
    """Resolve human-friendly kind names while preserving protocol order."""
    if names is None:
        return tuple(ArchiveKind)
    aliases = {
        "klines": ArchiveKind.KLINES,
        "kline": ArchiveKind.KLINES,
        "aggtrades": ArchiveKind.AGG_TRADES,
        "agg-trades": ArchiveKind.AGG_TRADES,
        "aggtrade": ArchiveKind.AGG_TRADES,
    }
    requested = tuple(str(name).strip().lower() for name in names)
    if not requested or any(name not in aliases for name in requested):
        unknown = sorted(name for name in requested if name not in aliases)
        raise ValueError("unknown archive kind(s): " + ", ".join(unknown))
    selected = {aliases[name] for name in requested}
    return tuple(kind for kind in ArchiveKind if kind in selected)


def latest_research_catalog(store: ObjectStore) -> ResearchArchiveCatalog | None:
    """Load the most complete resumable catalog in an object store, if present."""
    candidates: list[ResearchArchiveCatalog] = []
    for key in store.list_keys("catalogs/research-market/"):
        if not key.endswith(".json"):
            continue
        try:
            candidates.append(ResearchArchiveCatalog.load(store, key))
        except (KeyError, OSError, ValueError):
            # A malformed old catalog must not prevent a fresh acquisition.  It
            # is still retained as immutable evidence and can be investigated
            # independently.
            continue
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda catalog: (len(catalog.entries), -len(catalog.missing), catalog.sha256),
    )


def _source_uri(kind: ArchiveKind, month: str) -> str:
    if kind is ArchiveKind.KLINES:
        filename = f"BTCUSDT-1m-{month}.zip"
        return f"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/{filename}"
    filename = f"BTCUSDT-aggTrades-{month}.zip"
    return f"https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/{filename}"


def _catalog_entry(
    request: ArchiveRequest,
    manifest: DatasetManifest | AggTradeManifest,
    *,
    store: ObjectStore | None = None,
) -> ArchiveCatalogEntry:
    if isinstance(manifest, DatasetManifest):
        derived_keys = manifest.parquet_keys
        schema_version = manifest.schema_version
        transformations = manifest.transformations
        first_aggregate_trade_id = None
        last_aggregate_trade_id = None
    else:
        derived_keys = (manifest.parquet_key,)
        schema_version = manifest.schema_version
        transformations = ("verified published SHA-256", "validated aggregate-trade schema")
        first_aggregate_trade_id = manifest.first_aggregate_trade_id
        last_aggregate_trade_id = manifest.last_aggregate_trade_id
    derived_sha256 = (
        tuple(hashlib.sha256(store.read(key)).hexdigest() for key in derived_keys)
        if store is not None
        else ()
    )
    return ArchiveCatalogEntry(
        kind=request.kind,
        period=request.period,
        month=request.month,
        source_uri=request.source_uri,
        source_sha256=manifest.source_sha256,
        manifest_key=manifest.manifest_key,
        row_count=manifest.row_count,
        start_time_ms=manifest.start_time_ms,
        end_time_ms=manifest.end_time_ms,
        derived_keys=derived_keys,
        derived_sha256=derived_sha256,
        schema_version=schema_version,
        transformations=transformations,
        first_aggregate_trade_id=first_aggregate_trade_id,
        last_aggregate_trade_id=last_aggregate_trade_id,
    )


def _find_gaps(
    requests: tuple[ArchiveRequest, ...], entries: list[ArchiveCatalogEntry]
) -> tuple[str, ...]:
    expected = {(item.kind, item.period, item.month) for item in requests}
    actual = {(item.kind, item.period, item.month) for item in entries}
    gaps = [f"missing {kind}/{period}/{month}" for kind, period, month in sorted(expected - actual)]
    for kind in ArchiveKind:
        for period in sorted({item.period for item in requests}):
            items = sorted(
                (item for item in entries if item.kind is kind and item.period == period),
                key=lambda item: item.month,
            )
            for previous, current in pairwise(items):
                if (
                    kind is ArchiveKind.KLINES
                    and current.start_time_ms <= previous.end_time_ms
                ):
                    gaps.append(f"overlap between {previous.month} and {current.month}")
                if (
                    kind is ArchiveKind.KLINES
                    and current.start_time_ms > previous.end_time_ms + 60_001
                ):
                    gaps.append(f"gap between {previous.month} and {current.month}")
                if (
                    kind is ArchiveKind.AGG_TRADES
                    and previous.last_aggregate_trade_id is not None
                    and current.first_aggregate_trade_id is not None
                    and current.first_aggregate_trade_id != previous.last_aggregate_trade_id + 1
                ):
                    gaps.append(
                        f"aggregate-trade ID gap between {previous.month} and {current.month}"
                    )
    return tuple(sorted(set(gaps)))


def _parse_month(value: str) -> tuple[int, int]:
    try:
        year, month = (int(item) for item in value.split("-", 1))
    except (TypeError, ValueError):
        raise ValueError(f"invalid archive month: {value}") from None
    if year < 2017 or not 1 <= month <= 12:
        raise ValueError(f"invalid archive month: {value}")
    return year, month


def _parse_checksum(value: str) -> str:
    checksum = value.strip().split()[0] if value.strip() else ""
    if len(checksum) != 64 or any(
        character not in "0123456789abcdefABCDEF" for character in checksum
    ):
        raise ValueError("Binance checksum sidecar is malformed")
    return checksum.lower()


def _entry_sort_key(item: ArchiveCatalogEntry) -> tuple[str, str, str]:
    return item.kind.value, item.period, item.month


def _request_sort_key(item: ArchiveRequest) -> tuple[str, str, str]:
    return item.kind.value, item.period, item.month


def _list_of_dicts(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("catalog list must contain objects")
    return [item for item in value if isinstance(item, dict)]


def _list_of_strings(value: object) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("catalog gaps must contain strings")
    return [item for item in value if isinstance(item, str)]
