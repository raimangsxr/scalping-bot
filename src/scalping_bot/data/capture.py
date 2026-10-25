from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from scalping_bot.data.storage import ObjectStore


@dataclass(frozen=True)
class RawSegment:
    stream: str
    started_ms: int
    ended_ms: int
    event_count: int
    valid: bool
    sha256: str
    object_key: str
    manifest_key: str
    gap_before_ms: int | None = None
    invalid_reason: str | None = None


@dataclass(frozen=True)
class CaptureCatalogEntry:
    segment: RawSegment
    health: dict[str, object]
    catalog_key: str


@dataclass(frozen=True)
class CaptureCatalog:
    stream: str
    entries: tuple[CaptureCatalogEntry, ...]

    @property
    def completed(self) -> tuple[RawSegment, ...]:
        return tuple(item.segment for item in self.entries)

    @property
    def valid_book_segments(self) -> tuple[RawSegment, ...]:
        return tuple(
            item.segment
            for item in self.entries
            if item.segment.valid and "depth" in item.segment.stream
        )

    @property
    def valid_trade_segments(self) -> tuple[RawSegment, ...]:
        return tuple(
            item.segment
            for item in self.entries
            if item.segment.valid and "aggTrade" in item.segment.stream
        )

    @property
    def market_calibration_sample(self) -> tuple[RawSegment, ...]:
        """Return valid Trade Flow segments eligible for cost calibration."""
        return self.valid_trade_segments

    @property
    def invalid_segments(self) -> tuple[RawSegment, ...]:
        return tuple(item.segment for item in self.entries if not item.segment.valid)


class RawSegmentWriter:
    def __init__(
        self,
        store: ObjectStore,
        *,
        stream: str,
        segment_started_ms: int,
        gap_before_ms: int | None = None,
    ) -> None:
        self._store = store
        self._stream = stream
        self._started_ms = segment_started_ms
        self._events: list[bytes] = []
        self._last_receipt_ms = segment_started_ms
        self._gap_before_ms = gap_before_ms
        self._closed: RawSegment | None = None

    def append(self, event: dict[str, object]) -> None:
        if self._closed is not None:
            raise RuntimeError("raw segment is already closed")
        required = {"event_type", "exchange_time_ms", "receipt_time_ms"}
        if not required <= event.keys():
            raise ValueError(f"raw event is missing {sorted(required - event.keys())}")
        receipt_time = event["receipt_time_ms"]
        if not isinstance(receipt_time, int):
            raise ValueError("receipt_time_ms must be an integer")
        self._last_receipt_ms = receipt_time
        self._events.append(json.dumps(event, sort_keys=True, separators=(",", ":")).encode())

    def close(self, *, valid: bool, invalid_reason: str | None = None) -> RawSegment:
        if self._closed is not None:
            if self._closed.valid != valid:
                raise RuntimeError("closed segment validity is immutable")
            return self._closed
        payload = b"\n".join(self._events) + (b"\n" if self._events else b"")
        compressed = gzip.compress(payload, compresslevel=9, mtime=0)
        checksum = hashlib.sha256(compressed).hexdigest()
        object_key = f"raw/live/{self._stream}/{self._started_ms}-{checksum}.ndjson.gz"
        manifest_key = f"raw/live/{self._stream}/{self._started_ms}-{checksum}.manifest.json"
        segment = RawSegment(
            stream=self._stream,
            started_ms=self._started_ms,
            ended_ms=self._last_receipt_ms,
            event_count=len(self._events),
            valid=valid,
            sha256=checksum,
            object_key=object_key,
            manifest_key=manifest_key,
            gap_before_ms=self._gap_before_ms,
            invalid_reason=invalid_reason,
        )
        self._store.put_immutable(object_key, compressed)
        manifest = (
            json.dumps(asdict(segment), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        self._store.put_immutable(manifest_key, manifest)
        self._closed = segment
        return segment


class CaptureLedger:
    """Append-only catalog for completed live-capture segments.

    Segment objects and catalog entries are content addressed. A restarted
    collector therefore discovers completed work instead of rewriting it or
    pretending that a reconnect was continuous.
    """

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def record(self, segment: RawSegment, *, health: Mapping[str, object]) -> CaptureCatalogEntry:
        value = {"segment": asdict(segment), "health": health}
        content = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        key = f"catalog/live/{segment.stream}/{segment.sha256}.json"
        self.store.put_immutable(key, content)
        return CaptureCatalogEntry(segment=segment, health=dict(health), catalog_key=key)

    def catalog(self, stream: str) -> CaptureCatalog:
        entries: list[CaptureCatalogEntry] = []
        prefix = f"catalog/live/{stream}/"
        for key in self.store.list_keys(prefix):
            if not key.endswith(".json"):
                continue
            raw = json.loads(self.store.read(key))
            if not isinstance(raw, dict) or not isinstance(raw.get("segment"), dict):
                raise ValueError("capture catalog entry is malformed")
            segment = _segment(raw["segment"])
            if segment.stream != stream:
                raise ValueError("capture catalog entry targets another stream")
            health = raw.get("health", {})
            if not isinstance(health, dict):
                raise ValueError("capture health metadata is malformed")
            entries.append(CaptureCatalogEntry(segment, dict(health), key))
        entries.sort(key=lambda item: (item.segment.started_ms, item.segment.sha256))
        return CaptureCatalog(stream=stream, entries=tuple(entries))


class CaptureHealth:
    def __init__(self) -> None:
        self._last_receipt_ms: int | None = None
        self._clock_drift_ms = 0
        self._gaps = 0
        self._reconnects = 0
        self._storage_failures = 0

    def record_event(self, *, exchange_time_ms: int, receipt_time_ms: int) -> None:
        self._last_receipt_ms = receipt_time_ms
        self._clock_drift_ms = receipt_time_ms - exchange_time_ms

    def record_gap(self) -> None:
        self._gaps += 1

    def record_reconnect(self) -> None:
        self._reconnects += 1

    def record_storage_failure(self) -> None:
        self._storage_failures += 1

    def snapshot(self, *, now_ms: int) -> dict[str, int | None]:
        freshness = None if self._last_receipt_ms is None else now_ms - self._last_receipt_ms
        return {
            "freshness_ms": freshness,
            "clock_drift_ms": self._clock_drift_ms,
            "gaps": self._gaps,
            "reconnects": self._reconnects,
            "storage_failures": self._storage_failures,
        }

    def status(
        self,
        *,
        now_ms: int,
        stale_after_ms: int = 5_000,
        max_clock_drift_ms: int = 1_000,
    ) -> dict[str, object]:
        snapshot = self.snapshot(now_ms=now_ms)
        freshness = snapshot["freshness_ms"]
        clock_drift = snapshot["clock_drift_ms"]
        healthy = (
            freshness is not None
            and freshness <= stale_after_ms
            and clock_drift is not None
            and abs(clock_drift) <= max_clock_drift_ms
            and snapshot["storage_failures"] == 0
        )
        return {**snapshot, "healthy": healthy}


def _segment(value: dict[str, Any]) -> RawSegment:
    try:
        return RawSegment(
            stream=str(value["stream"]),
            started_ms=int(str(value["started_ms"])),
            ended_ms=int(str(value["ended_ms"])),
            event_count=int(str(value["event_count"])),
            valid=bool(value["valid"]),
            sha256=str(value["sha256"]),
            object_key=str(value["object_key"]),
            manifest_key=str(value["manifest_key"]),
            gap_before_ms=(
                int(str(value["gap_before_ms"])) if value.get("gap_before_ms") is not None else None
            ),
            invalid_reason=(str(value["invalid_reason"]) if value.get("invalid_reason") else None),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("capture segment metadata is malformed") from error
