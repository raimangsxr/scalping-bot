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
    def valid_quote_segments(self) -> tuple[RawSegment, ...]:
        return tuple(
            item.segment
            for item in self.entries
            if item.segment.valid and "bookTicker" in item.segment.stream
        )

    @property
    def market_calibration_sample(self) -> tuple[RawSegment, ...]:
        """Return valid Trade Flow segments eligible for cost calibration."""
        return self.valid_trade_segments

    @property
    def invalid_segments(self) -> tuple[RawSegment, ...]:
        return tuple(item.segment for item in self.entries if not item.segment.valid)


@dataclass(frozen=True)
class CaptureCohort:
    """Content-addressed selection of immutable capture segments.

    Cohorts are the seam between continuously arriving capture and prospective
    analysis.  Invalid, gapped, or out-of-window segments stay visible in the
    exclusion inventory and can never be silently interpolated into a study.
    """

    purpose: str
    streams: tuple[str, ...]
    start_ms: int
    end_ms: int
    segments: tuple[RawSegment, ...]
    excluded: tuple[str, ...] = ()
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not self.purpose.strip() or not self.streams:
            raise ValueError("capture cohort requires a purpose and streams")
        if tuple(sorted(set(self.streams))) != self.streams:
            raise ValueError("capture cohort streams must be sorted and unique")
        if self.start_ms < 0 or self.end_ms < self.start_ms:
            raise ValueError("capture cohort window must be ordered")
        if any(
            not segment.valid
            or segment.stream not in self.streams
            or segment.started_ms < self.start_ms
            or segment.ended_ms > self.end_ms
            for segment in self.segments
        ):
            raise ValueError("capture cohort contains an invalid or out-of-window segment")
        if (
            tuple(sorted(self.segments, key=lambda segment: (segment.stream, segment.started_ms)))
            != self.segments
        ):
            raise ValueError("capture cohort segments must be sorted")

    @property
    def segment_keys(self) -> tuple[str, ...]:
        return tuple(segment.manifest_key for segment in self.segments)

    @property
    def coverage_ms(self) -> int:
        return sum(max(0, segment.ended_ms - segment.started_ms) for segment in self.segments)

    def canonical_bytes(self) -> bytes:
        value = {
            "schema_version": self.schema_version,
            "purpose": self.purpose,
            "streams": list(self.streams),
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "segments": [asdict(segment) for segment in self.segments],
            "excluded": list(self.excluded),
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def persist(self, store: ObjectStore) -> str:
        key = f"capture-cohorts/{self.purpose}/{self.identity}.json"
        store.put_immutable(key, self.canonical_bytes())
        return key

    def verify(self, store: ObjectStore) -> None:
        for segment in self.segments:
            try:
                manifest = json.loads(store.read(segment.manifest_key))
                raw = store.read(segment.object_key)
            except (KeyError, OSError) as error:
                raise ValueError(
                    f"capture cohort segment is unavailable: {segment.manifest_key}"
                ) from error
            if not isinstance(manifest, dict) or manifest.get("sha256") != segment.sha256:
                raise ValueError(f"capture cohort manifest differs: {segment.manifest_key}")
            if hashlib.sha256(raw).hexdigest() != segment.sha256:
                raise ValueError(f"capture cohort segment checksum differs: {segment.object_key}")

    @classmethod
    def load(cls, store: ObjectStore, key: str) -> CaptureCohort:
        raw = store.read(key)
        expected = key.rsplit("/", 1)[-1].removesuffix(".json")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("capture cohort checksum does not match its key")
        value = json.loads(raw)
        if not isinstance(value, dict) or not isinstance(value.get("segments"), list):
            raise ValueError("capture cohort is malformed")
        segments = tuple(_segment(item) for item in value["segments"] if isinstance(item, dict))
        if len(segments) != len(value["segments"]):
            raise ValueError("capture cohort segment metadata is malformed")
        cohort = cls(
            purpose=str(value["purpose"]),
            streams=tuple(str(item) for item in value["streams"]),
            start_ms=int(str(value["start_ms"])),
            end_ms=int(str(value["end_ms"])),
            segments=segments,
            excluded=tuple(str(item) for item in value.get("excluded", [])),
            schema_version=int(str(value.get("schema_version", 1))),
        )
        if cohort.identity != expected:
            raise ValueError("capture cohort identity is invalid")
        return cohort


def freeze_capture_cohort(
    store: ObjectStore,
    *,
    purpose: str,
    streams: tuple[str, ...],
    start_ms: int,
    end_ms: int,
) -> CaptureCohort:
    """Freeze a reproducible valid-segment cohort from the append-only ledgers."""
    normalized_streams = tuple(sorted(set(streams)))
    if not normalized_streams:
        raise ValueError("capture cohort requires at least one stream")
    ledger = CaptureLedger(store)
    selected: list[RawSegment] = []
    excluded: list[str] = []
    for stream in normalized_streams:
        catalog = ledger.catalog(stream)
        for entry in catalog.entries:
            segment = entry.segment
            if not segment.valid:
                excluded.append(
                    f"{segment.manifest_key}:invalid:{segment.invalid_reason or 'unspecified'}"
                )
            elif segment.started_ms < start_ms or segment.ended_ms > end_ms:
                excluded.append(f"{segment.manifest_key}:outside-window")
            else:
                selected.append(segment)
    cohort = CaptureCohort(
        purpose=purpose,
        streams=normalized_streams,
        start_ms=start_ms,
        end_ms=end_ms,
        segments=tuple(sorted(selected, key=lambda segment: (segment.stream, segment.started_ms))),
        excluded=tuple(sorted(excluded)),
    )
    cohort.verify(store)
    return cohort


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
        # The payload checksum alone is not a segment identity: two reconnect
        # boundaries can legitimately contain identical bytes at different
        # times.  Include the immutable start boundary to avoid catalog key
        # collisions while retaining the payload checksum for integrity.
        key = f"catalog/live/{segment.stream}/{segment.started_ms}-{segment.sha256}.json"
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
        self._invalid_events = 0

    def record_event(self, *, exchange_time_ms: int, receipt_time_ms: int) -> None:
        self._last_receipt_ms = receipt_time_ms
        self._clock_drift_ms = receipt_time_ms - exchange_time_ms

    def record_gap(self) -> None:
        self._gaps += 1

    def record_reconnect(self) -> None:
        self._reconnects += 1

    def record_storage_failure(self) -> None:
        self._storage_failures += 1

    def record_invalid_event(self) -> None:
        self._invalid_events += 1

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
            and freshness >= 0
            and freshness <= stale_after_ms
            and clock_drift is not None
            and abs(clock_drift) <= max_clock_drift_ms
            and snapshot["gaps"] == 0
            and snapshot["storage_failures"] == 0
            and self._invalid_events == 0
        )
        return {**snapshot, "invalid_events": self._invalid_events, "healthy": healthy}


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
