from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import asdict, dataclass

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


class RawSegmentWriter:
    def __init__(self, store: ObjectStore, *, stream: str, segment_started_ms: int) -> None:
        self._store = store
        self._stream = stream
        self._started_ms = segment_started_ms
        self._events: list[bytes] = []
        self._last_receipt_ms = segment_started_ms
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

    def close(self, *, valid: bool) -> RawSegment:
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
        )
        self._store.put_immutable(object_key, compressed)
        manifest = (
            json.dumps(asdict(segment), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        self._store.put_immutable(manifest_key, manifest)
        self._closed = segment
        return segment


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
