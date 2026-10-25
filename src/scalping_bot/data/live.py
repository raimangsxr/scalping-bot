from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import websockets

from scalping_bot.data.book import BookGap, LocalOrderBook
from scalping_bot.data.capture import CaptureHealth, CaptureLedger, RawSegmentWriter
from scalping_bot.data.storage import ObjectStore
from scalping_bot.operations.observability import OperationalMetrics

PRODUCTION_PUBLIC_REST = "https://api.binance.com"
PRODUCTION_PUBLIC_STREAM = (
    "wss://stream.binance.com:9443/stream?streams="
    "btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth@100ms"
)


def _validate_market_payload(stream: str, data: dict[str, Any]) -> None:
    event_type = stream.split("@", 1)[-1]
    expected_type = "depthUpdate" if event_type == "depth@100ms" else event_type
    if not isinstance(data.get("e"), str) or data["e"] != expected_type:
        raise ValueError("event type does not match stream")
    _positive_integer(data.get("E"), "event time")
    if event_type == "aggTrade":
        _positive_integer(data.get("a"), "aggregate trade id")
        _positive_decimal(data.get("p"), "trade price")
        _positive_decimal(data.get("q"), "trade quantity")
        if not isinstance(data.get("m"), bool):
            raise ValueError("trade maker flag is malformed")
        return
    if event_type == "bookTicker":
        _positive_integer(data.get("u"), "book ticker update id")
        bid = _positive_decimal(data.get("b"), "best bid")
        ask = _positive_decimal(data.get("a"), "best ask")
        if bid > ask:
            raise ValueError("best bid exceeds best ask")
        return
    if event_type == "depth@100ms":
        first = _positive_integer(data.get("U"), "depth first update id")
        final = _positive_integer(data.get("u"), "depth final update id")
        if first > final:
            raise ValueError("depth update range is inverted")
        _levels_are_valid(data.get("b"), "depth bids")
        _levels_are_valid(data.get("a"), "depth asks")
        return
    raise ValueError("unsupported public stream")


def _positive_integer(value: object, label: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} is malformed") from error
    if parsed < 0:
        raise ValueError(f"{label} is negative")
    return parsed


def _positive_decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"{label} is malformed") from error
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{label} is not positive")
    return parsed


def _levels_are_valid(value: object, label: str) -> None:
    if not isinstance(value, list):
        raise ValueError(f"{label} are malformed")
    for level in value:
        if not isinstance(level, list) or len(level) != 2:
            raise ValueError(f"{label} contain a malformed level")
        _positive_decimal(level[0], f"{label} price")
        try:
            quantity = Decimal(str(level[1]))
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ValueError(f"{label} quantity is malformed") from error
        if not quantity.is_finite() or quantity < 0:
            raise ValueError(f"{label} quantity is invalid")


class BinancePublicCollector:
    """Credential-free production market-data collector; it has no trading methods."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        clock_ms: Callable[[], int] | None = None,
        segment_event_limit: int = 100_000,
        metrics: OperationalMetrics | None = None,
    ) -> None:
        self.store = store
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.segment_event_limit = segment_event_limit
        self.metrics = metrics
        self.health = CaptureHealth()
        self.ledger = CaptureLedger(store)
        self.book = LocalOrderBook("BTCUSDT")
        self._writers: dict[str, RawSegmentWriter] = {}
        self._counts: dict[str, int] = {}
        self._gap_pending: dict[str, int] = {}
        self._last_health_publish_ms: int | None = None

    async def run_forever(self) -> None:
        delay = 1.0
        while True:
            try:
                await self._capture_connection()
                delay = 1.0
            except asyncio.CancelledError:
                self._close_all(valid=self.book.valid, invalid_reason="collector cancelled")
                raise
            except Exception:
                self.health.record_reconnect()
                self._close_all(valid=False, invalid_reason="connection or sequence failure")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def _capture_connection(self) -> None:
        async with websockets.connect(PRODUCTION_PUBLIC_STREAM, ping_interval=20) as socket:
            snapshot = await self._snapshot()
            now = self.clock_ms()
            self.book.apply_snapshot(
                last_update_id=int(snapshot["lastUpdateId"]),
                bids=tuple(tuple(level) for level in snapshot["bids"]),
                asks=tuple(tuple(level) for level in snapshot["asks"]),
                exchange_time_ms=now,
                receipt_time_ms=now,
            )
            snapshot_writer = self._writer("btcusdt@depth@100ms", now)
            snapshot_writer.append(
                {
                    "event_type": "depth-snapshot",
                    "exchange_time_ms": now,
                    "receipt_time_ms": now,
                    "payload": {
                        "lastUpdateId": int(snapshot["lastUpdateId"]),
                        "bids": snapshot["bids"],
                        "asks": snapshot["asks"],
                    },
                }
            )
            self._counts["btcusdt@depth@100ms"] += 1
            self._publish_health(now)
            async for message in socket:
                await self.handle_message(json.loads(message), receipt_time_ms=self.clock_ms())
            self._close_all(valid=False, invalid_reason="market-data connection closed")

    async def _snapshot(self) -> dict[str, Any]:
        async with httpx.AsyncClient(base_url=PRODUCTION_PUBLIC_REST, timeout=10) as client:
            response = await client.get(
                "/api/v3/depth", params={"symbol": "BTCUSDT", "limit": 5000}
            )
            response.raise_for_status()
            return dict(response.json())

    async def handle_message(self, envelope: dict[str, Any], *, receipt_time_ms: int) -> None:
        raw_stream = envelope.get("stream")
        stream = raw_stream if isinstance(raw_stream, str) else "unknown"
        raw_data = envelope.get("data")
        data = raw_data if isinstance(raw_data, dict) else {}
        try:
            event_time = int(data.get("E", receipt_time_ms))
        except (TypeError, ValueError):
            event_time = receipt_time_ms
        event_type = stream.split("@", 1)[1] if "@" in stream else stream
        raw = {
            "event_type": event_type,
            "exchange_time_ms": event_time,
            "receipt_time_ms": receipt_time_ms,
            "payload": data,
        }
        try:
            _validate_market_payload(stream, data)
        except ValueError as error:
            self.health.record_invalid_event()
            self._record_invalid(stream, raw, receipt_time_ms, f"invalid payload: {error}")
            self._publish_health(receipt_time_ms)
            return
        self.health.record_event(exchange_time_ms=event_time, receipt_time_ms=receipt_time_ms)
        self._publish_health(receipt_time_ms)
        if "@depth" in stream:
            if not self.book.valid:
                self.health.record_invalid_event()
                self._record_invalid(
                    stream, raw, receipt_time_ms, "order-book is awaiting a fresh snapshot"
                )
                self._publish_health(receipt_time_ms)
                return
            try:
                self.book.apply_depth(
                    first_update_id=int(data["U"]),
                    final_update_id=int(data["u"]),
                    bids=tuple(tuple(level) for level in data["b"]),
                    asks=tuple(tuple(level) for level in data["a"]),
                    exchange_time_ms=event_time,
                    receipt_time_ms=receipt_time_ms,
                )
            except BookGap:
                self.health.record_gap()
                self._rotate(stream, valid=True, invalid_reason=None)
                self._record_invalid(stream, raw, receipt_time_ms, "order-book sequence gap")
                self._publish_health(receipt_time_ms)
                return
        writer = self._writer(stream, receipt_time_ms)
        try:
            writer.append(raw)
        except (OSError, RuntimeError):
            self.health.record_storage_failure()
            self._rotate(stream, valid=False, invalid_reason="storage failure")
            raise
        self._counts[stream] += 1
        if self._counts[stream] >= self.segment_event_limit:
            self._rotate(
                stream,
                valid=self.book.valid if "@depth" in stream else True,
                invalid_reason=None,
            )

    def _record_invalid(
        self, stream: str, raw: dict[str, object], now_ms: int, reason: str
    ) -> None:
        if stream in self._writers:
            self._rotate(stream, valid=True, invalid_reason=None)
        writer = self._writer(stream, now_ms)
        writer.append(raw)
        self._counts[stream] += 1
        self._rotate(stream, valid=False, invalid_reason=reason)

    def _writer(self, stream: str, now_ms: int) -> RawSegmentWriter:
        if stream not in self._writers:
            self._writers[stream] = RawSegmentWriter(
                self.store,
                stream=stream.replace("@", "-"),
                segment_started_ms=now_ms,
                gap_before_ms=self._gap_pending.pop(stream, None),
            )
            self._counts[stream] = 0
        return self._writers[stream]

    def _publish_health(self, now_ms: int) -> None:
        if (
            self._last_health_publish_ms is not None
            and now_ms - self._last_health_publish_ms < 5_000
        ):
            return
        status = self.health.status(now_ms=now_ms)
        if self.metrics is not None:
            freshness = status.get("freshness_ms")
            if isinstance(freshness, int):
                self.metrics.market_data_freshness_ms.set(freshness)
            gaps = status.get("gaps", 0)
            storage_failures = status.get("storage_failures", 0)
            self.metrics.market_data_gaps_total.set(
                float(gaps) if isinstance(gaps, (int, float)) else 0
            )
            self.metrics.storage_health.set(float(not bool(storage_failures)))
            self.metrics.heartbeat_timestamp_seconds.set(now_ms / 1000)
            self.metrics.operating_mode.set(1 if bool(status.get("healthy")) else 2)
        payload = {
            "schema_version": 1,
            "recorded_ms": now_ms,
            "status": status,
            "stream_counts": dict(sorted(self._counts.items())),
        }
        content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        identity = hashlib.sha256(content).hexdigest()
        self.store.put_immutable(f"health/collector/{now_ms}-{identity}.json", content)
        self._last_health_publish_ms = now_ms

    def _rotate(self, stream: str, *, valid: bool, invalid_reason: str | None) -> None:
        writer = self._writers.pop(stream, None)
        self._counts.pop(stream, None)
        if writer is not None:
            segment = writer.close(valid=valid, invalid_reason=invalid_reason)
            self.ledger.record(segment, health=self.health.snapshot(now_ms=segment.ended_ms))
        if not valid:
            self._gap_pending[stream] = self.clock_ms()

    def _close_all(self, *, valid: bool, invalid_reason: str | None) -> None:
        for stream in tuple(self._writers):
            self._rotate(
                stream,
                valid=valid,
                invalid_reason=invalid_reason,
            )
