from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any

import httpx
import websockets

from scalping_bot.data.book import BookGap, LocalOrderBook
from scalping_bot.data.capture import CaptureHealth, RawSegmentWriter
from scalping_bot.data.storage import ObjectStore

PRODUCTION_PUBLIC_REST = "https://api.binance.com"
PRODUCTION_PUBLIC_STREAM = (
    "wss://stream.binance.com:9443/stream?streams="
    "btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth@100ms"
)


class BinancePublicCollector:
    """Credential-free production market-data collector; it has no trading methods."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        clock_ms: Callable[[], int] | None = None,
        segment_event_limit: int = 100_000,
    ) -> None:
        self.store = store
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.segment_event_limit = segment_event_limit
        self.health = CaptureHealth()
        self.book = LocalOrderBook("BTCUSDT")
        self._writers: dict[str, RawSegmentWriter] = {}
        self._counts: dict[str, int] = {}

    async def run_forever(self) -> None:
        delay = 1.0
        while True:
            try:
                await self._capture_connection()
                delay = 1.0
            except asyncio.CancelledError:
                self._close_all(valid=self.book.valid)
                raise
            except Exception:
                self.health.record_reconnect()
                self._close_all(valid=False)
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
            async for message in socket:
                await self.handle_message(json.loads(message), receipt_time_ms=self.clock_ms())

    async def _snapshot(self) -> dict[str, Any]:
        async with httpx.AsyncClient(base_url=PRODUCTION_PUBLIC_REST, timeout=10) as client:
            response = await client.get(
                "/api/v3/depth", params={"symbol": "BTCUSDT", "limit": 5000}
            )
            response.raise_for_status()
            return dict(response.json())

    async def handle_message(self, envelope: dict[str, Any], *, receipt_time_ms: int) -> None:
        stream = str(envelope["stream"])
        data = dict(envelope["data"])
        event_time = int(data.get("E", receipt_time_ms))
        self.health.record_event(exchange_time_ms=event_time, receipt_time_ms=receipt_time_ms)
        raw = {
            "event_type": stream.split("@", 1)[1],
            "exchange_time_ms": event_time,
            "receipt_time_ms": receipt_time_ms,
            "payload": data,
        }
        writer = self._writer(stream, receipt_time_ms)
        writer.append(raw)
        self._counts[stream] += 1
        if "@depth" in stream:
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
                self._rotate(stream, valid=False)
                raise
        if self._counts[stream] >= self.segment_event_limit:
            self._rotate(stream, valid=self.book.valid if "@depth" in stream else True)

    def _writer(self, stream: str, now_ms: int) -> RawSegmentWriter:
        if stream not in self._writers:
            self._writers[stream] = RawSegmentWriter(
                self.store, stream=stream.replace("@", "-"), segment_started_ms=now_ms
            )
            self._counts[stream] = 0
        return self._writers[stream]

    def _rotate(self, stream: str, *, valid: bool) -> None:
        writer = self._writers.pop(stream, None)
        self._counts.pop(stream, None)
        if writer is not None:
            writer.close(valid=valid)

    def _close_all(self, *, valid: bool) -> None:
        for stream in tuple(self._writers):
            self._rotate(stream, valid=valid if "@depth" in stream else True)
