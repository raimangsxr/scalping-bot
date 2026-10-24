from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable, Iterator
from decimal import Decimal
from urllib.parse import urlparse

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import (
    CandleClosed,
    EngineEvent,
    EntryFillUpdated,
    ExitFillReceived,
    FillReceived,
    MarketTick,
)
from scalping_bot.domain.models import ExitMarket, PlaceProtection, SubmitEntry
from scalping_bot.execution.executor import ExecutionService, OrderResult
from scalping_bot.execution.sqlite_state import SQLiteState


class BinanceTestnetMarketStream:
    def __init__(self, endpoint: str) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme != "wss" or parsed.hostname != "stream.testnet.binance.vision":
            raise ValueError("only the Binance Spot Testnet market stream is allowed")
        self.url = (
            "wss://stream.testnet.binance.vision/stream?streams=btcusdt@trade/btcusdt@kline_1m"
        )

    def events(
        self, stop: threading.Event, *, on_reconnect: Callable[[], None] | None = None
    ) -> Iterator[EngineEvent]:
        connected_once = False
        while not stop.is_set():
            try:
                with connect(self.url, open_timeout=10, close_timeout=5) as websocket:
                    if connected_once and on_reconnect is not None:
                        on_reconnect()
                    connected_once = True
                    while not stop.is_set():
                        try:
                            message = websocket.recv(timeout=1)
                        except TimeoutError:
                            continue
                        if not isinstance(message, str):
                            continue
                        event = _parse_market_message(message)
                        if event is not None:
                            yield event
            except (ConnectionClosed, OSError):
                if not stop.wait(1):
                    continue


class LiveEngineRunner:
    """The single domain engine seam used by live Testnet and replay adapters."""

    def __init__(
        self, *, engine: Engine, execution: ExecutionService, durable_state: SQLiteState
    ) -> None:
        self.engine = engine
        self.execution = execution
        self.durable_state = durable_state

    def process(self, event: EngineEvent) -> None:
        if isinstance(event, MarketTick):
            for refresh_command, order in self.execution.refresh_partial_entries(
                event_time_ms=event.event_time_ms
            ):
                aggregate = _aggregate_fills(order)
                if aggregate is None:
                    continue
                price, quantity, commission, commission_asset = aggregate
                self.process(
                    EntryFillUpdated(
                        event_id=f"partial-{refresh_command.client_order_id}-{quantity}",
                        event_time_ms=event.event_time_ms,
                        client_order_id=refresh_command.client_order_id,
                        price=price,
                        cumulative_quantity=quantity,
                        cumulative_commission=commission,
                        commission_asset=commission_asset,
                    )
                )
            pending_exit = self.engine.state.pending_exit
            if pending_exit is not None:
                order = self.execution.submit_exit(pending_exit, event_time_ms=event.event_time_ms)
                exit_fill = _exit_fill_event(
                    pending_exit.client_order_id, event.event_time_ms, order
                )
                if exit_fill is not None:
                    self.process(exit_fill)
        result = self.engine.handle(event)
        for decision in result.decisions:
            self.durable_state.append_audit(
                kind=decision.kind,
                event_time_ms=decision.event_time_ms,
                details=dict(decision.details),
            )
        for command in result.commands:
            if isinstance(command, SubmitEntry):
                order = self.execution.submit_entry(command, event_time_ms=event.event_time_ms)
                entry_fill = _entry_fill_event(command.client_order_id, event.event_time_ms, order)
                if entry_fill is not None:
                    self.process(entry_fill)
            elif isinstance(command, ExitMarket):
                order = self.execution.submit_exit(command, event_time_ms=event.event_time_ms)
                exit_fill = _exit_fill_event(command.client_order_id, event.event_time_ms, order)
                if exit_fill is not None:
                    self.process(exit_fill)
            elif isinstance(command, PlaceProtection):
                self.execution.place_protection(command, event_time_ms=event.event_time_ms)
            else:
                raise TypeError(f"unsupported engine command: {type(command).__name__}")
        self.durable_state.persist_engine_state(
            self.engine.state, event_time_ms=event.event_time_ms
        )


def _parse_market_message(message: str) -> EngineEvent | None:
    envelope = json.loads(message)
    if not isinstance(envelope, dict):
        raise ValueError("Binance market message is malformed")
    payload = envelope.get("data", envelope)
    if not isinstance(payload, dict):
        raise ValueError("Binance market payload is malformed")
    event_type = payload.get("e")
    if event_type == "trade":
        event_time = int(payload["T"])
        trade_id = int(payload["t"])
        return MarketTick(
            event_id=f"trade-{trade_id}",
            event_time_ms=event_time,
            price=Decimal(str(payload["p"])),
        )
    if event_type == "kline":
        kline = payload.get("k")
        if not isinstance(kline, dict) or not bool(kline.get("x")):
            return None
        sequence = int(kline["t"])
        return CandleClosed(
            event_id=f"kline-{sequence}",
            event_time_ms=int(kline["T"]),
            sequence=sequence,
            open=Decimal(str(kline["o"])),
            high=Decimal(str(kline["h"])),
            low=Decimal(str(kline["l"])),
            close=Decimal(str(kline["c"])),
            complete=True,
            volume=Decimal(str(kline["v"])),
            buy_volume=Decimal(str(kline["V"])),
            sell_volume=Decimal(str(kline["v"])) - Decimal(str(kline["V"])),
            trade_count=int(kline["n"]),
        )
    return None


def _entry_fill_event(
    client_order_id: str, event_time_ms: int, result: OrderResult
) -> FillReceived | None:
    aggregate = _aggregate_fills(result)
    if aggregate is None:
        return None
    price, quantity, commission, commission_asset = aggregate
    identity = hashlib.sha256(
        f"entry:{client_order_id}:{result.exchange_order_id}".encode()
    ).hexdigest()[:24]
    return FillReceived(
        event_id=f"fill-{identity}",
        event_time_ms=event_time_ms,
        client_order_id=client_order_id,
        price=price,
        quantity=quantity,
        commission=commission,
        commission_asset=commission_asset,
    )


def _exit_fill_event(
    client_order_id: str, event_time_ms: int, result: OrderResult
) -> ExitFillReceived | None:
    if result.status != "FILLED":
        return None
    aggregate = _aggregate_fills(result)
    if aggregate is None:
        return None
    price, quantity, commission, _commission_asset = aggregate
    identity = hashlib.sha256(
        f"exit:{client_order_id}:{result.exchange_order_id}".encode()
    ).hexdigest()[:24]
    return ExitFillReceived(
        event_id=f"fill-{identity}",
        event_time_ms=event_time_ms,
        client_order_id=client_order_id,
        price=price,
        quantity=quantity,
        commission=commission,
    )


def _aggregate_fills(
    result: OrderResult,
) -> tuple[Decimal, Decimal, Decimal, str] | None:
    if not result.fills:
        return None
    quantity = sum((fill.quantity for fill in result.fills), Decimal(0))
    if quantity <= 0:
        return None
    notional = sum((fill.price * fill.quantity for fill in result.fills), Decimal(0))
    commission_assets = {fill.commission_asset for fill in result.fills}
    if len(commission_assets) != 1:
        raise ValueError("a single order returned commissions in multiple assets")
    commission = sum((fill.commission for fill in result.fills), Decimal(0))
    return notional / quantity, quantity, commission, commission_assets.pop()
