"""Durable application of Binance User Data Stream events."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from websockets.sync.client import connect

from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.safety import OperationalMode


@dataclass(frozen=True)
class ExecutionReportEvent:
    event_time_ms: int
    exchange_order_id: str
    client_order_id: str
    status: str
    trade_id: str | None
    last_price: str
    last_quantity: str
    commission: str
    commission_asset: str


@dataclass(frozen=True)
class AccountBalanceEvent:
    event_time_ms: int
    asset: str
    free: str
    locked: str


@dataclass(frozen=True)
class AccountSnapshotEvent:
    event_time_ms: int
    balances: tuple[AccountBalanceEvent, ...]


@dataclass(frozen=True)
class ListStatusEvent:
    event_time_ms: int
    list_id: str
    status: str
    client_order_id: str | None


ExchangeEvent = ExecutionReportEvent | AccountBalanceEvent | AccountSnapshotEvent | ListStatusEvent


class BinanceTestnetUserDataStream:
    """Reconnectable, read-only Binance Spot Testnet User Data Stream."""

    def __init__(
        self,
        endpoint: str,
        listen_key: Callable[[], str],
        *,
        keepalive: Callable[[str], None] | None = None,
        keepalive_interval_seconds: float = 30 * 60,
        retry_seconds: float = 1.0,
    ) -> None:
        from urllib.parse import urlparse

        parsed = urlparse(endpoint)
        if parsed.scheme != "wss" or parsed.hostname != "stream.testnet.binance.vision":
            raise ValueError("only the Binance Spot Testnet User Data Stream is allowed")
        if retry_seconds <= 0 or keepalive_interval_seconds <= 0:
            raise ValueError("User Data Stream timing values must be positive")
        self.endpoint = "wss://stream.testnet.binance.vision"
        self.listen_key = listen_key
        self.keepalive = keepalive
        self.keepalive_interval_seconds = keepalive_interval_seconds
        self.retry_seconds = retry_seconds

    def events(
        self,
        stop: threading.Event,
        *,
        on_disconnect: Callable[[str], None] | None = None,
        on_reconnect: Callable[[], None] | None = None,
    ) -> Iterator[ExchangeEvent]:
        connected_once = False
        while not stop.is_set():
            try:
                key = self.listen_key()
                if not key or any(character.isspace() for character in key):
                    raise ValueError("Binance User Data listen key is malformed")
                with connect(
                    f"{self.endpoint}/ws/{key}", open_timeout=10, close_timeout=5
                ) as websocket:
                    last_keepalive = time.monotonic()
                    if connected_once and on_reconnect is not None:
                        on_reconnect()
                    connected_once = True
                    while not stop.is_set():
                        try:
                            message = websocket.recv(timeout=1)
                        except TimeoutError:
                            if (
                                self.keepalive is not None
                                and time.monotonic() - last_keepalive
                                >= self.keepalive_interval_seconds
                            ):
                                try:
                                    self.keepalive(key)
                                except Exception as error:
                                    raise OSError("Binance User Data keepalive failed") from error
                                last_keepalive = time.monotonic()
                            continue
                        if not isinstance(message, str):
                            continue
                        value = json.loads(message)
                        if not isinstance(value, dict):
                            raise ValueError("Binance User Data message is malformed")
                        yield parse_user_data_message(value)
            except Exception as error:
                if on_disconnect is not None:
                    on_disconnect(type(error).__name__)
                stop.wait(self.retry_seconds)


class ExchangeEventApplier:
    def __init__(self, state: SQLiteState) -> None:
        self.state = state

    def apply(self, event: ExchangeEvent) -> None:
        if isinstance(event, ExecutionReportEvent):
            self._execution_report(event)
        elif isinstance(event, AccountBalanceEvent):
            self.state.upsert_balance(
                asset=event.asset,
                free=event.free,
                locked=event.locked,
                updated_ms=event.event_time_ms,
            )
            self.state.append_audit(
                kind="account-balance-updated",
                event_time_ms=event.event_time_ms,
                details={"asset": event.asset},
            )
        elif isinstance(event, AccountSnapshotEvent):
            for balance in event.balances:
                self.apply(balance)
        else:
            self.state.append_audit(
                kind="protection-list-updated",
                event_time_ms=event.event_time_ms,
                details={
                    "list_id": event.list_id,
                    "status": event.status,
                    "client_order_id": event.client_order_id,
                },
            )

    def stream_lost(self, *, event_time_ms: int, reason: str) -> None:
        self.state.set_operating_mode(OperationalMode.DEGRADED)
        self.state.set_entries_allowed(False)
        self.state.append_audit(
            kind="exchange-stream-lost",
            event_time_ms=event_time_ms,
            details={"reason": reason},
        )

    def stream_reconciled(
        self,
        *,
        event_time_ms: int,
        reconcile: Callable[[], bool],
    ) -> bool:
        degraded = self.state.operating_mode is OperationalMode.DEGRADED
        consistent = reconcile()
        self.state.append_audit(
            kind="exchange-stream-reconciled",
            event_time_ms=event_time_ms,
            details={"consistent": consistent},
        )
        if not consistent:
            self.state.set_operating_mode(OperationalMode.DEGRADED)
        elif degraded:
            self.state.set_operating_mode(OperationalMode.RUNNING)
            self.state.set_entries_allowed(True)
        return consistent

    def _execution_report(self, event: ExecutionReportEvent) -> None:
        try:
            intent = self.state.order_intent(event.client_order_id)
        except LookupError:
            self.state.set_operating_mode(OperationalMode.DEGRADED)
            self.state.append_audit(
                kind="unknown-exchange-order",
                event_time_ms=event.event_time_ms,
                details={
                    "client_order_id": event.client_order_id,
                    "exchange_order_id": event.exchange_order_id,
                },
            )
            return
        quantity = Decimal(event.last_quantity)
        if event.trade_id is not None and quantity > 0:
            self.state.record_fill(
                fill_id=event.trade_id,
                client_order_id=event.client_order_id,
                price=event.last_price,
                quantity=event.last_quantity,
                commission=event.commission,
                commission_asset=event.commission_asset,
                event_time_ms=event.event_time_ms,
            )
        executed = sum(
            (
                Decimal(str(item["quantity"]))
                for item in self.state.fills_for_order(event.client_order_id)
            ),
            Decimal(0),
        )
        self.state.update_order_intent(
            event.client_order_id,
            status=event.status,
            exchange_order_id=event.exchange_order_id,
            executed_quantity=str(executed),
        )
        if intent["side"] == "BUY":
            self.state.rebuild_position_from_order(event.client_order_id)
        elif intent["side"] == "SELL":
            self.state.apply_exit_fills(event.client_order_id, event_time_ms=event.event_time_ms)
        self.state.append_audit(
            kind="exchange-order-updated",
            event_time_ms=event.event_time_ms,
            details={
                "client_order_id": event.client_order_id,
                "status": event.status,
                "executed_quantity": str(executed),
            },
        )


def parse_user_data_message(value: dict[str, Any]) -> ExchangeEvent:
    event_type = value.get("e")
    if event_type == "executionReport":
        return ExecutionReportEvent(
            event_time_ms=int(value["E"]),
            exchange_order_id=str(value["i"]),
            client_order_id=str(value["c"]),
            status=str(value["X"]),
            trade_id=(str(value["t"]) if value.get("t") not in {None, -1, "-1"} else None),
            last_price=str(value.get("L", "0")),
            last_quantity=str(value.get("l", "0")),
            commission=str(value.get("n", "0")),
            commission_asset=str(value.get("N", "UNKNOWN")),
        )
    if event_type == "outboundAccountPosition":
        balances = value.get("B")
        if not isinstance(balances, list):
            raise ValueError("account balance event is malformed")
        parsed: list[AccountBalanceEvent] = []
        for item in balances:
            if not isinstance(item, dict):
                raise ValueError("account balance event is malformed")
            parsed.append(
                AccountBalanceEvent(
                    event_time_ms=int(value["E"]),
                    asset=str(item["a"]),
                    free=str(item["f"]),
                    locked=str(item["l"]),
                )
            )
        return AccountSnapshotEvent(int(value["E"]), tuple(parsed))
    if event_type == "listStatus":
        return ListStatusEvent(
            event_time_ms=int(value["E"]),
            list_id=str(value.get("g", value.get("orderListId"))),
            status=str(value.get("l", value.get("listStatusType", "UNKNOWN"))),
            client_order_id=(str(value["C"]) if value.get("C") not in {None, ""} else None),
        )
    raise ValueError(f"unsupported Binance User Data event: {event_type}")
