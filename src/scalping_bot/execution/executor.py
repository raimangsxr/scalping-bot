from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from binance_common.errors import NetworkError, ServerError

from scalping_bot.domain.models import ExitMarket, PlaceProtection, SubmitEntry
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.safety import OperationalMode


@dataclass(frozen=True)
class ExecutionFill:
    fill_id: str
    price: Decimal
    quantity: Decimal
    commission: Decimal
    commission_asset: str


@dataclass(frozen=True)
class ExecutionLimits:
    step_size: Decimal
    min_notional: Decimal
    tick_size: Decimal = Decimal("0.01")
    max_quantity: Decimal | None = None
    max_notional: Decimal | None = None

    def normalize(self, quantity: Decimal, price: Decimal) -> Decimal:
        normalized = quantity // self.step_size * self.step_size
        notional = normalized * price
        if (
            normalized <= 0
            or notional < self.min_notional
            or (self.max_quantity is not None and normalized > self.max_quantity)
            or (
                self.max_notional is not None
                and self.max_notional > 0
                and notional > self.max_notional
            )
        ):
            raise ValueError("entry is below Binance dynamic quantity/notional filters")
        return normalized

    def normalize_price(self, price: Decimal) -> Decimal:
        return price // self.tick_size * self.tick_size


@dataclass(frozen=True)
class OrderResult:
    status: str
    exchange_order_id: str
    fills: tuple[ExecutionFill, ...]


class ExecutionExchange(Protocol):
    def submit_market_buy(self, *, client_order_id: str, quantity: str) -> OrderResult: ...

    def query_order(self, *, client_order_id: str) -> OrderResult: ...

    def place_oco_protection(
        self,
        *,
        client_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
    ) -> str: ...

    def flatten_market_sell(self, *, client_order_id: str, quantity: str) -> OrderResult: ...

    def cancel_protection(self, *, client_order_id: str) -> None: ...


class ExecutionService:
    def __init__(
        self, state: SQLiteState, exchange: ExecutionExchange, limits: ExecutionLimits
    ) -> None:
        self.state = state
        self.exchange = exchange
        self.limits = limits

    def refresh_partial_entries(
        self, *, event_time_ms: int
    ) -> tuple[tuple[SubmitEntry, OrderResult], ...]:
        updates: list[tuple[SubmitEntry, OrderResult]] = []
        for intent in self.state.partial_entry_intents():
            result = self.exchange.query_order(client_order_id=str(intent["client_order_id"]))
            executed = sum((fill.quantity for fill in result.fills), Decimal(0))
            if executed <= Decimal(str(intent["executed_quantity"])):
                continue
            command = SubmitEntry(
                client_order_id=str(intent["client_order_id"]),
                candidate_version=str(intent["candidate_version"]),
                source_event_id=str(intent["source_event_id"]),
                quantity=Decimal(str(intent["quantity"])),
                price_reference=sum(
                    (fill.price * fill.quantity for fill in result.fills), Decimal(0)
                )
                / executed,
                invalidation_price=Decimal(str(intent["invalidation_price"])),
                rationale={"condition": "durable partial-fill reconciliation"},
            )
            protection = self.state.protection(command.client_order_id)
            if protection is not None:
                self.exchange.cancel_protection(client_order_id=str(protection["client_order_id"]))
            self._account_entry(command, result, event_time_ms=event_time_ms)
            updates.append((command, result))
        return tuple(updates)

    def submit_exit(self, command: ExitMarket, *, event_time_ms: int) -> OrderResult:
        position = self.state.position()
        if position is None:
            raise RuntimeError("cannot exit without a durable open position")
        entry_identity = self.state.position_entry_client_order_id()
        protection = self.state.protection(entry_identity)
        try:
            self.state.order_intent(command.client_order_id)
        except LookupError:
            self.state.persist_order_intent(
                client_order_id=command.client_order_id,
                side="SELL",
                quantity=str(command.quantity),
                status="SUBMITTING",
                created_ms=event_time_ms,
                candidate_version=str(position["candidate_version"]),
                source_event_id=str(position["source_event_id"]),
                invalidation_price=str(position["invalidation_price"]),
            )
            self.exchange.cancel_protection(client_order_id=f"{entry_identity}-protect")
            try:
                exit_quantity = self.limits.normalize(
                    command.quantity, Decimal(str(position["entry_price"]))
                )
                result = self.exchange.flatten_market_sell(
                    client_order_id=command.client_order_id, quantity=str(exit_quantity)
                )
            except (TimeoutError, ConnectionError, NetworkError, ServerError):
                self.state.update_order_intent(command.client_order_id, status="UNKNOWN")
                result = self.exchange.query_order(client_order_id=command.client_order_id)
        else:
            if protection is not None:
                self.exchange.cancel_protection(client_order_id=str(protection["client_order_id"]))
            result = self.exchange.query_order(client_order_id=command.client_order_id)
        executed = Decimal(0)
        for fill in result.fills:
            self.state.record_fill(
                fill_id=fill.fill_id,
                client_order_id=command.client_order_id,
                price=str(fill.price),
                quantity=str(fill.quantity),
                commission=str(fill.commission),
                commission_asset=fill.commission_asset,
                event_time_ms=event_time_ms,
            )
            executed += fill.quantity
        self.state.update_order_intent(
            command.client_order_id,
            status=result.status,
            exchange_order_id=result.exchange_order_id,
            executed_quantity=str(executed),
        )
        position_quantity = Decimal(str(position["quantity"]))
        if result.status == "FILLED" and executed >= position_quantity:
            self.state.update_order_intent(entry_identity, status="CLOSED")
            self.state.clear_position()
        else:
            remaining = position_quantity - executed
            if remaining <= 0 or protection is None:
                self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                raise RuntimeError("partial exit cannot be safely re-protected")
            self.state.upsert_position(
                candidate_version=str(position["candidate_version"]),
                source_event_id=str(position["source_event_id"]),
                entry_fill_id=str(position["entry_fill_id"]),
                entry_time_ms=int(str(position["entry_time_ms"])),
                entry_price=str(position["entry_price"]),
                quantity=str(remaining),
                invalidation_price=str(position["invalidation_price"]),
                commission=str(position["commission"]),
                protected=False,
            )
            protection_id = self.exchange.place_oco_protection(
                client_order_id=str(protection["client_order_id"]),
                quantity=str(remaining),
                stop_price=str(protection["stop_price"]),
                take_profit_price=str(protection["take_profit_price"]),
            )
            self.state.record_protection(
                client_order_id=str(protection["client_order_id"]),
                entry_client_order_id=entry_identity,
                exchange_order_id=protection_id,
                quantity=str(remaining),
                stop_price=str(protection["stop_price"]),
                take_profit_price=str(protection["take_profit_price"]),
                created_ms=event_time_ms,
            )
            self.state.mark_position_protected()
        self.state.append_audit(
            kind="position-exit-filled",
            event_time_ms=event_time_ms,
            details={
                "client_order_id": command.client_order_id,
                "reason": command.reason,
                "fill_count": len(result.fills),
            },
        )
        return result

    def submit_entry(self, command: SubmitEntry, *, event_time_ms: int) -> OrderResult:
        if (
            not self.state.entries_allowed
            or self.state.operating_mode is not OperationalMode.RUNNING
        ):
            raise RuntimeError("entry submission is blocked until reconciliation and unlock")
        quantity = self.limits.normalize(command.quantity, command.price_reference)
        try:
            existing = self.state.order_intent(command.client_order_id)
        except LookupError:
            existing = None
        if existing is None:
            self.state.persist_order_intent(
                client_order_id=command.client_order_id,
                side="BUY",
                quantity=str(quantity),
                status="SUBMITTING",
                created_ms=event_time_ms,
                candidate_version=command.candidate_version,
                source_event_id=command.source_event_id,
                invalidation_price=str(command.invalidation_price),
            )
            self.state.append_audit(
                kind="order-intent-persisted",
                event_time_ms=event_time_ms,
                details={"client_order_id": command.client_order_id},
            )
            try:
                result = self.exchange.submit_market_buy(
                    client_order_id=command.client_order_id, quantity=str(quantity)
                )
            except (TimeoutError, ConnectionError, NetworkError, ServerError) as error:
                self.state.update_order_intent(command.client_order_id, status="UNKNOWN")
                self.state.append_audit(
                    kind="unknown-order-outcome",
                    event_time_ms=event_time_ms,
                    details={
                        "client_order_id": command.client_order_id,
                        "error_type": type(error).__name__,
                    },
                )
                result = self.exchange.query_order(client_order_id=command.client_order_id)
                self.state.append_audit(
                    kind="order-reconciled",
                    event_time_ms=event_time_ms,
                    details={
                        "client_order_id": command.client_order_id,
                        "status": result.status,
                    },
                )
        else:
            result = self.exchange.query_order(client_order_id=command.client_order_id)
        self._account_entry(command, result, event_time_ms=event_time_ms)
        return result

    def _account_entry(
        self, command: SubmitEntry, result: OrderResult, *, event_time_ms: int
    ) -> None:
        executed = Decimal(0)
        notional = Decimal(0)
        total_commission = Decimal(0)
        for fill in result.fills:
            self.state.record_fill(
                fill_id=fill.fill_id,
                client_order_id=command.client_order_id,
                price=str(fill.price),
                quantity=str(fill.quantity),
                commission=str(fill.commission),
                commission_asset=fill.commission_asset,
                event_time_ms=event_time_ms,
            )
            executed += fill.quantity
            notional += fill.price * fill.quantity
            total_commission += fill.commission
        self.state.update_order_intent(
            command.client_order_id,
            status=result.status,
            exchange_order_id=result.exchange_order_id,
            executed_quantity=str(executed),
        )
        self.state.append_audit(
            kind="fills-accounted",
            event_time_ms=event_time_ms,
            details={
                "client_order_id": command.client_order_id,
                "executed_quantity": str(executed),
                "fill_count": len(result.fills),
            },
        )
        if executed <= 0:
            return
        average_price = notional / executed
        first_fill_id = result.fills[0].fill_id
        self.state.upsert_position(
            candidate_version=command.candidate_version,
            source_event_id=command.source_event_id,
            entry_fill_id=first_fill_id,
            entry_time_ms=event_time_ms,
            entry_price=str(average_price),
            quantity=str(executed),
            invalidation_price=str(command.invalidation_price),
            commission=str(total_commission),
            protected=False,
        )

    def place_protection(self, command: PlaceProtection, *, event_time_ms: int) -> None:
        position = self.state.position()
        if position is None:
            raise RuntimeError("cannot protect without a durable open position")
        try:
            protection_id = self.exchange.place_oco_protection(
                client_order_id=command.client_order_id,
                quantity=str(command.quantity),
                stop_price=str(self.limits.normalize_price(command.stop_price)),
                take_profit_price=str(self.limits.normalize_price(command.take_profit_price)),
            )
        except Exception as error:
            self.state.append_audit(
                kind="protection-failed",
                event_time_ms=event_time_ms,
                details={
                    "client_order_id": command.parent_client_order_id,
                    "error_type": type(error).__name__,
                },
            )
            self.exchange.flatten_market_sell(
                client_order_id=f"{command.parent_client_order_id}-flatten",
                quantity=str(command.quantity),
            )
            self.state.clear_position()
            self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
            self.state.update_order_intent(
                command.parent_client_order_id, status="FLATTENED_LOCKED"
            )
            return
        self.state.record_protection(
            client_order_id=command.client_order_id,
            entry_client_order_id=command.parent_client_order_id,
            exchange_order_id=protection_id,
            quantity=str(command.quantity),
            stop_price=str(self.limits.normalize_price(command.stop_price)),
            take_profit_price=str(self.limits.normalize_price(command.take_profit_price)),
            created_ms=event_time_ms,
        )
        self.state.update_order_intent(command.parent_client_order_id, status="PROTECTED")
        self.state.mark_position_protected()
        self.state.append_audit(
            kind="position-protected",
            event_time_ms=event_time_ms,
            details={
                "client_order_id": command.parent_client_order_id,
                "protection_client_order_id": command.client_order_id,
                "quantity": str(command.quantity),
            },
        )
