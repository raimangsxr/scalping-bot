from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from binance_common.errors import (
    NetworkError,
    RateLimitBanError,
    ServerError,
    TooManyRequestsError,
)

from scalping_bot.domain.models import ExitMarket, PlaceProtection, SubmitEntry
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.observability import OperationalMetrics
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

    def __post_init__(self) -> None:
        if (
            self.step_size <= 0
            or self.min_notional < 0
            or self.tick_size <= 0
            or (self.max_quantity is not None and self.max_quantity <= 0)
            or (self.max_notional is not None and self.max_notional < 0)
        ):
            raise ValueError("Binance execution filters must be positive")

    def normalize(self, quantity: Decimal, price: Decimal) -> Decimal:
        normalized = quantity // self.step_size * self.step_size
        notional = normalized * price
        if (
            normalized <= 0
            or price <= 0
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
        normalized = price // self.tick_size * self.tick_size
        if normalized <= 0:
            raise ValueError("price is below Binance tick-size filters")
        return normalized


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

    def cancel_order(self, *, client_order_id: str) -> None: ...


class ExecutionService:
    def __init__(
        self,
        state: SQLiteState,
        exchange: ExecutionExchange,
        limits: ExecutionLimits,
        metrics: OperationalMetrics | None = None,
    ) -> None:
        self.state = state
        self.exchange = exchange
        self.limits = limits
        self.metrics = metrics

    def refresh_partial_entries(
        self, *, event_time_ms: int
    ) -> tuple[tuple[SubmitEntry, OrderResult], ...]:
        updates: list[tuple[SubmitEntry, OrderResult]] = []
        for intent in self.state.partial_entry_intents():
            try:
                result = self.exchange.query_order(client_order_id=str(intent["client_order_id"]))
            except Exception as error:
                self.state.set_operating_mode(OperationalMode.DEGRADED)
                self.state.append_audit(
                    kind="partial-fill-reconciliation-failed",
                    event_time_ms=event_time_ms,
                    details={
                        "client_order_id": str(intent["client_order_id"]),
                        "error_type": type(error).__name__,
                    },
                )
                raise
            self.state.append_audit(
                kind="partial-fill-reconciled",
                event_time_ms=event_time_ms,
                details={"client_order_id": str(intent["client_order_id"])},
            )
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
                try:
                    self.exchange.cancel_protection(
                        client_order_id=str(protection["client_order_id"])
                    )
                except Exception as error:
                    self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                    self.state.append_audit(
                        kind="partial-fill-protection-cancel-failed",
                        event_time_ms=event_time_ms,
                        details={"error_type": type(error).__name__},
                    )
                    raise
            self._account_entry(command, result, event_time_ms=event_time_ms)
            updates.append((command, result))
        return tuple(updates)

    def flatten_and_lock(self, *, event_time_ms: int, reason: str) -> None:
        """Cancel pending entry work, flatten exposure, and persist the lock."""
        if not reason.strip():
            raise ValueError("flatten and lock requires a reason")
        for intent in self.state.active_intents():
            identity = str(intent["client_order_id"])
            if intent["side"] != "BUY":
                continue
            try:
                self.exchange.cancel_order(client_order_id=identity)
            except Exception as error:
                self.state.append_audit(
                    kind="pending-entry-cancel-failed",
                    event_time_ms=event_time_ms,
                    details={"client_order_id": identity, "error_type": type(error).__name__},
                )
                self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                raise
            self.state.update_order_intent(identity, status="CANCELED")
            self.state.append_audit(
                kind="pending-entry-canceled",
                event_time_ms=event_time_ms,
                details={"client_order_id": identity},
            )
        position = self.state.position()
        if position is None:
            self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
            self.state.append_audit(
                kind="flattened-and-locked",
                event_time_ms=event_time_ms,
                details={"reason": reason, "exposure": "flat"},
            )
            return
        entry_identity = self.state.position_entry_client_order_id()
        protection = self.state.protection(entry_identity)
        if protection is not None:
            try:
                self.exchange.cancel_protection(client_order_id=str(protection["client_order_id"]))
            except Exception as error:
                self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                self.state.append_audit(
                    kind="operator-protection-cancel-failed",
                    event_time_ms=event_time_ms,
                    details={"error_type": type(error).__name__},
                )
                raise
        self._flatten_and_lock(
            entry_identity,
            Decimal(str(position["quantity"])),
            event_time_ms=event_time_ms,
            reason=reason,
        )

    def submit_exit(self, command: ExitMarket, *, event_time_ms: int) -> OrderResult:
        position = self.state.position()
        if position is None:
            raise RuntimeError("cannot exit without a durable open position")
        entry_identity = self.state.position_entry_client_order_id()
        protection = self.state.protection(entry_identity)
        protection_client_id = (
            str(protection["client_order_id"])
            if protection is not None
            else f"{entry_identity}-protect"
        )
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
            try:
                exit_quantity = self.limits.normalize(
                    command.quantity, Decimal(str(position["entry_price"]))
                )
            except ValueError as error:
                self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                self.state.append_audit(
                    kind="exit-filter-failed",
                    event_time_ms=event_time_ms,
                    details={"error_type": type(error).__name__},
                )
                raise
            self._cancel_protection_before_exit(protection_client_id, event_time_ms=event_time_ms)
            try:
                result = self.exchange.flatten_market_sell(
                    client_order_id=command.client_order_id, quantity=str(exit_quantity)
                )
            except (
                TimeoutError,
                ConnectionError,
                NetworkError,
                ServerError,
                TooManyRequestsError,
                RateLimitBanError,
            ) as error:
                self.state.update_order_intent(command.client_order_id, status="UNKNOWN")
                self._record_unknown_order()
                self.state.append_audit(
                    kind="unknown-order-outcome",
                    event_time_ms=event_time_ms,
                    details={
                        "client_order_id": command.client_order_id,
                        "error_type": type(error).__name__,
                    },
                )
                try:
                    result = self.exchange.query_order(client_order_id=command.client_order_id)
                except (
                    TimeoutError,
                    ConnectionError,
                    NetworkError,
                    ServerError,
                    TooManyRequestsError,
                    RateLimitBanError,
                ) as query_error:
                    self.state.append_audit(
                        kind="unknown-order-recovery-failed",
                        event_time_ms=event_time_ms,
                        details={
                            "client_order_id": command.client_order_id,
                            "error_type": type(query_error).__name__,
                        },
                    )
                    self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                    raise
                if isinstance(error, (TooManyRequestsError, RateLimitBanError)):
                    self._record_rate_limit()
                    self.state.set_operating_mode(OperationalMode.DEGRADED)
                    self.state.append_audit(
                        kind="rate-limit-exit",
                        event_time_ms=event_time_ms,
                        details={"client_order_id": command.client_order_id},
                    )
        else:
            if protection is not None:
                self._cancel_protection_before_exit(
                    protection_client_id, event_time_ms=event_time_ms
                )
            result = self.exchange.query_order(client_order_id=command.client_order_id)
            self.state.append_audit(
                kind="duplicate-exit-reconciled",
                event_time_ms=event_time_ms,
                details={"client_order_id": command.client_order_id},
            )
        self.state.append_audit(
            kind="exit-request-reconciled",
            event_time_ms=event_time_ms,
            details={
                "client_order_id": command.client_order_id,
                "status": result.status,
                "fill_count": len(result.fills),
            },
        )
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
        durable_fills = self.state.fills_for_order(command.client_order_id)
        executed = sum((Decimal(str(fill["quantity"])) for fill in durable_fills), Decimal(0))
        exit_commission = sum(
            (Decimal(str(fill["commission"])) for fill in durable_fills), Decimal(0)
        )
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
            gross_pnl = sum(
                (
                    (Decimal(str(fill["price"])) - Decimal(str(position["entry_price"])))
                    * Decimal(str(fill["quantity"]))
                    for fill in durable_fills
                ),
                Decimal(0),
            )
            self.state.append_audit(
                kind="position-closed",
                event_time_ms=event_time_ms,
                details={
                    "entry_client_order_id": entry_identity,
                    "client_order_id": command.client_order_id,
                    "gross_pnl": str(gross_pnl),
                    "net_pnl": str(
                        gross_pnl - Decimal(str(position["commission"])) - exit_commission
                    ),
                    "commission_assets": sorted(
                        {str(fill["commission_asset"]) for fill in durable_fills}
                    ),
                },
            )
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
            try:
                protection_id = self.exchange.place_oco_protection(
                    client_order_id=str(protection["client_order_id"]),
                    quantity=str(remaining),
                    stop_price=str(protection["stop_price"]),
                    take_profit_price=str(protection["take_profit_price"]),
                )
            except Exception as error:
                self.state.append_audit(
                    kind="reprotection-failed",
                    event_time_ms=event_time_ms,
                    details={"error_type": type(error).__name__},
                )
                self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                raise RuntimeError("partial exit could not be safely re-protected") from error
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
        try:
            existing = self.state.order_intent(command.client_order_id)
        except LookupError:
            existing = None
        if existing is None and (
            not self.state.entries_allowed
            or self.state.operating_mode is not OperationalMode.RUNNING
        ):
            raise RuntimeError("entry submission is blocked until reconciliation and unlock")
        quantity = (
            Decimal(str(existing["quantity"]))
            if existing is not None
            else self.limits.normalize(command.quantity, command.price_reference)
        )
        if existing is not None and str(existing["status"]) in {
            "CLOSED",
            "FLATTENED_LOCKED",
            "CANCELED",
            "REJECTED",
        }:
            result = self.exchange.query_order(client_order_id=command.client_order_id)
            self.state.append_audit(
                kind="terminal-entry-reconciled",
                event_time_ms=event_time_ms,
                details={"client_order_id": command.client_order_id},
            )
            return result
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
                self.state.append_audit(
                    kind="entry-request-submitted",
                    event_time_ms=event_time_ms,
                    details={
                        "client_order_id": command.client_order_id,
                        "quantity": str(quantity),
                    },
                )
            except (
                TimeoutError,
                ConnectionError,
                NetworkError,
                ServerError,
                TooManyRequestsError,
                RateLimitBanError,
            ) as error:
                self.state.update_order_intent(command.client_order_id, status="UNKNOWN")
                self._record_unknown_order()
                self.state.append_audit(
                    kind="unknown-order-outcome",
                    event_time_ms=event_time_ms,
                    details={
                        "client_order_id": command.client_order_id,
                        "error_type": type(error).__name__,
                    },
                )
                try:
                    result = self.exchange.query_order(client_order_id=command.client_order_id)
                except (
                    TimeoutError,
                    ConnectionError,
                    NetworkError,
                    ServerError,
                    TooManyRequestsError,
                    RateLimitBanError,
                ) as query_error:
                    self.state.append_audit(
                        kind="unknown-order-recovery-failed",
                        event_time_ms=event_time_ms,
                        details={
                            "client_order_id": command.client_order_id,
                            "error_type": type(query_error).__name__,
                        },
                    )
                    self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                    raise
                if isinstance(error, (TooManyRequestsError, RateLimitBanError)):
                    self._record_rate_limit()
                    self.state.set_operating_mode(OperationalMode.DEGRADED)
                    self.state.append_audit(
                        kind="rate-limit-entry",
                        event_time_ms=event_time_ms,
                        details={"client_order_id": command.client_order_id},
                    )
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
            self.state.append_audit(
                kind="duplicate-entry-reconciled",
                event_time_ms=event_time_ms,
                details={"client_order_id": command.client_order_id},
            )
        self._account_entry(command, result, event_time_ms=event_time_ms)
        return result

    def _cancel_protection_before_exit(self, client_order_id: str, *, event_time_ms: int) -> None:
        try:
            self.exchange.cancel_protection(client_order_id=client_order_id)
        except Exception as error:
            self.state.set_operating_mode(OperationalMode.DEGRADED)
            self.state.append_audit(
                kind="protection-cancel-failed-before-exit",
                event_time_ms=event_time_ms,
                details={
                    "client_order_id": client_order_id,
                    "error_type": type(error).__name__,
                },
            )

    def _account_entry(
        self, command: SubmitEntry, result: OrderResult, *, event_time_ms: int
    ) -> None:
        intent_before = self.state.order_intent(command.client_order_id)
        previous_executed = Decimal(str(intent_before["executed_quantity"]))
        previous_position = self.state.position()
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
        durable_fills = self.state.fills_for_order(command.client_order_id)
        reported_executed = sum(
            (Decimal(str(fill["quantity"])) for fill in durable_fills), Decimal(0)
        )
        notional = sum(
            (
                Decimal(str(fill["price"])) * Decimal(str(fill["quantity"]))
                for fill in durable_fills
            ),
            Decimal(0),
        )
        total_commission = sum(
            (Decimal(str(fill["commission"])) for fill in durable_fills), Decimal(0)
        )
        executed = max(previous_executed, reported_executed)
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
        if durable_fills and reported_executed > 0:
            average_price = notional / reported_executed
            first_fill_id = str(durable_fills[0]["fill_id"])
            commission = total_commission
        elif previous_position is not None:
            average_price = Decimal(str(previous_position["entry_price"]))
            first_fill_id = str(previous_position["entry_fill_id"])
            commission = Decimal(str(previous_position["commission"]))
        else:
            return
        position_protected = bool(previous_position and previous_position["protected"])
        if reported_executed > previous_executed:
            position_protected = False
        self.state.upsert_position(
            candidate_version=command.candidate_version,
            source_event_id=command.source_event_id,
            entry_fill_id=first_fill_id,
            entry_time_ms=event_time_ms,
            entry_price=str(average_price),
            quantity=str(executed),
            invalidation_price=str(command.invalidation_price),
            commission=str(commission),
            protected=position_protected,
        )

    def place_protection(self, command: PlaceProtection, *, event_time_ms: int) -> None:
        position = self.state.position()
        if position is None:
            raise RuntimeError("cannot protect without a durable open position")
        position_quantity = Decimal(str(position["quantity"]))
        existing_protection = self.state.protection(command.parent_client_order_id)
        if (
            existing_protection is not None
            and str(existing_protection["client_order_id"]) == command.client_order_id
            and Decimal(str(existing_protection["quantity"])) == position_quantity
        ):
            self.state.append_audit(
                kind="duplicate-protection-reconciled",
                event_time_ms=event_time_ms,
                details={"client_order_id": command.client_order_id},
            )
            return
        if command.quantity != position_quantity:
            self._flatten_and_lock(
                command.parent_client_order_id,
                position_quantity,
                event_time_ms=event_time_ms,
                reason="protection quantity did not cover actual exposure",
            )
            return
        try:
            self.state.append_audit(
                kind="protection-request-submitted",
                event_time_ms=event_time_ms,
                details={
                    "client_order_id": command.client_order_id,
                    "parent_client_order_id": command.parent_client_order_id,
                    "quantity": str(command.quantity),
                },
            )
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
            self._flatten_and_lock(
                command.parent_client_order_id,
                position_quantity,
                event_time_ms=event_time_ms,
                reason="protection request failed",
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
        parent_status = str(self.state.order_intent(command.parent_client_order_id)["status"])
        self.state.update_order_intent(
            command.parent_client_order_id,
            status=("PARTIALLY_FILLED" if parent_status == "PARTIALLY_FILLED" else "PROTECTED"),
        )
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

    def _flatten_and_lock(
        self,
        parent_client_order_id: str,
        quantity: Decimal,
        *,
        event_time_ms: int,
        reason: str,
    ) -> None:
        """Persist and account the emergency exit before declaring the lock."""
        position = self.state.position()
        flatten_id = f"{parent_client_order_id}-flatten"
        try:
            existing = self.state.order_intent(flatten_id)
        except LookupError:
            existing = None
        if existing is None:
            self.state.persist_order_intent(
                client_order_id=flatten_id,
                side="SELL",
                quantity=str(quantity),
                status="SUBMITTING",
                created_ms=event_time_ms,
                candidate_version=(
                    str(position["candidate_version"]) if position is not None else None
                ),
                source_event_id=(
                    str(position["source_event_id"]) if position is not None else None
                ),
                invalidation_price=(
                    str(position["invalidation_price"]) if position is not None else None
                ),
            )
        if existing is not None and str(existing["status"]) in {"FILLED", "CLOSED"}:
            result = OrderResult(
                status=str(existing["status"]),
                exchange_order_id=str(existing["exchange_order_id"] or "unknown"),
                fills=tuple(
                    ExecutionFill(
                        fill_id=str(fill["fill_id"]),
                        price=Decimal(str(fill["price"])),
                        quantity=Decimal(str(fill["quantity"])),
                        commission=Decimal(str(fill["commission"])),
                        commission_asset=str(fill["commission_asset"]),
                    )
                    for fill in self.state.fills_for_order(flatten_id)
                ),
            )
        else:
            try:
                result = (
                    self.exchange.query_order(client_order_id=flatten_id)
                    if existing is not None
                    else self.exchange.flatten_market_sell(
                        client_order_id=flatten_id,
                        quantity=str(quantity),
                    )
                )
            except Exception as error:
                if existing is None:
                    self.state.update_order_intent(flatten_id, status="UNKNOWN")
                    self._record_unknown_order()
                self.state.append_audit(
                    kind="flatten-unknown-order-outcome",
                    event_time_ms=event_time_ms,
                    details={"client_order_id": flatten_id, "error_type": type(error).__name__},
                )
                try:
                    result = self.exchange.query_order(client_order_id=flatten_id)
                except Exception as query_error:
                    self.state.append_audit(
                        kind="flatten-unknown-order-recovery-failed",
                        event_time_ms=event_time_ms,
                        details={
                            "client_order_id": flatten_id,
                            "error_type": type(query_error).__name__,
                        },
                    )
                    self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
                    self.state.update_order_intent(
                        parent_client_order_id, status="FLATTENED_LOCKED"
                    )
                    return
        for fill in result.fills:
            self.state.record_fill(
                fill_id=fill.fill_id,
                client_order_id=flatten_id,
                price=str(fill.price),
                quantity=str(fill.quantity),
                commission=str(fill.commission),
                commission_asset=fill.commission_asset,
                event_time_ms=event_time_ms,
            )
        durable_fills = self.state.fills_for_order(flatten_id)
        executed = sum((Decimal(str(fill["quantity"])) for fill in durable_fills), Decimal(0))
        self.state.update_order_intent(
            flatten_id,
            status=result.status,
            exchange_order_id=result.exchange_order_id,
            executed_quantity=str(executed),
        )
        if position is not None and executed < quantity:
            remaining = quantity - executed
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
        else:
            self.state.clear_position()
        self.state.update_order_intent(parent_client_order_id, status="FLATTENED_LOCKED")
        self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
        self.state.append_audit(
            kind="flattened-and-locked",
            event_time_ms=event_time_ms,
            details={
                "client_order_id": parent_client_order_id,
                "flatten_client_order_id": flatten_id,
                "requested_quantity": str(quantity),
                "executed_quantity": str(executed),
                "reason": reason,
                "status": result.status,
            },
        )

    def _record_unknown_order(self) -> None:
        if self.metrics is not None:
            self.metrics.unknown_order_outcomes_total.inc()

    def _record_rate_limit(self) -> None:
        if self.metrics is not None:
            self.metrics.rate_limit_events_total.inc()
