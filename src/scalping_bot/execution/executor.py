from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from scalping_bot.domain.models import SubmitEntry
from scalping_bot.execution.sqlite_state import SQLiteState


@dataclass(frozen=True)
class OrderResult:
    status: str
    exchange_order_id: str
    fills: tuple[tuple[str, str, str, str, str], ...]


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


class ExecutionService:
    def __init__(self, state: SQLiteState, exchange: ExecutionExchange) -> None:
        self.state = state
        self.exchange = exchange

    def submit_entry(self, command: SubmitEntry, *, event_time_ms: int) -> OrderResult:
        if not self.state.entries_allowed or self.state.operating_mode != "RUNNING":
            raise RuntimeError("entry submission is blocked until reconciliation and unlock")
        try:
            existing = self.state.order_intent(command.client_order_id)
        except LookupError:
            existing = None
        if existing is None:
            self.state.persist_order_intent(
                client_order_id=command.client_order_id,
                side="BUY",
                quantity=str(command.quantity),
                status="SUBMITTING",
                created_ms=event_time_ms,
                candidate_version=command.candidate_version,
                invalidation_price=str(command.invalidation_price),
            )
            self.state.append_audit(
                kind="order-intent-persisted",
                event_time_ms=event_time_ms,
                details={"client_order_id": command.client_order_id},
            )
            try:
                result = self.exchange.submit_market_buy(
                    client_order_id=command.client_order_id, quantity=str(command.quantity)
                )
            except (TimeoutError, ConnectionError) as error:
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
        self._account_and_protect(command, result, event_time_ms=event_time_ms)
        return result

    def _account_and_protect(
        self, command: SubmitEntry, result: OrderResult, *, event_time_ms: int
    ) -> None:
        executed = Decimal(0)
        notional = Decimal(0)
        for fill_id, price, quantity, commission, commission_asset in result.fills:
            self.state.record_fill(
                fill_id=fill_id,
                client_order_id=command.client_order_id,
                price=price,
                quantity=quantity,
                commission=commission,
                commission_asset=commission_asset,
                event_time_ms=event_time_ms,
            )
            executed += Decimal(quantity)
            notional += Decimal(price) * Decimal(quantity)
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
        take_profit = average_price + 2 * (average_price - command.invalidation_price)
        protection_client_id = f"{command.client_order_id}-protect"
        try:
            protection_id = self.exchange.place_oco_protection(
                client_order_id=protection_client_id,
                quantity=str(executed),
                stop_price=str(command.invalidation_price),
                take_profit_price=str(take_profit),
            )
        except Exception as error:
            self.state.append_audit(
                kind="protection-failed",
                event_time_ms=event_time_ms,
                details={
                    "client_order_id": command.client_order_id,
                    "error_type": type(error).__name__,
                },
            )
            self.exchange.flatten_market_sell(
                client_order_id=f"{command.client_order_id}-flatten", quantity=str(executed)
            )
            self.state.set_operating_mode("FLATTEN_AND_LOCK")
            self.state.update_order_intent(command.client_order_id, status="FLATTENED_LOCKED")
            return
        self.state.record_protection(
            client_order_id=protection_client_id,
            entry_client_order_id=command.client_order_id,
            exchange_order_id=protection_id,
            quantity=str(executed),
            stop_price=str(command.invalidation_price),
            take_profit_price=str(take_profit),
            created_ms=event_time_ms,
        )
        self.state.update_order_intent(command.client_order_id, status="PROTECTED")
        self.state.append_audit(
            kind="position-protected",
            event_time_ms=event_time_ms,
            details={
                "client_order_id": command.client_order_id,
                "protection_client_order_id": protection_client_id,
                "quantity": str(executed),
            },
        )
