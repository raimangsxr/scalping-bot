from __future__ import annotations

from dataclasses import dataclass

from scalping_bot.execution.binance import BinanceSpotTestnetAdapter
from scalping_bot.execution.sqlite_state import SQLiteState


@dataclass(frozen=True)
class ReconciliationResult:
    consistent: bool
    divergences: tuple[str, ...]


class Reconciler:
    def __init__(self, state: SQLiteState, exchange: BinanceSpotTestnetAdapter) -> None:
        self.state = state
        self.exchange = exchange

    def run(self, *, reason: str, event_time_ms: int) -> ReconciliationResult:
        self.state.set_entries_allowed(False)
        snapshot = self.exchange.reconciliation_snapshot()
        orders = snapshot["open_orders"]
        if not isinstance(orders, list):
            raise ValueError("exchange open-order snapshot is invalid")
        remote_ids = {
            str(order.get("clientOrderId"))
            for order in orders
            if isinstance(order, dict) and order.get("clientOrderId")
        }
        local = self.state.active_intents()
        local_ids = {str(intent["client_order_id"]) for intent in local}
        divergences = tuple(
            [
                f"local intent {identity} is absent from Binance"
                for identity in sorted(local_ids - remote_ids)
            ]
            + [
                f"Binance order {identity} is absent from local intent"
                for identity in sorted(remote_ids - local_ids)
            ]
        )
        result = ReconciliationResult(consistent=not divergences, divergences=divergences)
        self.state.record_reconciliation(
            reason=reason,
            event_time_ms=event_time_ms,
            consistent=result.consistent,
            divergences=divergences,
            exchange_snapshot=snapshot,
        )
        self.state.append_audit(
            kind="reconciliation",
            event_time_ms=event_time_ms,
            details={"reason": reason, "consistent": result.consistent, "divergences": divergences},
        )
        return result
