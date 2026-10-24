from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

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
        local = self.state.reconciliation_intents()
        local_ids = {str(intent["client_order_id"]) for intent in local}
        divergence_list = [
            f"local intent {identity} is absent from Binance"
            for identity in sorted(local_ids - remote_ids)
        ]
        divergence_list += [
            f"Binance order {identity} is absent from local intent"
            for identity in sorted(remote_ids - local_ids)
            if identity.startswith("scalp-")
        ]
        remote_trades = snapshot.get("trades")
        if not isinstance(remote_trades, list):
            raise ValueError("exchange trade snapshot is invalid")
        remote_fill_ids = {
            str(trade.get("id", trade.get("tradeId")))
            for trade in remote_trades
            if isinstance(trade, dict)
            and (trade.get("id") is not None or trade.get("tradeId") is not None)
        }
        exchange_orders = self.state.exchange_orders()
        for trade in remote_trades:
            if not isinstance(trade, dict):
                continue
            exchange_order_id = str(trade.get("orderId", trade.get("order_id", "")))
            client_order_id = exchange_orders.get(exchange_order_id)
            fill_id = str(trade.get("id", trade.get("tradeId", "")))
            if client_order_id is None or not fill_id or fill_id in self.state.fill_ids():
                continue
            self.state.record_fill(
                fill_id=fill_id,
                client_order_id=client_order_id,
                price=str(trade.get("price")),
                quantity=str(trade.get("qty", trade.get("quantity"))),
                commission=str(trade.get("commission", "0")),
                commission_asset=str(
                    trade.get("commissionAsset", trade.get("commission_asset", "UNKNOWN"))
                ),
                event_time_ms=int(str(trade.get("time", event_time_ms))),
            )
        divergence_list += [
            f"local fill {identity} is absent from Binance"
            for identity in sorted(self.state.fill_ids() - remote_fill_ids)
        ]
        remote_protection_ids = {
            str(order.get("orderListId"))
            for order in orders
            if isinstance(order, dict) and order.get("orderListId") not in {None, -1}
        }
        divergence_list += [
            f"local protection {identity} is absent from Binance"
            for identity in sorted(self.state.active_protection_ids() - remote_protection_ids)
        ]
        balances = snapshot.get("balances")
        if not isinstance(balances, list):
            raise ValueError("exchange balance snapshot is invalid")
        position = self.state.position()
        if position is not None:
            btc = next(
                (
                    item
                    for item in balances
                    if isinstance(item, dict) and item.get("asset") == "BTC"
                ),
                None,
            )
            available = (
                Decimal(str(btc.get("free", "0"))) + Decimal(str(btc.get("locked", "0")))
                if btc is not None
                else Decimal(0)
            )
            if available < Decimal(str(position["quantity"])):
                divergence_list.append("Binance BTC balance is below the durable open position")
        else:
            btc = next(
                (
                    item
                    for item in balances
                    if isinstance(item, dict) and item.get("asset") == "BTC"
                ),
                None,
            )
            exposure = (
                Decimal(str(btc.get("free", "0"))) + Decimal(str(btc.get("locked", "0")))
                if btc is not None
                else Decimal(0)
            )
            if exposure > 0:
                divergence_list.append("Binance BTC exposure exists without a durable position")
        divergences = tuple(divergence_list)
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
