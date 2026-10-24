from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from scalping_bot.execution.binance import BinanceSpotTestnetAdapter
from scalping_bot.execution.executor import OrderResult
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
        try:
            snapshot = self.exchange.reconciliation_snapshot()
        except Exception as error:
            self.state.set_entries_allowed(False)
            self.state.append_audit(
                kind="reconciliation-failed",
                event_time_ms=event_time_ms,
                details={"reason": reason, "error_type": type(error).__name__},
            )
            raise
        orders = snapshot["open_orders"]
        if not isinstance(orders, list):
            raise ValueError("exchange open-order snapshot is invalid")
        remote_ids = {
            str(identity)
            for order in orders
            if isinstance(order, dict)
            for identity in (order.get("clientOrderId") or order.get("origClientOrderId"),)
            if identity is not None
        }
        local = self.state.reconciliation_intents()
        local_ids = {str(intent["client_order_id"]) for intent in local}
        resolved_local_ids: set[str] = set()
        exchange_orders = self.state.exchange_orders()
        for order in orders:
            if not isinstance(order, dict):
                continue
            client_order_id = order.get("clientOrderId") or order.get("origClientOrderId")
            if client_order_id is None or str(client_order_id) not in local_ids:
                continue
            identity = str(client_order_id)
            exchange_order_id = order.get("orderId") or order.get("order_id")
            self.state.update_order_intent(
                identity,
                status=str(order.get("status", "NEW")),
                exchange_order_id=(
                    str(exchange_order_id) if exchange_order_id is not None else None
                ),
            )
            if exchange_order_id is not None:
                exchange_orders[identity] = str(exchange_order_id)
            resolved_local_ids.add(identity)
        for intent in local:
            identity = str(intent["client_order_id"])
            if identity in resolved_local_ids or identity in remote_ids:
                continue
            try:
                query_result = self.exchange.query_order(client_order_id=identity)
            except Exception as error:
                self.state.append_audit(
                    kind="intent-reconciliation-failed",
                    event_time_ms=event_time_ms,
                    details={"client_order_id": identity, "error_type": type(error).__name__},
                )
                continue
            self._record_order_result(identity, query_result, event_time_ms=event_time_ms)
            exchange_orders[identity] = query_result.exchange_order_id
            resolved_local_ids.add(identity)
        divergence_list = [
            f"local intent {identity} is absent from Binance"
            for identity in sorted(local_ids - remote_ids - resolved_local_ids)
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
        for trade in remote_trades:
            if not isinstance(trade, dict):
                continue
            exchange_order_id = str(trade.get("orderId", trade.get("order_id", "")))
            raw_client_order_id = trade.get("clientOrderId") or trade.get("origClientOrderId")
            client_order_id = exchange_orders.get(exchange_order_id) or (
                str(raw_client_order_id) if raw_client_order_id is not None else None
            )
            fill_value = trade.get("id", trade.get("tradeId"))
            fill_id = str(fill_value) if fill_value is not None else ""
            if not fill_id or fill_id in self.state.fill_ids():
                continue
            if client_order_id is None or client_order_id not in local_ids | set(exchange_orders):
                divergence_list.append(f"Binance fill {fill_id} is absent from local intent")
                continue
            price = trade.get("price")
            quantity = trade.get("qty", trade.get("quantity"))
            if price is None or quantity is None:
                divergence_list.append(f"Binance fill {fill_id} is malformed")
                continue
            self.state.record_fill(
                fill_id=fill_id,
                client_order_id=client_order_id,
                price=str(price),
                quantity=str(quantity),
                commission=str(trade.get("commission", "0")),
                commission_asset=str(
                    trade.get("commissionAsset", trade.get("commission_asset", "UNKNOWN"))
                ),
                event_time_ms=int(str(trade.get("time", event_time_ms))),
            )
            resolved_local_ids.add(client_order_id)
            fills = self.state.fills_for_order(client_order_id)
            current_status = str(self.state.order_intent(client_order_id)["status"])
            self.state.update_order_intent(
                client_order_id,
                status=(
                    current_status
                    if current_status in {"FILLED", "PROTECTED", "PARTIALLY_FILLED"}
                    else "PARTIALLY_FILLED"
                    if str(self.state.order_intent(client_order_id)["side"]) == "BUY"
                    else "FILLED"
                ),
                exchange_order_id=(exchange_order_id or None),
                executed_quantity=str(
                    sum((Decimal(str(item["quantity"])) for item in fills), Decimal(0))
                ),
            )
            self.state.rebuild_position_from_order(client_order_id)
        divergence_list = (
            [
                f"local intent {identity} is absent from Binance"
                for identity in sorted(local_ids - remote_ids - resolved_local_ids)
            ]
            + [item for item in divergence_list if item.startswith("Binance order ")]
            + [
                item
                for item in divergence_list
                if item.startswith("Binance fill ") or item.startswith("local fill ")
            ]
        )
        divergence_list += [
            f"local fill {identity} is absent from Binance"
            for identity in sorted(self.state.fill_ids() - remote_fill_ids)
        ]
        remote_protection_ids = {
            str(order.get("orderListId") or order.get("listOrderId"))
            for order in orders
            if isinstance(order, dict)
            and (order.get("orderListId") or order.get("listOrderId")) not in {None, -1, "-1"}
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

    def _record_order_result(
        self, client_order_id: str, result: OrderResult, *, event_time_ms: int
    ) -> None:
        for fill in result.fills:
            self.state.record_fill(
                fill_id=fill.fill_id,
                client_order_id=client_order_id,
                price=str(fill.price),
                quantity=str(fill.quantity),
                commission=str(fill.commission),
                commission_asset=fill.commission_asset,
                event_time_ms=event_time_ms,
            )
        fills = self.state.fills_for_order(client_order_id)
        executed = sum((Decimal(str(fill["quantity"])) for fill in fills), Decimal(0))
        self.state.update_order_intent(
            client_order_id,
            status=result.status,
            exchange_order_id=result.exchange_order_id,
            executed_quantity=str(executed),
        )
        self.state.rebuild_position_from_order(client_order_id)
