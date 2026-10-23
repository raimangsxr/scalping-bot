from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise


@dataclass(frozen=True)
class AggTrade:
    aggregate_trade_id: int
    time_ms: int
    price: Decimal
    quantity: Decimal
    buyer_maker: bool


@dataclass(frozen=True)
class ConfirmedSignal:
    confirmed_ms: int
    side: str
    stop_price: Decimal
    take_profit_price: Decimal


@dataclass(frozen=True)
class ChronologicalExecution:
    entry_trade_id: int
    entry_price: Decimal
    exit_trade_id: int | None
    exit_price: Decimal | None
    exit_reason: str | None


class AggTradeExecution:
    def execute(
        self, signal: ConfirmedSignal, trades: tuple[AggTrade, ...]
    ) -> ChronologicalExecution:
        if signal.side != "buy":
            raise ValueError("the first version supports long entries only")
        for previous, current in pairwise(trades):
            if (current.time_ms, current.aggregate_trade_id) <= (
                previous.time_ms,
                previous.aggregate_trade_id,
            ):
                raise ValueError("aggregate trades must be chronological")
        post_confirmation = [trade for trade in trades if trade.time_ms > signal.confirmed_ms]
        if not post_confirmation:
            raise ValueError("no aggregate trade is available after signal confirmation")
        entry = post_confirmation[0]
        for trade in post_confirmation[1:]:
            if trade.price <= signal.stop_price:
                return ChronologicalExecution(
                    entry.aggregate_trade_id,
                    entry.price,
                    trade.aggregate_trade_id,
                    trade.price,
                    "protective-stop",
                )
            if trade.price >= signal.take_profit_price:
                return ChronologicalExecution(
                    entry.aggregate_trade_id,
                    entry.price,
                    trade.aggregate_trade_id,
                    trade.price,
                    "take-profit",
                )
        return ChronologicalExecution(entry.aggregate_trade_id, entry.price, None, None, None)
