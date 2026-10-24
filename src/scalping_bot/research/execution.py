from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise

from scalping_bot.domain.events import MarketTick


@dataclass(frozen=True)
class AggTrade:
    aggregate_trade_id: int
    time_ms: int
    price: Decimal
    quantity: Decimal
    buyer_maker: bool


class AggTradeReplay:
    """Adapts immutable aggregate trades to the shared engine event vocabulary."""

    def events_after(
        self, confirmed_ms: int, trades: tuple[AggTrade, ...]
    ) -> tuple[MarketTick, ...]:
        for previous, current in pairwise(trades):
            if (current.time_ms, current.aggregate_trade_id) <= (
                previous.time_ms,
                previous.aggregate_trade_id,
            ):
                raise ValueError("aggregate trades must be chronological")
        return tuple(
            MarketTick(
                event_id=f"aggTrade-{trade.aggregate_trade_id}",
                event_time_ms=trade.time_ms,
                price=trade.price,
            )
            for trade in trades
            if trade.time_ms > confirmed_ms
        )
