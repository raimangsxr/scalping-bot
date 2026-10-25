from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise

from scalping_bot.domain.events import MarketTick
from scalping_bot.domain.market_context import MarketContext


@dataclass(frozen=True)
class AggTrade:
    aggregate_trade_id: int
    time_ms: int
    price: Decimal
    quantity: Decimal
    buyer_maker: bool
    volatility_bucket: str | None = None
    size_bucket: str | None = None


def canonical_aggregate_trade_bytes(trades: tuple[AggTrade, ...]) -> bytes:
    """Serialize parsed aggregate trades for content-addressed evidence."""
    ordered = tuple(sorted(trades, key=lambda item: (item.time_ms, item.aggregate_trade_id)))
    if len({item.aggregate_trade_id for item in ordered}) != len(ordered):
        raise ValueError("aggregate-trade dataset contains duplicate identifiers")
    value = {
        "schema_version": 1,
        "trades": [
            {
                "aggregate_trade_id": item.aggregate_trade_id,
                "time_ms": item.time_ms,
                "price": str(item.price),
                "quantity": str(item.quantity),
                "buyer_maker": item.buyer_maker,
                "volatility_bucket": item.volatility_bucket,
                "size_bucket": item.size_bucket,
            }
            for item in ordered
        ],
    }
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


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
        context = MarketContext()
        events: list[MarketTick] = []
        for trade in trades:
            event = context.annotate_trade(
                MarketTick(
                    event_id=f"aggTrade-{trade.aggregate_trade_id}",
                    event_time_ms=trade.time_ms,
                    price=trade.price,
                    volatility_bucket=trade.volatility_bucket,
                    size_bucket=trade.size_bucket,
                ),
                quantity=trade.quantity,
            )
            if trade.time_ms > confirmed_ms:
                events.append(event)
        return tuple(events)
