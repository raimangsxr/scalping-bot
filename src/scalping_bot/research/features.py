from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise


@dataclass(frozen=True)
class TradePrint:
    aggregate_trade_id: int
    time_ms: int
    price: Decimal
    quantity: Decimal
    buyer_maker: bool


@dataclass(frozen=True)
class TradeFlowFeatures:
    valid: bool
    executed_buy_volume: Decimal = Decimal(0)
    executed_sell_volume: Decimal = Decimal(0)
    cumulative_delta: Decimal = Decimal(0)
    trade_intensity: Decimal = Decimal(0)


class TradeFlowFeatureBuilder:
    def __init__(self, *, window_ms: int) -> None:
        if window_ms <= 0:
            raise ValueError("Trade Flow feature windows must be positive")
        self.window_ms = window_ms

    def build(self, trades: tuple[TradePrint, ...], *, as_of_ms: int) -> TradeFlowFeatures:
        for previous, current in pairwise(trades):
            if current.aggregate_trade_id != previous.aggregate_trade_id + 1:
                return TradeFlowFeatures(valid=False)
            if current.time_ms < previous.time_ms:
                return TradeFlowFeatures(valid=False)
        if any(trade.quantity <= 0 or trade.price <= 0 for trade in trades):
            return TradeFlowFeatures(valid=False)
        eligible = [
            trade for trade in trades if as_of_ms - self.window_ms < trade.time_ms <= as_of_ms
        ]
        if not eligible:
            return TradeFlowFeatures(valid=False)
        buy = sum((trade.quantity for trade in eligible if not trade.buyer_maker), Decimal(0))
        sell = sum((trade.quantity for trade in eligible if trade.buyer_maker), Decimal(0))
        seconds = Decimal(self.window_ms) / Decimal(1000)
        return TradeFlowFeatures(
            valid=True,
            executed_buy_volume=buy,
            executed_sell_volume=sell,
            cumulative_delta=buy - sell,
            trade_intensity=Decimal(len(eligible)) / seconds,
        )


@dataclass(frozen=True)
class BookFlowInput:
    valid_segment: bool
    best_bid_quantity: Decimal = Decimal(0)
    best_ask_quantity: Decimal = Decimal(0)
    bid_depth: Decimal = Decimal(0)
    ask_depth: Decimal = Decimal(0)
    previous_bid_depth: Decimal = Decimal(0)
    previous_ask_depth: Decimal = Decimal(0)
    executed_buy_volume: Decimal = Decimal(0)
    executed_sell_volume: Decimal = Decimal(0)
    market_data_quality: str = "valid"
    segment_id: str | None = None

    @classmethod
    def invalid(cls) -> BookFlowInput:
        return cls(valid_segment=False, market_data_quality="missing")


@dataclass(frozen=True)
class BookFlowFeatures:
    valid: bool
    best_quote_imbalance: Decimal = Decimal(0)
    depth_imbalance: Decimal = Decimal(0)
    bid_replenishment: Decimal = Decimal(0)
    ask_removal: Decimal = Decimal(0)
    liquidity_change_confirmed_by_trades: bool = False
    market_data_quality: str = "invalid"
    segment_id: str | None = None


def book_flow_features(item: BookFlowInput) -> BookFlowFeatures:
    if not item.valid_segment or item.market_data_quality != "valid" or not item.segment_id:
        return BookFlowFeatures(
            valid=False,
            market_data_quality=item.market_data_quality,
            segment_id=item.segment_id,
        )
    quantities = (
        item.best_bid_quantity,
        item.best_ask_quantity,
        item.bid_depth,
        item.ask_depth,
        item.previous_bid_depth,
        item.previous_ask_depth,
        item.executed_buy_volume,
        item.executed_sell_volume,
    )
    if any(value < 0 for value in quantities):
        return BookFlowFeatures(
            valid=False,
            market_data_quality="invalid-values",
            segment_id=item.segment_id,
        )
    quote_total = item.best_bid_quantity + item.best_ask_quantity
    depth_total = item.bid_depth + item.ask_depth
    if quote_total <= 0 or depth_total <= 0:
        return BookFlowFeatures(
            valid=False,
            market_data_quality="empty-book",
            segment_id=item.segment_id,
        )
    bid_replenishment = max(Decimal(0), item.bid_depth - item.previous_bid_depth)
    ask_removal = max(Decimal(0), item.previous_ask_depth - item.ask_depth)
    return BookFlowFeatures(
        valid=True,
        best_quote_imbalance=(item.best_bid_quantity - item.best_ask_quantity) / quote_total,
        depth_imbalance=(item.bid_depth - item.ask_depth) / depth_total,
        bid_replenishment=bid_replenishment,
        ask_removal=ask_removal,
        liquidity_change_confirmed_by_trades=(
            item.executed_buy_volume + item.executed_sell_volume > 0
            and item.executed_buy_volume > item.executed_sell_volume
            and (bid_replenishment > 0 or ask_removal > 0)
        ),
        market_data_quality=item.market_data_quality,
        segment_id=item.segment_id,
    )
