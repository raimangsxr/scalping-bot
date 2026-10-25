"""Causal market-context derivation shared by capture, replay, and live streams."""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from decimal import Decimal

from scalping_bot.domain.events import MarketTick

VOLATILITY_LOW_MAX = Decimal("0.001")
VOLATILITY_MEDIUM_MAX = Decimal("0.005")
SIZE_SMALL_MAX_NOTIONAL = Decimal("1000")
SIZE_MEDIUM_MAX_NOTIONAL = Decimal("10000")


def size_bucket(notional: Decimal) -> str:
    if notional < 0:
        raise ValueError("market notional cannot be negative")
    if notional < SIZE_SMALL_MAX_NOTIONAL:
        return "small"
    if notional < SIZE_MEDIUM_MAX_NOTIONAL:
        return "medium"
    return "large"


def volatility_bucket(mean_absolute_return: Decimal) -> str:
    if mean_absolute_return < 0:
        raise ValueError("volatility cannot be negative")
    if mean_absolute_return < VOLATILITY_LOW_MAX:
        return "low"
    if mean_absolute_return < VOLATILITY_MEDIUM_MAX:
        return "medium"
    return "high"


class MarketContext:
    """Derive only from current and previously observed prices, never future data."""

    def __init__(self, *, window: int = 20) -> None:
        if window < 1:
            raise ValueError("market-context window must be positive")
        self._previous_price: Decimal | None = None
        self._returns: deque[Decimal] = deque(maxlen=window)

    def annotate_trade(self, event: MarketTick, *, quantity: Decimal) -> MarketTick:
        if quantity < 0:
            raise ValueError("trade quantity cannot be negative")
        return self.annotate(event, notional=event.price * quantity)

    def annotate(self, event: MarketTick, *, notional: Decimal | None = None) -> MarketTick:
        if event.price <= 0:
            raise ValueError("market price must be positive")
        if self._previous_price is not None:
            self._returns.append(abs(event.price - self._previous_price) / self._previous_price)
        self._previous_price = event.price
        mean_return = sum(self._returns, Decimal(0)) / Decimal(len(self._returns) or 1)
        return replace(
            event,
            volatility_bucket=volatility_bucket(mean_return),
            size_bucket=size_bucket(notional) if notional is not None else event.size_bucket,
        )
