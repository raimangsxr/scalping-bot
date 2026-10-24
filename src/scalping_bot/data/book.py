from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal


class BookGap(RuntimeError):
    pass


class LocalOrderBook:
    """Gap-intolerant Binance Spot order-book reconstruction."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._last_update_id: int | None = None
        self._awaiting_first_update = True
        self.valid = False
        self.latency_ms: int | None = None
        self.segment_id: str | None = None

    def apply_snapshot(
        self,
        *,
        last_update_id: int,
        bids: Iterable[tuple[str, str]],
        asks: Iterable[tuple[str, str]],
        exchange_time_ms: int,
        receipt_time_ms: int,
    ) -> None:
        self._bids = _levels(bids)
        self._asks = _levels(asks)
        if not self._bids or not self._asks:
            self.valid = False
            raise BookGap("a valid snapshot must contain both bid and ask levels")
        self._last_update_id = last_update_id
        self._awaiting_first_update = True
        self.valid = True
        self.latency_ms = max(0, receipt_time_ms - exchange_time_ms)
        self.segment_id = f"{self.symbol}:{last_update_id}"

    def apply_depth(
        self,
        *,
        first_update_id: int,
        final_update_id: int,
        bids: Iterable[tuple[str, str]],
        asks: Iterable[tuple[str, str]],
        exchange_time_ms: int,
        receipt_time_ms: int,
    ) -> None:
        if not self.valid or self._last_update_id is None:
            raise BookGap("a new snapshot is required")
        if final_update_id <= self._last_update_id:
            return
        expected = self._last_update_id + 1
        continuous = (
            first_update_id <= expected <= final_update_id
            if self._awaiting_first_update
            else first_update_id == expected
        )
        if not continuous:
            self.valid = False
            raise BookGap(f"depth gap after {self._last_update_id}; a new snapshot is required")
        _apply_levels(self._bids, bids)
        _apply_levels(self._asks, asks)
        if not self._bids or not self._asks:
            self.valid = False
            raise BookGap("depth update removed every bid or ask; a new snapshot is required")
        self._last_update_id = final_update_id
        self._awaiting_first_update = False
        self.latency_ms = max(0, receipt_time_ms - exchange_time_ms)

    @property
    def best_bid(self) -> tuple[Decimal, Decimal]:
        self._require_valid()
        price = max(self._bids)
        return price, self._bids[price]

    @property
    def best_ask(self) -> tuple[Decimal, Decimal]:
        self._require_valid()
        price = min(self._asks)
        return price, self._asks[price]

    def _require_valid(self) -> None:
        if not self.valid:
            raise BookGap("book segment is invalid; a new snapshot is required")

    @property
    def market_data_quality(self) -> str:
        return "valid" if self.valid else "invalid"


def _levels(values: Iterable[tuple[str, str]]) -> dict[Decimal, Decimal]:
    return {Decimal(price): Decimal(quantity) for price, quantity in values if Decimal(quantity)}


def _apply_levels(book: dict[Decimal, Decimal], changes: Iterable[tuple[str, str]]) -> None:
    for raw_price, raw_quantity in changes:
        price, quantity = Decimal(raw_price), Decimal(raw_quantity)
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity
