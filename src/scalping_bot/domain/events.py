from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, kw_only=True)
class Event:
    event_id: str
    event_time_ms: int


@dataclass(frozen=True, kw_only=True)
class CandleClosed(Event):
    sequence: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    complete: bool


@dataclass(frozen=True, kw_only=True)
class MarketTick(Event):
    price: Decimal


@dataclass(frozen=True, kw_only=True)
class FillReceived(Event):
    client_order_id: str
    price: Decimal
    quantity: Decimal
    commission: Decimal
    commission_asset: str


MarketEvent = CandleClosed | MarketTick | FillReceived
