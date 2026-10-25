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
    volume: Decimal = Decimal(0)
    buy_volume: Decimal = Decimal(0)
    sell_volume: Decimal = Decimal(0)
    trade_count: int = 0


@dataclass(frozen=True, kw_only=True)
class MarketTick(Event):
    price: Decimal
    volatility_bucket: str | None = None
    size_bucket: str | None = None


@dataclass(frozen=True, kw_only=True)
class FillReceived(Event):
    client_order_id: str
    price: Decimal
    quantity: Decimal
    commission: Decimal
    commission_asset: str


@dataclass(frozen=True, kw_only=True)
class EntryFillUpdated(Event):
    client_order_id: str
    price: Decimal
    cumulative_quantity: Decimal
    cumulative_commission: Decimal
    commission_asset: str


@dataclass(frozen=True, kw_only=True)
class ExitFillReceived(Event):
    client_order_id: str
    price: Decimal
    quantity: Decimal
    commission: Decimal


@dataclass(frozen=True, kw_only=True)
class OperatorConfirmedNewDay(Event):
    utc_day: int


EngineEvent = (
    CandleClosed
    | MarketTick
    | FillReceived
    | EntryFillUpdated
    | ExitFillReceived
    | OperatorConfirmedNewDay
)
