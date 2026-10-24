from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from scalping_bot.domain.costs import ExecutionCosts

__all__ = [
    "AuditDecision",
    "Command",
    "EngineResult",
    "EntryProposal",
    "ExecutionCosts",
    "ExitMarket",
    "PlaceProtection",
    "Position",
    "SubmitEntry",
]


@dataclass(frozen=True)
class EntryProposal:
    candidate_version: str
    source_event_id: str
    event_time_ms: int
    direction: str
    rationale: Mapping[str, str]
    feature_snapshot: Mapping[str, str]
    invalidation_price: Decimal
    expiry_ms: int
    market_data_dependencies: tuple[str, ...]


@dataclass(frozen=True)
class AuditDecision:
    event_id: str
    event_time_ms: int
    kind: str
    details: Mapping[str, object]


@dataclass(frozen=True)
class SubmitEntry:
    client_order_id: str
    candidate_version: str
    source_event_id: str
    quantity: Decimal
    price_reference: Decimal
    invalidation_price: Decimal
    rationale: Mapping[str, str]


@dataclass(frozen=True)
class PlaceProtection:
    client_order_id: str
    parent_client_order_id: str
    quantity: Decimal
    stop_price: Decimal
    take_profit_price: Decimal


@dataclass(frozen=True)
class ExitMarket:
    client_order_id: str
    quantity: Decimal
    reason: str


Command = SubmitEntry | PlaceProtection | ExitMarket


@dataclass(frozen=True)
class Position:
    candidate_version: str
    source_event_id: str
    entry_fill_id: str
    entry_time_ms: int
    entry_price: Decimal
    quantity: Decimal
    invalidation_price: Decimal
    commission: Decimal
    protected: bool = False


@dataclass(frozen=True)
class EngineResult:
    commands: tuple[Command, ...]
    decisions: tuple[AuditDecision, ...]
