from __future__ import annotations

from decimal import Decimal

from scalping_bot.domain.models import (
    AuditDecision,
    EntryProposal,
    ExitMarket,
    Position,
    SubmitEntry,
)


class InMemoryState:
    def __init__(self, *, initial_equity: Decimal) -> None:
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.pending_proposal: EntryProposal | None = None
        self.pending_entry: SubmitEntry | None = None
        self.pending_exit: ExitMarket | None = None
        self.position: Position | None = None
        self.last_exit_time_ms: int | None = None
        self.utc_day = 0
        self.day_open_equity = initial_equity
        self.high_water_mark = initial_equity
        self.daily_locked = False
        self.drawdown_locked = False
        self.operator_confirmed_day: int | None = None
        self.entries_allowed = True
        self.dependency_status: dict[str, bool] = {}
        self.audit: list[AuditDecision] = []
        self._processed_event_ids: set[str] = set()

    def already_processed(self, event_id: str) -> bool:
        return event_id in self._processed_event_ids

    def append(self, decision: AuditDecision) -> None:
        self.audit.append(decision)

    def mark_processed(self, event_id: str) -> None:
        self._processed_event_ids.add(event_id)
