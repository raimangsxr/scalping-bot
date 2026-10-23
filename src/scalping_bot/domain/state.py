from __future__ import annotations

from decimal import Decimal

from scalping_bot.domain.models import AuditDecision, EntryProposal, Position, SubmitEntry


class InMemoryState:
    def __init__(self, *, initial_equity: Decimal) -> None:
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.pending_proposal: EntryProposal | None = None
        self.pending_entry: SubmitEntry | None = None
        self.position: Position | None = None
        self.audit: list[AuditDecision] = []
        self._processed_event_ids: set[str] = set()

    def already_processed(self, event_id: str) -> bool:
        return event_id in self._processed_event_ids

    def append(self, decision: AuditDecision) -> None:
        self.audit.append(decision)

    def mark_processed(self, event_id: str) -> None:
        self._processed_event_ids.add(event_id)
