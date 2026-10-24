from __future__ import annotations

from dataclasses import dataclass

from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import EngineEvent
from scalping_bot.domain.models import AuditDecision, ExitMarket, PlaceProtection, SubmitEntry
from scalping_bot.operations.promotion import LiveShadowGate


@dataclass(frozen=True)
class ShadowProcessResult:
    decisions: tuple[AuditDecision, ...]
    hypothetical_commands: tuple[str, ...]


class LiveShadowRunner:
    """Run the shared event engine without returning an executable command."""

    def __init__(self, *, engine: Engine, gate: LiveShadowGate) -> None:
        self.engine = engine
        self.gate = gate

    def process(self, event: EngineEvent) -> ShadowProcessResult:
        result = self.engine.handle(event)
        hypothetical_commands: list[str] = []
        for decision in result.decisions:
            self.gate.record_hypothetical_decision(
                event_time_ms=decision.event_time_ms,
                kind=decision.kind,
            )
        for command in result.commands:
            kind = f"hypothetical:{type(command).__name__}"
            self.gate.record_hypothetical_decision(
                event_time_ms=event.event_time_ms,
                kind=kind,
            )
            hypothetical_commands.append(kind)
            if isinstance(command, SubmitEntry):
                self.engine.state.pending_entry = None
            elif isinstance(command, ExitMarket):
                self.engine.state.pending_exit = None
            elif isinstance(command, PlaceProtection):
                continue
        return ShadowProcessResult(tuple(result.decisions), tuple(hypothetical_commands))
