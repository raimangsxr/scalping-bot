from __future__ import annotations

from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import EngineEvent
from scalping_bot.domain.models import EngineResult
from scalping_bot.operations.promotion import LiveShadowGate


class LiveShadowRunner:
    """Run the shared event engine while keeping every command hypothetical."""

    def __init__(self, *, engine: Engine, gate: LiveShadowGate) -> None:
        self.engine = engine
        self.gate = gate

    def process(self, event: EngineEvent) -> EngineResult:
        result = self.engine.handle(event)
        for decision in result.decisions:
            self.gate.record_hypothetical_decision(
                event_time_ms=decision.event_time_ms,
                kind=decision.kind,
            )
        for command in result.commands:
            self.gate.record_hypothetical_decision(
                event_time_ms=event.event_time_ms,
                kind=f"hypothetical:{type(command).__name__}",
            )
        return result
