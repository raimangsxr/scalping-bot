from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from scalping_bot.domain.engine import Engine, StrategyPolicy
from scalping_bot.domain.events import EngineEvent
from scalping_bot.domain.models import (
    AuditDecision,
    EngineResult,
    ExecutionCosts,
    ExitMarket,
    PlaceProtection,
    SubmitEntry,
)
from scalping_bot.domain.state import InMemoryState
from scalping_bot.operations.promotion import LiveShadowGate


@dataclass(frozen=True)
class ShadowProcessResult:
    decisions: tuple[AuditDecision, ...]
    hypothetical_commands: tuple[str, ...]


class LiveShadowRunner:
    """Run the shared event engine without returning an executable command."""

    __slots__ = ("_process_event", "gate")

    def __init__(
        self,
        *,
        policy: StrategyPolicy,
        initial_equity: Decimal,
        gate: LiveShadowGate,
        execution_costs: ExecutionCosts | None = None,
    ) -> None:
        self.gate = gate
        engine = Engine(
            InMemoryState(initial_equity=initial_equity),
            policy,
            execution_costs=execution_costs,
        )

        def process_event(event: EngineEvent) -> EngineResult:
            result = engine.handle(event)
            for command in result.commands:
                if isinstance(command, SubmitEntry):
                    engine.state.pending_entry = None
                elif isinstance(command, ExitMarket):
                    engine.state.pending_exit = None
                elif isinstance(command, PlaceProtection):
                    continue
            return result

        self._process_event: Callable[[EngineEvent], EngineResult] = process_event

    def process(self, event: EngineEvent) -> ShadowProcessResult:
        result = self._process_event(event)
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
        return ShadowProcessResult(tuple(result.decisions), tuple(hypothetical_commands))
