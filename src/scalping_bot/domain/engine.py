from __future__ import annotations

import hashlib
from dataclasses import replace

from scalping_bot.domain.events import CandleClosed, FillReceived, MarketEvent, MarketTick
from scalping_bot.domain.models import (
    AuditDecision,
    Command,
    EngineResult,
    PlaceProtection,
    Position,
    SubmitEntry,
)
from scalping_bot.domain.risk import position_quantity
from scalping_bot.domain.state import InMemoryState
from scalping_bot.strategies.momentum import MomentumPolicy


class Engine:
    def __init__(self, state: InMemoryState, policy: MomentumPolicy) -> None:
        self.state = state
        self.policy = policy

    def handle(self, event: MarketEvent) -> EngineResult:
        if self.state.already_processed(event.event_id):
            return EngineResult(commands=(), decisions=())
        commands: list[Command] = []
        decisions: list[AuditDecision] = []
        if isinstance(event, CandleClosed):
            self._handle_candle(event, decisions)
        elif isinstance(event, MarketTick):
            self._handle_tick(event, commands, decisions)
        elif isinstance(event, FillReceived):
            self._handle_fill(event, commands, decisions)
        self.state.mark_processed(event.event_id)
        for decision in decisions:
            self.state.append(decision)
        return EngineResult(commands=tuple(commands), decisions=tuple(decisions))

    def _handle_candle(self, event: CandleClosed, decisions: list[AuditDecision]) -> None:
        if self.state.position is not None or self.state.pending_entry is not None:
            return
        proposal = self.policy.observe(event)
        if proposal is None:
            return
        self.state.pending_proposal = proposal
        decisions.append(
            AuditDecision(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                kind="entry-proposed",
                details={
                    "candidate_version": proposal.candidate_version,
                    "source_event_id": proposal.source_event_id,
                    "rationale": dict(proposal.rationale),
                    "dependencies": proposal.market_data_dependencies,
                    "expiry_ms": proposal.expiry_ms,
                },
            )
        )

    def _handle_tick(
        self,
        event: MarketTick,
        commands: list[Command],
        decisions: list[AuditDecision],
    ) -> None:
        proposal = self.state.pending_proposal
        if proposal is None:
            return
        if event.event_time_ms <= proposal.event_time_ms:
            return
        if event.event_time_ms > proposal.expiry_ms:
            self.state.pending_proposal = None
            decisions.append(
                AuditDecision(
                    event_id=event.event_id,
                    event_time_ms=event.event_time_ms,
                    kind="proposal-expired",
                    details={"source_event_id": proposal.source_event_id},
                )
            )
            return
        quantity = position_quantity(
            equity=self.state.current_equity,
            initial_capital=self.state.initial_equity,
            entry_price=event.price,
            invalidation_price=proposal.invalidation_price,
        )
        if quantity <= 0:
            return
        identity = hashlib.sha256(
            f"{proposal.candidate_version}:{proposal.source_event_id}".encode()
        ).hexdigest()[:24]
        command = SubmitEntry(
            client_order_id=f"scalp-{identity}",
            candidate_version=proposal.candidate_version,
            quantity=quantity,
            price_reference=event.price,
            invalidation_price=proposal.invalidation_price,
            rationale=proposal.rationale,
        )
        self.state.pending_entry = command
        self.state.pending_proposal = None
        commands.append(command)
        decisions.append(
            AuditDecision(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                kind="entry-submitted",
                details={
                    "client_order_id": command.client_order_id,
                    "source_event_id": proposal.source_event_id,
                    "quantity": str(quantity),
                    "price_reference": str(event.price),
                },
            )
        )

    def _handle_fill(
        self,
        event: FillReceived,
        commands: list[Command],
        decisions: list[AuditDecision],
    ) -> None:
        entry = self.state.pending_entry
        if entry is None or entry.client_order_id != event.client_order_id:
            return
        position = Position(
            candidate_version=entry.candidate_version,
            source_event_id=_source_event(self.state, entry.client_order_id),
            entry_fill_id=event.event_id,
            entry_time_ms=event.event_time_ms,
            entry_price=event.price,
            quantity=event.quantity,
            invalidation_price=entry.invalidation_price,
            commission=event.commission,
        )
        protection = PlaceProtection(
            client_order_id=f"{entry.client_order_id}-protect",
            parent_client_order_id=entry.client_order_id,
            quantity=event.quantity,
            stop_price=entry.invalidation_price,
        )
        self.state.position = replace(position, protected=True)
        self.state.pending_entry = None
        commands.append(protection)
        decisions.append(
            AuditDecision(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                kind="entry-filled",
                details={
                    "client_order_id": entry.client_order_id,
                    "fill_id": event.event_id,
                    "price": str(event.price),
                    "quantity": str(event.quantity),
                    "commission": str(event.commission),
                    "commission_asset": event.commission_asset,
                    "protection_client_order_id": protection.client_order_id,
                },
            )
        )


def _source_event(state: InMemoryState, client_order_id: str) -> str:
    for decision in reversed(state.audit):
        if (
            decision.kind == "entry-submitted"
            and decision.details.get("client_order_id") == client_order_id
        ):
            return str(decision.details["source_event_id"])
    raise RuntimeError("entry intent has no source event")
