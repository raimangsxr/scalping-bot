from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Protocol

from scalping_bot.domain.events import (
    CandleClosed,
    EngineEvent,
    EntryFillUpdated,
    ExitFillReceived,
    FillReceived,
    MarketTick,
    OperatorConfirmedNewDay,
)
from scalping_bot.domain.models import (
    AuditDecision,
    Command,
    EngineResult,
    EntryProposal,
    ExecutionCosts,
    ExitMarket,
    PlaceProtection,
    Position,
    SubmitEntry,
)
from scalping_bot.domain.risk import position_quantity
from scalping_bot.domain.state import InMemoryState


class StrategyPolicy(Protocol):
    def observe(self, event: CandleClosed) -> EntryProposal | None: ...


class Engine:
    def __init__(
        self,
        state: InMemoryState,
        policy: StrategyPolicy,
        *,
        execution_costs: ExecutionCosts | None = None,
    ) -> None:
        self.state = state
        self.policy = policy
        self.execution_costs = execution_costs or ExecutionCosts(commission_rate=Decimal(0))

    def handle(self, event: EngineEvent) -> EngineResult:
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
        elif isinstance(event, EntryFillUpdated):
            self._handle_fill_update(event, commands, decisions)
        elif isinstance(event, ExitFillReceived):
            self._handle_exit_fill(event, decisions)
        elif isinstance(event, OperatorConfirmedNewDay):
            self._handle_new_day_confirmation(event, decisions)
        self.state.mark_processed(event.event_id)
        for decision in decisions:
            self.state.append(decision)
        return EngineResult(commands=tuple(commands), decisions=tuple(decisions))

    def _handle_candle(self, event: CandleClosed, decisions: list[AuditDecision]) -> None:
        if (
            self.state.position is not None
            or self.state.pending_entry is not None
            or self.state.pending_exit is not None
            or self.state.daily_locked
            or self.state.drawdown_locked
        ):
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
        if self.state.position is not None:
            self._evaluate_open_position(event, commands, decisions)
            return
        proposal = self.state.pending_proposal
        if proposal is None:
            return
        if self.state.daily_locked or self.state.drawdown_locked:
            self.state.pending_proposal = None
            return
        if (
            self.state.last_exit_time_ms is not None
            and event.event_time_ms < self.state.last_exit_time_ms + 60_000
        ):
            decisions.append(
                AuditDecision(
                    event_id=event.event_id,
                    event_time_ms=event.event_time_ms,
                    kind="entry-blocked-cooldown",
                    details={"until_ms": self.state.last_exit_time_ms + 60_000},
                )
            )
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
            source_event_id=proposal.source_event_id,
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
            source_event_id=entry.source_event_id,
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
            take_profit_price=event.price + 2 * (event.price - entry.invalidation_price),
        )
        self.state.position = position
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

    def _evaluate_open_position(
        self,
        event: MarketTick,
        commands: list[Command],
        decisions: list[AuditDecision],
    ) -> None:
        position = self.state.position
        if position is None or self.state.pending_exit is not None:
            return
        estimated_close_commission = self.execution_costs.estimated_close_cost(
            event.price * position.quantity
        )
        mark_equity = (
            self.state.current_equity
            + (event.price - position.entry_price) * position.quantity
            - position.commission
            - estimated_close_commission
        )
        self.state.high_water_mark = max(self.state.high_water_mark, mark_equity)
        held_ms = event.event_time_ms - position.entry_time_ms
        reason: str | None = None
        if event.price <= position.invalidation_price:
            reason = "protective-stop"
        elif mark_equity <= self.state.high_water_mark * Decimal("0.90"):
            self.state.drawdown_locked = True
            reason = "drawdown-circuit-breaker"
        elif mark_equity <= self.state.day_open_equity * Decimal("0.98"):
            self.state.daily_locked = True
            reason = "daily-circuit-breaker"
        elif held_ms >= 30 * 60_000:
            reason = "timed-force"
        elif held_ms >= 25 * 60_000 and mark_equity >= self.state.current_equity:
            reason = "timed-break-even"
        if reason is None:
            return
        identity = hashlib.sha256(
            f"exit:{position.entry_fill_id}:{reason}:{event.event_id}".encode()
        ).hexdigest()[:24]
        command = ExitMarket(
            client_order_id=f"scalp-{identity}", quantity=position.quantity, reason=reason
        )
        self.state.pending_exit = command
        commands.append(command)
        decisions.append(
            AuditDecision(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                kind="exit-submitted",
                details={
                    "client_order_id": command.client_order_id,
                    "reason": reason,
                    "quantity": str(command.quantity),
                    "mark_equity": str(mark_equity),
                },
            )
        )

    def _handle_fill_update(
        self,
        event: EntryFillUpdated,
        commands: list[Command],
        decisions: list[AuditDecision],
    ) -> None:
        position = self.state.position
        if position is None or event.cumulative_quantity <= position.quantity:
            return
        updated = Position(
            candidate_version=position.candidate_version,
            source_event_id=position.source_event_id,
            entry_fill_id=position.entry_fill_id,
            entry_time_ms=position.entry_time_ms,
            entry_price=event.price,
            quantity=event.cumulative_quantity,
            invalidation_price=position.invalidation_price,
            commission=event.cumulative_commission,
            protected=False,
        )
        self.state.position = updated
        commands.append(
            PlaceProtection(
                client_order_id=f"{event.client_order_id}-protect",
                parent_client_order_id=event.client_order_id,
                quantity=event.cumulative_quantity,
                stop_price=position.invalidation_price,
                take_profit_price=event.price + 2 * (event.price - position.invalidation_price),
            )
        )
        decisions.append(
            AuditDecision(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                kind="entry-fill-updated",
                details={"cumulative_quantity": str(event.cumulative_quantity)},
            )
        )

    def _handle_exit_fill(self, event: ExitFillReceived, decisions: list[AuditDecision]) -> None:
        exit_order, position = self.state.pending_exit, self.state.position
        if (
            exit_order is None
            or position is None
            or event.client_order_id != exit_order.client_order_id
        ):
            return
        pnl = (
            (event.price - position.entry_price) * event.quantity
            - position.commission
            - event.commission
        )
        self.state.current_equity += pnl
        self.state.position = None
        self.state.pending_exit = None
        self.state.last_exit_time_ms = event.event_time_ms
        decisions.append(
            AuditDecision(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                kind="position-closed",
                details={
                    "reason": exit_order.reason,
                    "price": str(event.price),
                    "quantity": str(event.quantity),
                    "net_pnl": str(pnl),
                    "equity": str(self.state.current_equity),
                },
            )
        )

    def _handle_new_day_confirmation(
        self, event: OperatorConfirmedNewDay, decisions: list[AuditDecision]
    ) -> None:
        if self.state.drawdown_locked or event.utc_day <= self.state.utc_day:
            return
        self.state.utc_day = event.utc_day
        self.state.day_open_equity = self.state.current_equity
        self.state.daily_locked = False
        self.state.operator_confirmed_day = event.utc_day
        decisions.append(
            AuditDecision(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                kind="new-utc-day-confirmed",
                details={"utc_day": event.utc_day},
            )
        )
