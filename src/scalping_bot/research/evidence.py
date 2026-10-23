from __future__ import annotations

import hashlib
import json
import math
import random
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from statistics import fmean, pstdev

from scalping_bot.data.storage import ObjectStore


class CandidateLifecycle(StrEnum):
    PROPOSED = "proposed"
    SPECIFIED = "specified"
    SCREENED = "screened"
    WALK_FORWARD_VALIDATED = "walk-forward-validated"
    HOLDOUT_TESTED = "holdout-tested"
    TESTNET_READY = "testnet-ready"
    REJECTED = "rejected"


@dataclass(frozen=True)
class LifecycleTransition:
    previous: CandidateLifecycle
    current: CandidateLifecycle
    evidence: str


@dataclass(frozen=True)
class CandidateRecord:
    candidate_id: str
    version: int
    family: str
    source_uri: str
    source_license: str
    thesis: str
    original_rules: Mapping[str, object]
    modifications: Mapping[str, object]
    parameters: Mapping[str, object]
    rejection_criteria: tuple[str, ...]
    lifecycle: CandidateLifecycle = CandidateLifecycle.PROPOSED
    history: tuple[LifecycleTransition, ...] = ()
    frozen: bool = False

    @property
    def identity(self) -> str:
        return f"{self.candidate_id}-v{self.version}"

    def transition(self, target: CandidateLifecycle, *, evidence: str) -> CandidateRecord:
        if self.lifecycle is CandidateLifecycle.REJECTED:
            raise ValueError("rejected is a terminal candidate lifecycle")
        ordered = (
            CandidateLifecycle.PROPOSED,
            CandidateLifecycle.SPECIFIED,
            CandidateLifecycle.SCREENED,
            CandidateLifecycle.WALK_FORWARD_VALIDATED,
            CandidateLifecycle.HOLDOUT_TESTED,
            CandidateLifecycle.TESTNET_READY,
        )
        if target is not CandidateLifecycle.REJECTED:
            current_index = ordered.index(self.lifecycle)
            if current_index + 1 >= len(ordered) or ordered[current_index + 1] is not target:
                raise ValueError(f"invalid lifecycle transition: {self.lifecycle} -> {target}")
        transition = LifecycleTransition(self.lifecycle, target, evidence)
        return replace(self, lifecycle=target, history=(*self.history, transition))

    def freeze(self) -> CandidateRecord:
        if self.lifecycle is not CandidateLifecycle.WALK_FORWARD_VALIDATED:
            raise ValueError("only a walk-forward-validated finalist can be frozen")
        return replace(self, frozen=True)


@dataclass(frozen=True)
class ExperimentManifest:
    code_commit: str
    candidate_identity: str
    configuration: Mapping[str, object]
    dataset_checksums: tuple[str, ...]
    commission_snapshot: Mapping[str, str]
    execution_cost_model: Mapping[str, str]
    period: tuple[str, str]
    random_seed: int
    schema_version: int = 1

    def canonical_bytes(self) -> bytes:
        return (json.dumps(asdict(self), sort_keys=True, separators=(",", ":")) + "\n").encode()

    def persist(self, store: ObjectStore) -> str:
        content = self.canonical_bytes()
        identity = hashlib.sha256(content).hexdigest()
        key = f"experiments/{self.candidate_identity}/{identity}.json"
        store.put_immutable(key, content)
        return key


@dataclass(frozen=True)
class WalkForwardFold:
    development: tuple[str, str]
    validation: tuple[str, str]


@dataclass(frozen=True)
class ScalpResult:
    scalp_id: str
    opened_ms: int
    closed_ms: int
    entry_price: Decimal
    exit_price: Decimal
    quantity: Decimal
    gross_pnl: Decimal
    costs: Decimal
    fold: int
    trace_event_ids: tuple[str, ...] = ()
    risk_compliant: bool = True

    @property
    def net_pnl(self) -> Decimal:
        return self.gross_pnl - self.costs


@dataclass(frozen=True)
class EvidenceMetrics:
    net_return: Decimal
    max_drawdown: Decimal
    maximum_daily_loss: Decimal
    expectancy: Decimal | None
    profit_factor: Decimal | None
    daily_sharpe: Decimal | None
    daily_sortino: Decimal | None
    win_rate: Decimal | None
    exposure: Decimal
    turnover: Decimal
    trade_count: int


@dataclass(frozen=True)
class EvidenceResult:
    metrics: EvidenceMetrics
    bootstrap_lower_95: Decimal | None
    positive_fold_fraction: Decimal
    concentration_failed: bool
    promoted: bool
    selection_adjusted_alpha: Decimal
    comparators: Mapping[str, Decimal | None] = field(
        default_factory=lambda: {"no_trade": Decimal(0), "buy_and_hold": None, "momentum": None}
    )


class EvidenceProtocol:
    PRIMARY_START = (2020, 1)
    PRIMARY_END = (2025, 9)

    def walk_forward_folds(self) -> tuple[WalkForwardFold, ...]:
        folds: list[WalkForwardFold] = []
        start_index = _month_index(*self.PRIMARY_START)
        end_index = _month_index(*self.PRIMARY_END)
        while start_index + 14 <= end_index:
            folds.append(
                WalkForwardFold(
                    development=(_month_label(start_index), _month_label(start_index + 11)),
                    validation=(_month_label(start_index + 12), _month_label(start_index + 14)),
                )
            )
            start_index += 3
        return tuple(folds)

    def evaluate(
        self,
        trades: tuple[ScalpResult, ...],
        *,
        initial_equity: Decimal,
        attempts: int,
        random_seed: int = 7,
        comparators: Mapping[str, Decimal | None] | None = None,
    ) -> EvidenceResult:
        if attempts < 1:
            raise ValueError("selection attempt count must be positive")
        metrics = _metrics(trades, initial_equity)
        alpha = Decimal("0.05") / Decimal(attempts)
        lower = _daily_block_bootstrap(trades, alpha=alpha, seed=random_seed)
        by_fold: dict[int, Decimal] = defaultdict(Decimal)
        for trade in trades:
            by_fold[trade.fold] += trade.net_pnl
        positive_fraction = (
            Decimal(sum(value > 0 for value in by_fold.values())) / Decimal(len(by_fold))
            if by_fold
            else Decimal(0)
        )
        concentration_failed = _concentration_failed(trades)
        promoted = (
            len(trades) >= 300
            and positive_fraction >= Decimal("0.70")
            and lower is not None
            and lower >= 0
            and not concentration_failed
            and all(trade.risk_compliant for trade in trades)
        )
        return EvidenceResult(
            metrics=metrics,
            bootstrap_lower_95=lower,
            positive_fold_fraction=positive_fraction,
            concentration_failed=concentration_failed,
            promoted=promoted,
            selection_adjusted_alpha=alpha,
            comparators=comparators
            or {"no_trade": Decimal(0), "buy_and_hold": None, "momentum": None},
        )


class HoldoutAccessError(RuntimeError):
    pass


class HoldoutRegistry:
    PERIOD = ("2025-10-01", "2026-09-30")

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def reveal(self, candidate: CandidateRecord, *, command: str, actor: str) -> str:
        if command != "reveal-final-holdout":
            raise HoldoutAccessError("Final Holdout requires the explicit audited command")
        if (
            not candidate.frozen
            or candidate.lifecycle is not CandidateLifecycle.WALK_FORWARD_VALIDATED
        ):
            raise HoldoutAccessError("candidate must be a frozen walk-forward finalist")
        key = f"holdout-access/{candidate.identity}.json"
        try:
            self.store.read(key)
        except FileNotFoundError:
            pass
        else:
            raise HoldoutAccessError(
                "Final Holdout was already revealed for this candidate version"
            )
        audit = {
            "actor": actor,
            "candidate_identity": candidate.identity,
            "command": command,
            "period": self.PERIOD,
        }
        self.store.put_immutable(
            key, (json.dumps(audit, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )
        return key


def _metrics(trades: tuple[ScalpResult, ...], initial_equity: Decimal) -> EvidenceMetrics:
    net = [trade.net_pnl for trade in trades]
    total = sum(net, Decimal(0))
    equity = initial_equity
    peak = initial_equity
    max_drawdown = Decimal(0)
    by_day: dict[str, Decimal] = defaultdict(Decimal)
    for trade in sorted(trades, key=lambda item: item.closed_ms):
        equity += trade.net_pnl
        peak = max(peak, equity)
        if peak:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
        day = datetime.fromtimestamp(trade.closed_ms / 1000, tz=UTC).date().isoformat()
        by_day[day] += trade.net_pnl
    wins = sum((value for value in net if value > 0), Decimal(0))
    losses = -sum((value for value in net if value < 0), Decimal(0))
    daily = [float(value / initial_equity) for value in by_day.values()]
    mean_daily = fmean(daily) if daily else None
    daily_std = pstdev(daily) if len(daily) > 1 else 0.0
    downside = [min(value, 0.0) for value in daily]
    downside_std = math.sqrt(fmean(value * value for value in downside)) if downside else 0.0
    span = max((trade.closed_ms for trade in trades), default=0) - min(
        (trade.opened_ms for trade in trades), default=0
    )
    held = sum((trade.closed_ms - trade.opened_ms for trade in trades), 0)
    turnover = sum(
        ((trade.entry_price + trade.exit_price) * trade.quantity for trade in trades), Decimal(0)
    )
    return EvidenceMetrics(
        net_return=total / initial_equity,
        max_drawdown=max_drawdown,
        maximum_daily_loss=max(
            (-value for value in by_day.values() if value < 0), default=Decimal(0)
        )
        / initial_equity,
        expectancy=(total / len(trades)) if trades else None,
        profit_factor=(wins / losses) if losses else None,
        daily_sharpe=(Decimal(str(mean_daily / daily_std * math.sqrt(365))))
        if mean_daily is not None and daily_std
        else None,
        daily_sortino=(Decimal(str(mean_daily / downside_std * math.sqrt(365))))
        if mean_daily is not None and downside_std
        else None,
        win_rate=(Decimal(sum(value > 0 for value in net)) / len(net)) if net else None,
        exposure=Decimal(held) / Decimal(span) if span > 0 else Decimal(0),
        turnover=turnover / initial_equity,
        trade_count=len(trades),
    )


def _daily_block_bootstrap(
    trades: tuple[ScalpResult, ...], *, alpha: Decimal, seed: int, samples: int = 2_000
) -> Decimal | None:
    if not trades:
        return None
    groups: dict[str, list[Decimal]] = defaultdict(list)
    for trade in trades:
        day = datetime.fromtimestamp(trade.closed_ms / 1000, tz=UTC).date().isoformat()
        groups[day].append(trade.net_pnl)
    days = list(groups.values())
    generator = random.Random(seed)
    expectancies: list[Decimal] = []
    for _ in range(samples):
        sample = [generator.choice(days) for _ in days]
        values = [value for day in sample for value in day]
        expectancies.append(sum(values, Decimal(0)) / len(values))
    expectancies.sort()
    index = max(0, min(len(expectancies) - 1, int(float(alpha) * len(expectancies))))
    return expectancies[index].normalize()


def _concentration_failed(trades: tuple[ScalpResult, ...]) -> bool:
    total_profit = sum((trade.net_pnl for trade in trades), Decimal(0))
    if total_profit <= 0:
        return True
    if any(trade.net_pnl > total_profit * Decimal("0.10") for trade in trades):
        return True
    months: dict[str, Decimal] = defaultdict(Decimal)
    for trade in trades:
        month = datetime.fromtimestamp(trade.closed_ms / 1000, tz=UTC).strftime("%Y-%m")
        months[month] += trade.net_pnl
    return any(value > total_profit * Decimal("0.25") for value in months.values())


def _month_index(year: int, month: int) -> int:
    return year * 12 + month - 1


def _month_label(index: int) -> str:
    year, zero_month = divmod(index, 12)
    return f"{year:04d}-{zero_month + 1:02d}"
