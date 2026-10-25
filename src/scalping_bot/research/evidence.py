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
    market_data_dependencies: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ValueError("candidate versions must be positive")
        if not self.candidate_id or not self.family:
            raise ValueError("candidate identity and strategy family are required")
        if not self.source_uri.startswith(("https://", "http://", "test://")):
            raise ValueError("candidate source provenance must be an HTTP(S) URI")
        if not self.source_license or not self.thesis:
            raise ValueError("candidate provenance and causal thesis are required")
        if not self.original_rules or not self.parameters or not self.rejection_criteria:
            raise ValueError(
                "candidate rules, preregistered parameters, and rejection criteria are required"
            )
        if not self.market_data_dependencies:
            dependency = (
                "aggTrades" if self.family == "executed-trade-pressure" else "closed-1m-klines"
            )
            object.__setattr__(self, "market_data_dependencies", (dependency,))

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
    market_fidelity: str = "aggTrades"
    equity_marks: tuple[tuple[int, Decimal], ...] = ()
    regime: str | None = None

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
    cost_stress_multipliers: tuple[Decimal, ...] = ()
    cost_scenario_metrics: Mapping[Decimal, EvidenceMetrics] = field(default_factory=dict)
    comparators: Mapping[str, Decimal | None] = field(
        default_factory=lambda: {"no_trade": Decimal(0), "buy_and_hold": None, "momentum": None}
    )
    baseline_metrics: EvidenceMetrics | None = None
    incremental_net_return: Decimal | None = None
    fold_metrics: Mapping[int, EvidenceMetrics] = field(default_factory=dict)
    regime_metrics: Mapping[str, EvidenceMetrics] = field(default_factory=dict)
    market_fidelity_verified: bool = False
    baseline_cost_scenario_metrics: Mapping[Decimal, EvidenceMetrics] = field(default_factory=dict)
    frozen_parameters: Mapping[str, object] | None = None
    dataset_checksum: str | None = None
    execution_cost_model_identity: str | None = None


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
        cost_stress_multipliers: tuple[Decimal, ...] = (),
        cost_scenario_trades: Mapping[Decimal, tuple[ScalpResult, ...]] | None = None,
        baseline_trades: tuple[ScalpResult, ...] | None = None,
        baseline_cost_scenario_trades: Mapping[Decimal, tuple[ScalpResult, ...]] | None = None,
        fold_count_override: int | None = None,
        frozen_parameters: Mapping[str, object] | None = None,
        dataset_checksum: str | None = None,
        execution_cost_model_identity: str | None = None,
    ) -> EvidenceResult:
        if attempts < 1:
            raise ValueError("selection attempt count must be positive")
        metrics = _metrics(trades, initial_equity)
        alpha = Decimal("0.05") / Decimal(attempts)
        lower = _daily_block_bootstrap(trades, alpha=alpha, seed=random_seed)
        by_fold: dict[int, Decimal] = defaultdict(Decimal)
        for trade in trades:
            by_fold[trade.fold] += trade.net_pnl
        fold_count = fold_count_override or len(self.walk_forward_folds())
        if fold_count < 1:
            raise ValueError("evidence evaluation requires at least one fold")
        positive_fraction = Decimal(
            sum(by_fold.get(index, Decimal(0)) > 0 for index in range(fold_count))
        ) / Decimal(fold_count)
        concentration_failed = _concentration_failed(trades)
        baseline_metrics = (
            _metrics(baseline_trades, initial_equity) if baseline_trades is not None else None
        )
        fold_metrics = {
            fold: _metrics(tuple(trade for trade in trades if trade.fold == fold), initial_equity)
            for fold in sorted({trade.fold for trade in trades})
        }
        regime_metrics = {
            regime: _metrics(
                tuple(trade for trade in trades if trade.regime == regime), initial_equity
            )
            for regime in sorted({trade.regime for trade in trades if trade.regime is not None})
        }
        required_stress = {Decimal("1"), Decimal("1.5"), Decimal("2")}
        promoted = (
            len(trades) >= 300
            and positive_fraction >= Decimal("0.70")
            and lower is not None
            and lower >= 0
            and not concentration_failed
            and metrics.max_drawdown <= Decimal("0.10")
            and metrics.maximum_daily_loss <= Decimal("0.02")
            and all(trade.risk_compliant for trade in trades)
            and all(trade.market_fidelity == "aggTrades" for trade in trades)
            and required_stress.issubset(cost_stress_multipliers)
            and required_stress.issubset((cost_scenario_trades or {}).keys())
        )
        return EvidenceResult(
            metrics=metrics,
            bootstrap_lower_95=lower,
            positive_fold_fraction=positive_fraction,
            concentration_failed=concentration_failed,
            promoted=promoted,
            selection_adjusted_alpha=alpha,
            cost_stress_multipliers=cost_stress_multipliers,
            cost_scenario_metrics={
                multiplier: _metrics(scenario, initial_equity)
                for multiplier, scenario in (cost_scenario_trades or {}).items()
            },
            comparators=comparators
            or {"no_trade": Decimal(0), "buy_and_hold": None, "momentum": None},
            baseline_metrics=baseline_metrics,
            incremental_net_return=(
                metrics.net_return - baseline_metrics.net_return
                if baseline_metrics is not None
                else None
            ),
            fold_metrics=fold_metrics,
            regime_metrics=regime_metrics,
            market_fidelity_verified=bool(trades)
            and all(trade.market_fidelity == "aggTrades" for trade in trades),
            baseline_cost_scenario_metrics={
                multiplier: _metrics(scenario, initial_equity)
                for multiplier, scenario in (baseline_cost_scenario_trades or {}).items()
            },
            frozen_parameters=(dict(frozen_parameters) if frozen_parameters is not None else None),
            dataset_checksum=dataset_checksum,
            execution_cost_model_identity=execution_cost_model_identity,
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
            "candidate_sha256": _candidate_sha256(candidate),
            "command": command,
            "period": self.PERIOD,
        }
        self.store.put_immutable(
            key, (json.dumps(audit, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )
        return key

    def was_revealed(self, candidate: CandidateRecord) -> bool:
        """Return whether this exact frozen candidate version has an audited reveal."""
        key = f"holdout-access/{candidate.identity}.json"
        try:
            audit = json.loads(self.store.read(key))
        except FileNotFoundError:
            return False
        if not isinstance(audit, dict):
            raise HoldoutAccessError("Final Holdout audit is malformed")
        if audit.get("candidate_sha256") != _candidate_sha256(candidate):
            raise HoldoutAccessError(
                "Final Holdout audit does not match the frozen candidate version"
            )
        return True


def _metrics(trades: tuple[ScalpResult, ...], initial_equity: Decimal) -> EvidenceMetrics:
    net = [trade.net_pnl for trade in trades]
    total = sum(net, Decimal(0))
    realized_equity = initial_equity
    peak = initial_equity
    max_drawdown = Decimal(0)
    by_day: dict[str, Decimal] = defaultdict(Decimal)
    for trade in sorted(trades, key=lambda item: item.closed_ms):
        marks = trade.equity_marks or (
            (trade.opened_ms, trade.entry_price),
            (trade.closed_ms, trade.exit_price),
        )
        for _mark_ms, mark_price in marks:
            marked_equity = (
                realized_equity
                + (mark_price - trade.entry_price) * trade.quantity
                - trade.costs / 2
            )
            peak = max(peak, marked_equity)
            if peak:
                max_drawdown = max(max_drawdown, (peak - marked_equity) / peak)
        realized_equity += trade.net_pnl
        peak = max(peak, realized_equity)
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


def _candidate_sha256(candidate: CandidateRecord) -> str:
    canonical = json.dumps(
        {
            "candidate_id": candidate.candidate_id,
            "version": candidate.version,
            "family": candidate.family,
            "source_uri": candidate.source_uri,
            "source_license": candidate.source_license,
            "thesis": candidate.thesis,
            "original_rules": dict(candidate.original_rules),
            "modifications": dict(candidate.modifications),
            "parameters": dict(candidate.parameters),
            "rejection_criteria": candidate.rejection_criteria,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


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
