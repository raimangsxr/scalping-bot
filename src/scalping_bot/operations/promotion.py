from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal

from scalping_bot.data.storage import ObjectStore

DAY_MS = 86_400_000


@dataclass(frozen=True)
class LiveReplayComparison:
    event_time_ms: int
    live_features: dict[str, str]
    replay_features: dict[str, str]
    live_signal: bool
    replay_signal: bool
    expiry_match: bool
    latency_ms: int
    data_quality: str
    hypothetical_cost: str
    divergence: str | None = None


@dataclass(frozen=True)
class DegradedShadowDecision:
    mode: str
    hypothetical_entry: bool
    dependency: str


@dataclass(frozen=True)
class LiveShadowResult:
    approved: bool
    reasons: tuple[str, ...]
    report: str


class LiveShadowGate:
    def __init__(self, *, candidate_identity: str, started_ms: int) -> None:
        self.candidate_identity = candidate_identity
        self.started_ms = started_ms
        self.comparisons: list[LiveReplayComparison] = []
        self.resolutions: dict[str, tuple[str, str]] = {}
        self.continuity_broken = False

    def record(self, comparison: LiveReplayComparison) -> None:
        if self.comparisons and comparison.event_time_ms <= self.comparisons[-1].event_time_ms:
            raise ValueError("Live Shadow observations must be chronological")
        self.comparisons.append(comparison)

    def missing_dependency(self, *, event_time_ms: int, dependency: str) -> DegradedShadowDecision:
        self.comparisons.append(
            LiveReplayComparison(
                event_time_ms=event_time_ms,
                live_features={},
                replay_features={},
                live_signal=False,
                replay_signal=False,
                expiry_match=True,
                latency_ms=0,
                data_quality=f"missing:{dependency}",
                hypothetical_cost="non-evaluable",
                divergence=f"missing dependency: {dependency}",
            )
        )
        return DegradedShadowDecision("DEGRADED", False, dependency)

    def resolve_divergence(self, divergence: str, *, resolution: str, regression: str) -> None:
        if not resolution or not regression:
            raise ValueError("a divergence resolution requires regression coverage")
        self.resolutions[divergence] = (resolution, regression)

    def evaluate(self, *, now_ms: int) -> LiveShadowResult:
        reasons: list[str] = []
        if now_ms - self.started_ms < 14 * DAY_MS:
            reasons.append("fewer than fourteen continuous calendar days")
        if self.continuity_broken:
            reasons.append("Live Shadow continuity was broken")
        unresolved = {
            item.divergence
            for item in self.comparisons
            if item.divergence and item.divergence not in self.resolutions
        }
        if unresolved:
            reasons.append(f"unresolved divergences: {', '.join(sorted(unresolved))}")
        if not self.comparisons:
            reasons.append("no live/replay comparisons were recorded")
        approved = not reasons
        report = (
            f"# Live Shadow — {self.candidate_identity}\n\n"
            f"Decision: {'APPROVE Testnet Soak' if approved else 'REJECT progression'}\n\n"
            f"Comparisons: {len(self.comparisons)}\n"
            f"Feature/signal/expiry/latency/data-quality/cost divergences unresolved: "
            f"{len(unresolved)}\n"
        )
        return LiveShadowResult(approved, tuple(reasons), report)


@dataclass(frozen=True)
class SoakResult:
    complete: bool
    reasons: tuple[str, ...]
    report: str


class SoakGate:
    def __init__(self, *, candidate_identity: str, started_ms: int) -> None:
        self.candidate_identity = candidate_identity
        self.started_ms = started_ms
        self.continuous_started_ms: int | None = None
        self.last_critical_incident_ms: int | None = None
        self.scalps = 0
        self.safety_failures = 0
        self.testnet_pnl = Decimal(0)

    def begin_continuous(self, *, now_ms: int, alerts_verified: bool) -> None:
        stable_since = self.last_critical_incident_ms or self.started_ms
        if now_ms - stable_since < 7 * DAY_MS:
            raise ValueError("seven incident-free days are required before continuous operation")
        if not alerts_verified:
            raise ValueError("alerts must be verified before continuous operation")
        self.continuous_started_ms = now_ms

    def record_scalp(
        self,
        *,
        event_time_ms: int,
        protected: bool,
        duplicate_order: bool,
        reconciled: bool,
        testnet_pnl: Decimal,
    ) -> None:
        del event_time_ms
        self.scalps += 1
        self.testnet_pnl += testnet_pnl
        if not protected or duplicate_order or not reconciled:
            self.safety_failures += 1

    def record_incident(
        self, *, event_time_ms: int, critical: bool, regression: str | None
    ) -> None:
        if critical and not regression:
            raise ValueError("a critical incident requires a replayable regression")
        if critical:
            self.last_critical_incident_ms = event_time_ms
            self.continuous_started_ms = None

    def evaluate(self, *, now_ms: int) -> SoakResult:
        reasons: list[str] = []
        if now_ms - self.started_ms < 30 * DAY_MS:
            reasons.append("fewer than thirty calendar days")
        if self.scalps < 300:
            reasons.append("fewer than 300 executed scalps")
        if self.continuous_started_ms is None:
            reasons.append("continuous operation was not admitted after supervised stability")
        if self.safety_failures:
            reasons.append("unprotected, duplicate, or unreconciled execution was observed")
        complete = not reasons
        report = (
            f"# Testnet Soak — {self.candidate_identity}\n\n"
            f"Decision: {'COMPLETE' if complete else 'INCOMPLETE'}\n\n"
            f"Executed scalps: {self.scalps}\n"
            f"Safety failures: {self.safety_failures}\n\n"
            "Testnet profitability is not production evidence and is excluded from economic "
            "promotion.\n"
        )
        return SoakResult(complete, tuple(reasons), report)


@dataclass(frozen=True)
class RiskEvaluation:
    position_risk: Decimal
    candidate_rules_sha256: str
    net_expectancy: Decimal
    max_daily_loss: Decimal
    max_drawdown: Decimal
    notional_fraction: Decimal
    cost_stress_passed: bool
    operational_controls_passed: bool
    concentration_passed: bool

    @classmethod
    def safe_baseline(cls, rules: str) -> RiskEvaluation:
        return cls(
            Decimal("0.0025"),
            rules,
            Decimal("0.5"),
            Decimal("0.01"),
            Decimal("0.05"),
            Decimal("0.5"),
            True,
            True,
            True,
        )

    @classmethod
    def safe_promoted(
        cls, rules: str, *, max_drawdown: Decimal = Decimal("0.08")
    ) -> RiskEvaluation:
        return cls(
            Decimal("0.005"),
            rules,
            Decimal("0.6"),
            Decimal("0.015"),
            max_drawdown,
            Decimal("0.5"),
            True,
            True,
            True,
        )


@dataclass(frozen=True)
class PromotedRiskResult:
    accepted: bool
    reasons: tuple[str, ...]
    proven_baseline_risk: Decimal
    manifest_key: str


class PromotedRiskGate:
    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def evaluate(
        self,
        candidate_identity: str,
        *,
        baseline: RiskEvaluation,
        promoted: RiskEvaluation,
        baseline_promoted: bool,
        soak_complete: bool,
    ) -> PromotedRiskResult:
        if not baseline_promoted or not soak_complete:
            raise ValueError("0.5% evaluation requires promotion and Testnet Soak at 0.25%")
        if baseline.position_risk != Decimal("0.0025") or promoted.position_risk != Decimal(
            "0.005"
        ):
            raise ValueError("risk evaluations must compare 0.25% with 0.5%")
        if baseline.candidate_rules_sha256 != promoted.candidate_rules_sha256:
            raise ValueError(
                "candidate rules must remain immutable during promoted-risk evaluation"
            )
        reasons: list[str] = []
        if promoted.max_drawdown > Decimal("0.10"):
            reasons.append("drawdown exceeds 10%")
        if promoted.max_daily_loss > Decimal("0.02"):
            reasons.append("daily loss exceeds 2%")
        if (
            promoted.notional_fraction != baseline.notional_fraction
            or promoted.notional_fraction > Decimal("0.5")
        ):
            reasons.append("position notional limit changed or exceeds 50%")
        if baseline.net_expectancy <= 0:
            reasons.append("larger size cannot rescue weak baseline expectancy")
        if not promoted.cost_stress_passed:
            reasons.append("cost stress deteriorated beyond its gate")
        if not promoted.operational_controls_passed:
            reasons.append("operational controls deteriorated beyond their gate")
        if not promoted.concentration_passed:
            reasons.append("profit concentration deteriorated beyond its gate")
        manifest = {
            "candidate_identity": candidate_identity,
            "baseline": _json_evaluation(baseline),
            "promoted": _json_evaluation(promoted),
            "proven_baseline_risk": "0.0025",
            "decision": "accepted" if not reasons else "rejected",
            "reasons": reasons,
        }
        content = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
        identity = hashlib.sha256(content).hexdigest()
        key = f"promoted-risk/{candidate_identity}/{identity}.json"
        self.store.put_immutable(key, content)
        return PromotedRiskResult(not reasons, tuple(reasons), Decimal("0.0025"), key)


def _json_evaluation(evaluation: RiskEvaluation) -> dict[str, object]:
    return {
        key: str(value) if isinstance(value, Decimal) else value
        for key, value in asdict(evaluation).items()
    }
