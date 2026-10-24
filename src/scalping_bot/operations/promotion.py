from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from itertools import pairwise

from scalping_bot.data.storage import ObjectStore

DAY_MS = 86_400_000
LIVE_SHADOW_DAYS = 14
SOAK_DAYS = 30
STABILITY_DAYS = 7
SOAK_SCALPS = 300
INITIAL_POSITION_RISK = Decimal("0.0025")
PROMOTED_POSITION_RISK = Decimal("0.005")
MAX_DAILY_LOSS = Decimal("0.02")
MAX_DRAWDOWN = Decimal("0.10")
MAX_NOTIONAL_FRACTION = Decimal("0.50")
DEFAULT_RISK_CONTROLS = (
    "single-open-scalp",
    "protective-exit-required",
    "daily-circuit-breaker",
    "drawdown-circuit-breaker",
    "no-profit-compounding",
    "testnet-only",
)


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
    replay_hypothetical_cost: str | None = None
    replay_data_quality: str | None = None
    replay_latency_ms: int | None = None
    live_expiry_ms: int | None = None
    replay_expiry_ms: int | None = None
    orders_submitted: int = 0


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
    report_key: str | None = None


class LiveShadowGate:
    """Collect and evaluate no-order live/replay evidence for one candidate."""

    def __init__(
        self,
        *,
        candidate_identity: str,
        started_ms: int,
        candidate_manifest_sha256: str | None = None,
    ) -> None:
        self.candidate_identity = candidate_identity
        self.candidate_manifest_sha256 = candidate_manifest_sha256
        self.started_ms = started_ms
        self.comparisons: list[LiveReplayComparison] = []
        self.resolutions: dict[str, tuple[str, str]] = {}
        self.continuity_broken = False
        self._continuity_reasons: list[str] = []
        self._order_submissions: list[tuple[int, str]] = []
        self._hypothetical_decisions: list[tuple[int, str]] = []

    def record(self, comparison: LiveReplayComparison) -> None:
        if comparison.event_time_ms < self.started_ms:
            raise ValueError("Live Shadow observations cannot precede the start")
        if self.comparisons and comparison.event_time_ms <= self.comparisons[-1].event_time_ms:
            raise ValueError("Live Shadow observations must be chronological")
        if comparison.orders_submitted < 0:
            raise ValueError("Live Shadow order count cannot be negative")
        self.comparisons.append(comparison)

    def record_order_submission(self, *, event_time_ms: int, client_order_id: str) -> None:
        """Record an attempted exchange order so the shadow gate can fail closed."""
        if event_time_ms < self.started_ms:
            raise ValueError("Live Shadow order attempts cannot precede the start")
        if not client_order_id:
            raise ValueError("Live Shadow order attempts require a client order id")
        self._order_submissions.append((event_time_ms, client_order_id))

    def record_hypothetical_decision(self, *, event_time_ms: int, kind: str) -> None:
        if event_time_ms < self.started_ms:
            raise ValueError("Shadow decisions cannot precede the start")
        if not kind:
            raise ValueError("Shadow decisions require a kind")
        self._hypothetical_decisions.append((event_time_ms, kind))

    def mark_continuity_broken(self, reason: str) -> None:
        if not reason:
            raise ValueError("a continuity break requires a reason")
        self.continuity_broken = True
        self._continuity_reasons.append(reason)

    def missing_dependency(self, *, event_time_ms: int, dependency: str) -> DegradedShadowDecision:
        if not dependency:
            raise ValueError("a missing dependency must be named")
        self.record(
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
        if not divergence:
            raise ValueError("a divergence must be named")
        if not resolution or not regression:
            raise ValueError("a divergence resolution requires regression coverage")
        self.resolutions[divergence] = (resolution, regression)

    def evaluate(self, *, now_ms: int) -> LiveShadowResult:
        reasons: list[str] = []
        unresolved = self._unresolved_divergences()

        if now_ms < self.started_ms:
            reasons.append("Live Shadow evaluation precedes its start")
        elif now_ms - self.started_ms < LIVE_SHADOW_DAYS * DAY_MS:
            reasons.append("fewer than fourteen continuous calendar days")
        if self.continuity_broken:
            reasons.append("Live Shadow continuity was broken")
            reasons.extend(f"continuity break: {reason}" for reason in self._continuity_reasons)
        if unresolved:
            reasons.append(f"unresolved divergences: {', '.join(sorted(unresolved))}")
        if not self.comparisons:
            reasons.append("no live/replay comparisons were recorded")
        else:
            observation_times = [item.event_time_ms for item in self.comparisons]
            if any(time < self.started_ms or time > now_ms for time in observation_times):
                reasons.append("Live Shadow contains an observation outside its evaluation window")
            boundaries = [self.started_ms, *observation_times, now_ms]
            if any(current - previous > DAY_MS for previous, current in pairwise(boundaries)):
                reasons.append("Live Shadow has an unobserved calendar-day gap")
        order_count = len(self._order_submissions) + sum(
            item.orders_submitted for item in self.comparisons
        )
        if order_count:
            reasons.append("Live Shadow submitted orders; shadow operation is no-order")

        approved = not reasons
        report = self._render_report(
            approved=approved,
            reasons=tuple(reasons),
            unresolved=unresolved,
            order_count=order_count,
            now_ms=now_ms,
        )
        return LiveShadowResult(approved, tuple(reasons), report)

    def persist_report(self, result: LiveShadowResult, store: ObjectStore) -> str:
        key = _report_key("live-shadow", self.candidate_identity, result.report)
        store.put_immutable(key, result.report.encode())
        return key

    def _unresolved_divergences(self) -> set[str]:
        divergences: set[str] = set()
        for comparison in self.comparisons:
            divergences.update(_comparison_divergences(comparison))
        return {divergence for divergence in divergences if divergence not in self.resolutions}

    def _render_report(
        self,
        *,
        approved: bool,
        reasons: tuple[str, ...],
        unresolved: set[str],
        order_count: int,
        now_ms: int,
    ) -> str:
        decision = "APPROVE Testnet Soak" if approved else "REJECT progression"
        manifest = self.candidate_manifest_sha256 or "not supplied"
        reason_lines = "\n".join(f"- {reason}" for reason in reasons) or "- none"
        resolution_lines = (
            "\n".join(
                f"- {divergence}: {resolution} (regression: {regression})"
                for divergence, (resolution, regression) in sorted(self.resolutions.items())
            )
            or "- none"
        )
        return (
            f"# Live Shadow — {self.candidate_identity}\n\n"
            f"Candidate manifest SHA-256: {manifest}\n"
            f"Decision: {decision}\n\n"
            f"Window: {self.started_ms}-{now_ms} ms\n"
            f"Comparisons: {len(self.comparisons)}\n"
            f"Hypothetical decisions: {len(self._hypothetical_decisions)}\n"
            f"Unresolved divergences: {len(unresolved)}\n"
            f"Orders submitted: {order_count}\n"
            "Compared: feature values, signals, expiry, latency, data quality, and "
            "hypothetical costs.\n\n"
            "Divergence resolutions:\n"
            f"{resolution_lines}\n\n"
            "Reasons:\n"
            f"{reason_lines}\n"
        )


@dataclass(frozen=True)
class SoakResult:
    complete: bool
    reasons: tuple[str, ...]
    report: str
    report_key: str | None = None


class SoakGate:
    """Enforce the supervised-to-continuous Testnet Soak contract at 0.25% risk."""

    def __init__(
        self,
        *,
        candidate_identity: str,
        started_ms: int,
        position_risk: Decimal = INITIAL_POSITION_RISK,
        notional_fraction: Decimal = MAX_NOTIONAL_FRACTION,
    ) -> None:
        if position_risk != INITIAL_POSITION_RISK:
            raise ValueError("Testnet Soak requires exactly 0.25% position risk")
        if notional_fraction > MAX_NOTIONAL_FRACTION:
            raise ValueError("Testnet Soak notional cannot exceed 50%")
        self.candidate_identity = candidate_identity
        self.started_ms = started_ms
        self.position_risk = position_risk
        self.notional_fraction = notional_fraction
        self.continuous_started_ms: int | None = None
        self.last_critical_incident_ms: int | None = None
        self.scalps = 0
        self.scalp_times: list[int] = []
        self.safety_failures = 0
        self.risk_violations = 0
        self.unresolved_divergences: list[str] = []
        self.critical_incidents: list[tuple[int, str]] = []
        self.supervised_windows: list[tuple[int, int, bool]] = []
        self.alerts_verified = False
        self.testnet_pnl = Decimal(0)

    def record_supervised_window(
        self, *, start_ms: int, end_ms: int, alerts_verified: bool
    ) -> None:
        if start_ms < self.started_ms or end_ms <= start_ms:
            raise ValueError("supervised windows must be valid and inside the soak")
        if self.supervised_windows and start_ms < self.supervised_windows[-1][1]:
            raise ValueError("supervised windows must be chronological and non-overlapping")
        self.supervised_windows.append((start_ms, end_ms, alerts_verified))

    def begin_continuous(self, *, now_ms: int, alerts_verified: bool) -> None:
        stable_since = self.last_critical_incident_ms or self.started_ms
        if now_ms - stable_since < STABILITY_DAYS * DAY_MS:
            raise ValueError("seven incident-free days are required before continuous operation")
        if not alerts_verified:
            raise ValueError("alerts must be verified before continuous operation")
        if not self.supervised_windows:
            raise ValueError("supervised windows are required before continuous operation")
        if not self._supervision_covers(stable_since, now_ms):
            raise ValueError("supervised windows must cover the stability period")
        self.alerts_verified = True
        self.continuous_started_ms = now_ms

    def record_scalp(
        self,
        *,
        event_time_ms: int,
        protected: bool,
        duplicate_order: bool,
        reconciled: bool,
        testnet_pnl: Decimal,
        position_risk: Decimal | None = None,
        notional_fraction: Decimal | None = None,
        unresolved_divergences: tuple[str, ...] = (),
        executed: bool = True,
    ) -> None:
        if self.continuous_started_ms is None or event_time_ms < self.continuous_started_ms:
            raise ValueError("scalps count only after continuous operation is admitted")
        if self.scalp_times and event_time_ms <= self.scalp_times[-1]:
            raise ValueError("soak scalps must be chronological")
        if not executed:
            return
        self.scalp_times.append(event_time_ms)
        self.scalps += 1
        self.testnet_pnl += testnet_pnl

        effective_risk = self.position_risk if position_risk is None else position_risk
        if effective_risk != self.position_risk:
            self.risk_violations += 1
        if notional_fraction is not None and notional_fraction > self.notional_fraction:
            self.risk_violations += 1
        if not protected or duplicate_order or not reconciled:
            self.safety_failures += 1
        if not reconciled and not unresolved_divergences:
            unresolved_divergences = ("exchange/local reconciliation was not consistent",)
        self.unresolved_divergences.extend(unresolved_divergences)

    def record_incident(
        self, *, event_time_ms: int, critical: bool, regression: str | None
    ) -> None:
        if event_time_ms < self.started_ms:
            raise ValueError("incidents cannot precede the soak")
        if self.critical_incidents and event_time_ms <= self.critical_incidents[-1][0]:
            raise ValueError("soak incidents must be chronological")
        if critical and not regression:
            raise ValueError("a critical incident requires a replayable regression")
        if critical:
            self.last_critical_incident_ms = event_time_ms
            self.continuous_started_ms = None
            self.critical_incidents.append((event_time_ms, regression or ""))

    def evaluate(self, *, now_ms: int) -> SoakResult:
        reasons: list[str] = []
        if now_ms - self.started_ms < SOAK_DAYS * DAY_MS:
            reasons.append("fewer than thirty calendar days")
        if self.scalps < SOAK_SCALPS:
            reasons.append("fewer than 300 executed scalps at 0.25% position risk")
        if self.continuous_started_ms is None:
            reasons.append("continuous operation was not admitted after supervised stability")
        if self.safety_failures:
            reasons.append("unprotected, duplicate, or unreconciled execution was observed")
        if self.risk_violations:
            reasons.append("an executed scalp used risk or notional above the soak contract")
        if self.unresolved_divergences:
            reasons.append("unresolved exchange/local divergences were observed")
        if self.scalp_times and (
            self.scalp_times[0] - self.started_ms > 8 * DAY_MS
            or self.scalp_times[-1] < self.started_ms + SOAK_DAYS * DAY_MS
        ):
            reasons.append("executed scalp evidence does not span the thirty-day soak")

        complete = not reasons
        report = self._render_report(complete=complete, reasons=tuple(reasons))
        return SoakResult(complete, tuple(reasons), report)

    def persist_report(self, result: SoakResult, store: ObjectStore) -> str:
        key = _report_key("testnet-soak", self.candidate_identity, result.report)
        store.put_immutable(key, result.report.encode())
        return key

    def _supervision_covers(self, start_ms: int, end_ms: int) -> bool:
        cursor = start_ms
        for window_start, window_end, alerts_verified in self.supervised_windows:
            if window_start > cursor:
                return False
            if window_end > end_ms:
                return False
            if not alerts_verified:
                return False
            cursor = max(cursor, window_end)
            if cursor >= end_ms:
                return True
        return cursor >= end_ms

    def _render_report(self, *, complete: bool, reasons: tuple[str, ...]) -> str:
        decision = "COMPLETE" if complete else "INCOMPLETE"
        reason_lines = "\n".join(f"- {reason}" for reason in reasons) or "- none"
        return (
            f"# Testnet Soak — {self.candidate_identity}\n\n"
            f"Decision: {decision}\n"
            f"Position risk: {self.position_risk:.2%}\n"
            f"Notional cap: {self.notional_fraction:.2%}\n"
            f"Window: {self.started_ms}-{self.started_ms + SOAK_DAYS * DAY_MS} ms\n"
            f"Executed scalps: {self.scalps}/{SOAK_SCALPS}\n"
            f"Supervised windows: {len(self.supervised_windows)}\n"
            f"Critical incidents: {len(self.critical_incidents)}\n"
            f"Safety failures: {self.safety_failures}\n"
            f"Risk violations: {self.risk_violations}\n"
            f"Unresolved divergences: {len(self.unresolved_divergences)}\n\n"
            "Operational fidelity: protection, duplicate-order prevention, reconciliation, "
            "risk sizing, and incident stability are evaluated.\n"
            "Testnet profitability is not production evidence and is excluded from economic "
            "promotion.\n\n"
            "Reasons:\n"
            f"{reason_lines}\n"
        )


@dataclass(frozen=True)
class StressEvaluation:
    net_expectancy: Decimal
    max_daily_loss: Decimal
    max_drawdown: Decimal
    cost_stress_passed: bool
    operational_controls_passed: bool
    concentration_passed: bool
    artifact_key: str = ""
    artifact_sha256: str = ""


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
    daily_loss_limit: Decimal = MAX_DAILY_LOSS
    drawdown_limit: Decimal = MAX_DRAWDOWN
    risk_controls: tuple[str, ...] = DEFAULT_RISK_CONTROLS
    historical_stress: StressEvaluation | None = None
    testnet_stress: StressEvaluation | None = None

    @classmethod
    def safe_baseline(cls, rules: str) -> RiskEvaluation:
        """Build a configuration-shaped baseline; it is not promotion evidence."""
        return cls(
            position_risk=INITIAL_POSITION_RISK,
            candidate_rules_sha256=rules,
            net_expectancy=Decimal("0.5"),
            max_daily_loss=Decimal("0.01"),
            max_drawdown=Decimal("0.05"),
            notional_fraction=MAX_NOTIONAL_FRACTION,
            cost_stress_passed=True,
            operational_controls_passed=True,
            concentration_passed=True,
        )

    @classmethod
    def safe_promoted(
        cls, rules: str, *, max_drawdown: Decimal = Decimal("0.08")
    ) -> RiskEvaluation:
        """Build a configuration-shaped trial; it is not promotion evidence."""
        return cls(
            position_risk=PROMOTED_POSITION_RISK,
            candidate_rules_sha256=rules,
            net_expectancy=Decimal("0.6"),
            max_daily_loss=Decimal("0.015"),
            max_drawdown=max_drawdown,
            notional_fraction=MAX_NOTIONAL_FRACTION,
            cost_stress_passed=True,
            operational_controls_passed=True,
            concentration_passed=True,
        )


@dataclass(frozen=True)
class PromotedRiskResult:
    accepted: bool
    reasons: tuple[str, ...]
    proven_baseline_risk: Decimal
    manifest_key: str
    baseline_preserved: bool = True
    report: str = ""


class PromotedRiskGate:
    """Compare 0.25% and 0.5% evidence without changing the proven baseline."""

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
        promotion_artifact_key: str | None = None,
        soak_report_key: str | None = None,
    ) -> PromotedRiskResult:
        if not baseline_promoted or not soak_complete:
            raise ValueError("0.5% evaluation requires promotion and Testnet Soak at 0.25%")
        if baseline.position_risk != INITIAL_POSITION_RISK or promoted.position_risk != (
            PROMOTED_POSITION_RISK
        ):
            raise ValueError("risk evaluations must compare 0.25% with 0.5%")
        if baseline.candidate_rules_sha256 != promoted.candidate_rules_sha256:
            raise ValueError(
                "candidate rules must remain immutable during promoted-risk evaluation"
            )

        reasons: list[str] = []
        self._check_reference(promotion_artifact_key, "promotion evidence artifact", reasons)
        self._check_reference(soak_report_key, "Testnet Soak report artifact", reasons)
        self._check_limits(baseline, "baseline", reasons)
        self._check_limits(promoted, "promoted", reasons)
        if baseline.notional_fraction != promoted.notional_fraction:
            reasons.append("position notional limit changed between risk evaluations")
        if (
            baseline.notional_fraction > MAX_NOTIONAL_FRACTION
            or promoted.notional_fraction > MAX_NOTIONAL_FRACTION
        ):
            reasons.append("position notional limit exceeds 50%")
        if baseline.daily_loss_limit != promoted.daily_loss_limit:
            reasons.append("daily loss control changed between risk evaluations")
        if baseline.drawdown_limit != promoted.drawdown_limit:
            reasons.append("drawdown control changed between risk evaluations")
        if baseline.daily_loss_limit != MAX_DAILY_LOSS:
            reasons.append("daily loss control is not capped at 2%")
        if baseline.drawdown_limit != MAX_DRAWDOWN:
            reasons.append("drawdown control is not capped at 10%")
        if baseline.risk_controls != promoted.risk_controls:
            reasons.append("risk control policy changed between risk evaluations")
        if baseline.risk_controls != DEFAULT_RISK_CONTROLS:
            reasons.append("baseline risk control policy is not the approved policy")

        if baseline.net_expectancy <= 0:
            reasons.append("larger size cannot rescue weak baseline expectancy")
        self._check_criteria(baseline, "baseline", reasons)
        self._check_criteria(promoted, "promoted", reasons)
        self._check_stress(
            "historical", baseline.historical_stress, promoted.historical_stress, reasons
        )
        self._check_stress("Testnet", baseline.testnet_stress, promoted.testnet_stress, reasons)

        manifest = {
            "manifest_version": 1,
            "candidate_identity": candidate_identity,
            "candidate_rules_sha256": baseline.candidate_rules_sha256,
            "promotion_artifact_key": promotion_artifact_key,
            "soak_report_key": soak_report_key,
            "baseline": _json_evaluation(baseline),
            "promoted": _json_evaluation(promoted),
            "proven_baseline_risk": str(INITIAL_POSITION_RISK),
            "decision": "accepted" if not reasons else "rejected",
            "reasons": reasons,
        }
        content = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
        identity = hashlib.sha256(content).hexdigest()
        key = f"promoted-risk/{_artifact_component(candidate_identity)}/{identity}.json"
        self.store.put_immutable(key, content)
        report = self._render_report(candidate_identity, reasons, baseline, promoted)
        return PromotedRiskResult(
            not reasons,
            tuple(reasons),
            INITIAL_POSITION_RISK,
            key,
            baseline_preserved=True,
            report=report,
        )

    @staticmethod
    def _check_limits(evaluation: RiskEvaluation, label: str, reasons: list[str]) -> None:
        PromotedRiskGate._check_metric_limits(
            label, evaluation.max_daily_loss, evaluation.max_drawdown, reasons
        )

    @staticmethod
    def _check_metric_limits(
        label: str, max_daily_loss: Decimal, max_drawdown: Decimal, reasons: list[str]
    ) -> None:
        if max_daily_loss > MAX_DAILY_LOSS:
            reasons.append(f"{label} daily loss exceeds 2%")
        if max_drawdown > MAX_DRAWDOWN:
            reasons.append(f"{label} drawdown exceeds 10%")

    @staticmethod
    def _check_criteria(evaluation: RiskEvaluation, label: str, reasons: list[str]) -> None:
        PromotedRiskGate._check_criteria_values(
            label,
            evaluation.cost_stress_passed,
            evaluation.operational_controls_passed,
            evaluation.concentration_passed,
            reasons,
        )

    @staticmethod
    def _check_criteria_values(
        label: str,
        cost_stress_passed: bool,
        operational_controls_passed: bool,
        concentration_passed: bool,
        reasons: list[str],
    ) -> None:
        if not cost_stress_passed:
            reasons.append(f"{label} cost stress failed")
        if not operational_controls_passed:
            reasons.append(f"{label} operational controls failed")
        if not concentration_passed:
            reasons.append(f"{label} profit concentration failed")

    def _check_stress(
        self,
        label: str,
        baseline: StressEvaluation | None,
        promoted: StressEvaluation | None,
        reasons: list[str],
    ) -> None:
        if baseline is None or promoted is None:
            reasons.append(f"{label} stress results are required at both risk levels")
            return
        if baseline.net_expectancy <= 0:
            reasons.append(f"{label} baseline expectancy is not positive")
        if promoted.net_expectancy <= 0:
            reasons.append(f"{label} 0.5% expectancy is not positive")
        for risk_label, result in (("baseline", baseline), ("promoted", promoted)):
            self._check_stress_artifact(label, risk_label, result, reasons)
            self._check_metric_limits(
                f"{label} {risk_label}", result.max_daily_loss, result.max_drawdown, reasons
            )
            self._check_criteria_values(
                f"{label} {risk_label}",
                result.cost_stress_passed,
                result.operational_controls_passed,
                result.concentration_passed,
                reasons,
            )

    def _check_stress_artifact(
        self,
        label: str,
        risk_label: str,
        result: StressEvaluation,
        reasons: list[str],
    ) -> None:
        if not result.artifact_key or not result.artifact_sha256:
            reasons.append(f"{label} {risk_label} stress artifact is required")
            return
        expected = result.artifact_sha256.lower()
        if len(expected) != 64 or any(
            character not in "0123456789abcdef" for character in expected
        ):
            reasons.append(f"{label} {risk_label} stress artifact hash is malformed")
            return
        try:
            content = self.store.read(result.artifact_key)
        except (KeyError, OSError):
            reasons.append(f"{label} {risk_label} stress artifact is unavailable")
            return
        if hashlib.sha256(content).hexdigest() != expected:
            reasons.append(f"{label} {risk_label} stress artifact hash does not match")

    def _check_reference(self, key: str | None, label: str, reasons: list[str]) -> None:
        if not key:
            reasons.append(f"{label} is required")
            return
        try:
            self.store.read(key)
        except (KeyError, OSError):
            reasons.append(f"{label} is unavailable")

    @staticmethod
    def _render_report(
        candidate_identity: str,
        reasons: list[str],
        baseline: RiskEvaluation,
        promoted: RiskEvaluation,
    ) -> str:
        decision = "ACCEPT 0.5% evaluation" if not reasons else "REJECT 0.5% evaluation"
        reason_lines = "\n".join(f"- {reason}" for reason in reasons) or "- none"
        return (
            f"# Promoted Position Risk — {candidate_identity}\n\n"
            f"Decision: {decision}\n"
            "Proven baseline: 0.25% remains the baseline regardless of this decision.\n"
            f"Candidate rules: {baseline.candidate_rules_sha256}\n"
            f"Baseline expectancy: {baseline.net_expectancy}\n"
            f"Promoted expectancy: {promoted.net_expectancy}\n"
            "Compared evidence: historical stress, Testnet stress, cost stress, operational "
            "controls, concentration, daily loss, drawdown, and notional cap.\n\n"
            "Reasons:\n"
            f"{reason_lines}\n"
        )


def _comparison_divergences(comparison: LiveReplayComparison) -> tuple[str, ...]:
    if comparison.divergence is not None:
        return (comparison.divergence,)
    divergences: list[str] = []
    if comparison.live_features != comparison.replay_features:
        divergences.append("feature values differ")
    if comparison.live_signal != comparison.replay_signal:
        divergences.append("signal differs")
    if not comparison.expiry_match or (
        comparison.live_expiry_ms is not None
        and comparison.replay_expiry_ms is not None
        and comparison.live_expiry_ms != comparison.replay_expiry_ms
    ):
        divergences.append("expiry differs")
    if (
        comparison.replay_hypothetical_cost is not None
        and comparison.hypothetical_cost != comparison.replay_hypothetical_cost
    ):
        divergences.append("hypothetical cost differs")
    if (
        comparison.replay_data_quality is not None
        and comparison.data_quality != comparison.replay_data_quality
    ):
        divergences.append("data quality differs")
    if (
        comparison.replay_latency_ms is not None
        and comparison.latency_ms != comparison.replay_latency_ms
    ):
        divergences.append("latency differs")
    if comparison.data_quality != "valid":
        divergences.append("live data quality is not valid")
    if comparison.latency_ms < 0:
        divergences.append("latency is negative")
    return tuple(divergences)


def _json_evaluation(evaluation: RiskEvaluation) -> dict[str, object]:
    serialized = _json_value(asdict(evaluation))
    if not isinstance(serialized, dict):
        raise TypeError("risk evaluation serialization must be an object")
    return serialized


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def _artifact_component(identity: str) -> str:
    return identity.replace("/", "_").replace("..", "_")


def _report_key(kind: str, candidate_identity: str, report: str) -> str:
    digest = hashlib.sha256(report.encode()).hexdigest()
    return f"{kind}/{_artifact_component(candidate_identity)}/{digest}.md"
