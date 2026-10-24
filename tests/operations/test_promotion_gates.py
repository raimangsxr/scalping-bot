from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.operations.promotion import (
    LiveReplayComparison,
    LiveShadowGate,
    PromotedRiskGate,
    RiskEvaluation,
    SoakGate,
)

DAY = 86_400_000


def test_live_shadow_requires_fourteen_continuous_days_and_resolved_divergences() -> None:
    gate = LiveShadowGate(candidate_identity="momentum-001-v1", started_ms=0)
    gate.record(
        LiveReplayComparison(
            event_time_ms=DAY,
            live_features={"return": "0.01"},
            replay_features={"return": "0.01"},
            live_signal=True,
            replay_signal=True,
            expiry_match=True,
            latency_ms=20,
            data_quality="valid",
            hypothetical_cost="1.2",
        )
    )
    early = gate.evaluate(now_ms=14 * DAY - 1)
    assert not early.approved

    for day in range(2, 14):
        gate.record(
            LiveReplayComparison(
                event_time_ms=day * DAY,
                live_features={"return": "0.01"},
                replay_features={"return": "0.01"},
                live_signal=True,
                replay_signal=True,
                expiry_match=True,
                latency_ms=20,
                data_quality="valid",
                hypothetical_cost="1.2",
            )
        )

    gate.record(
        LiveReplayComparison(
            event_time_ms=14 * DAY,
            live_features={"return": "0.02"},
            replay_features={"return": "0.01"},
            live_signal=True,
            replay_signal=False,
            expiry_match=True,
            latency_ms=30,
            data_quality="valid",
            hypothetical_cost="1.3",
            divergence="signal mismatch",
        )
    )
    assert not gate.evaluate(now_ms=14 * DAY).approved
    gate.resolve_divergence("signal mismatch", resolution="fixed", regression="reg-live-1")
    assert gate.evaluate(now_ms=14 * DAY).approved


def test_missing_live_dependency_enters_degraded_mode_without_hypothetical_entry() -> None:
    gate = LiveShadowGate(candidate_identity="momentum-001-v1", started_ms=0)

    decision = gate.missing_dependency(event_time_ms=1, dependency="aggTrades")

    assert decision.mode == "DEGRADED"
    assert not decision.hypothetical_entry


def test_soak_requires_supervision_then_verified_incident_free_stability() -> None:
    gate = SoakGate(candidate_identity="momentum-001-v1", started_ms=0)
    with pytest.raises(ValueError, match="seven"):
        gate.begin_continuous(now_ms=6 * DAY, alerts_verified=True)

    gate.begin_continuous(now_ms=7 * DAY, alerts_verified=True)
    for index in range(300):
        gate.record_scalp(
            event_time_ms=7 * DAY + index * (23 * DAY) // 299,
            protected=True,
            duplicate_order=False,
            reconciled=True,
            testnet_pnl=Decimal("100"),
        )

    result = gate.evaluate(now_ms=30 * DAY)
    assert result.complete
    assert "profitability is not production evidence" in result.report


def test_critical_soak_incident_requires_regression_and_restarts_stability() -> None:
    gate = SoakGate(candidate_identity="momentum-001-v1", started_ms=0)

    with pytest.raises(ValueError, match="regression"):
        gate.record_incident(event_time_ms=6 * DAY, critical=True, regression=None)
    gate.record_incident(event_time_ms=6 * DAY, critical=True, regression="reg-incident-1")
    with pytest.raises(ValueError, match="seven"):
        gate.begin_continuous(now_ms=12 * DAY, alerts_verified=True)
    gate.begin_continuous(now_ms=13 * DAY, alerts_verified=True)


def test_promoted_risk_is_isolated_and_never_rescues_weak_expectancy(tmp_path: Path) -> None:
    gate = PromotedRiskGate(LocalObjectStore(tmp_path))
    baseline = RiskEvaluation(
        position_risk=Decimal("0.0025"),
        candidate_rules_sha256="rules-immutable",
        net_expectancy=Decimal("0.8"),
        max_daily_loss=Decimal("0.01"),
        max_drawdown=Decimal("0.05"),
        notional_fraction=Decimal("0.5"),
        cost_stress_passed=True,
        operational_controls_passed=True,
        concentration_passed=True,
    )
    promoted = RiskEvaluation(
        position_risk=Decimal("0.005"),
        candidate_rules_sha256="rules-immutable",
        net_expectancy=Decimal("0.9"),
        max_daily_loss=Decimal("0.019"),
        max_drawdown=Decimal("0.09"),
        notional_fraction=Decimal("0.5"),
        cost_stress_passed=True,
        operational_controls_passed=True,
        concentration_passed=True,
    )

    result = gate.evaluate(
        "momentum-001-v1",
        baseline=baseline,
        promoted=promoted,
        baseline_promoted=True,
        soak_complete=True,
    )

    assert result.accepted
    assert result.proven_baseline_risk == Decimal("0.0025")
    assert (tmp_path / result.manifest_key).exists()


def test_promoted_risk_rejects_changed_rules_or_deteriorated_limits(tmp_path: Path) -> None:
    gate = PromotedRiskGate(LocalObjectStore(tmp_path))
    baseline = RiskEvaluation.safe_baseline("rules-a")
    changed = RiskEvaluation.safe_promoted("rules-b")
    with pytest.raises(ValueError, match="rules"):
        gate.evaluate(
            "candidate-v1",
            baseline=baseline,
            promoted=changed,
            baseline_promoted=True,
            soak_complete=True,
        )

    deteriorated = RiskEvaluation.safe_promoted("rules-a", max_drawdown=Decimal("0.101"))
    result = gate.evaluate(
        "candidate-v1",
        baseline=baseline,
        promoted=deteriorated,
        baseline_promoted=True,
        soak_complete=True,
    )
    assert not result.accepted
    assert "drawdown" in result.reasons[0]
