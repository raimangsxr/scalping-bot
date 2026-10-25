from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.domain.events import CandleClosed, MarketTick
from scalping_bot.operations.promotion import (
    LiveReplayComparison,
    LiveShadowGate,
    PromotedRiskGate,
    RiskEvaluation,
    SoakGate,
    StressEvaluation,
)
from scalping_bot.operations.shadow import LiveShadowRunner
from scalping_bot.strategies.momentum import MomentumPolicy

DAY = 86_400_000


def _stress_evidence(
    root: Path,
    key: str,
    *,
    candidate_identity: str,
    candidate_rules_sha256: str,
    sample: str,
    position_risk: str,
    net_expectancy: str,
    max_daily_loss: str,
    max_drawdown: str,
) -> StressEvaluation:
    source_key = f"{key}.source"
    source_content = b"immutable stress source\n"
    LocalObjectStore(root).put_immutable(source_key, source_content)
    content = json.dumps(
        {
            "candidate_identity": candidate_identity,
            "candidate_rules_sha256": candidate_rules_sha256,
            "sample": sample,
            "position_risk": position_risk,
            "net_expectancy": net_expectancy,
            "max_daily_loss": max_daily_loss,
            "max_drawdown": max_drawdown,
            "cost_stress_passed": True,
            "operational_controls_passed": True,
            "concentration_passed": True,
            "source_evidence": [
                f"{source_key}#{hashlib.sha256(source_content).hexdigest()}"
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    LocalObjectStore(root).put_immutable(key, content)
    return StressEvaluation(
        net_expectancy=Decimal(net_expectancy),
        max_daily_loss=Decimal(max_daily_loss),
        max_drawdown=Decimal(max_drawdown),
        cost_stress_passed=True,
        operational_controls_passed=True,
        concentration_passed=True,
        artifact_key=key,
        artifact_sha256=hashlib.sha256(content).hexdigest(),
    )


def _baseline_evaluation(rules: str) -> RiskEvaluation:
    return RiskEvaluation(
        position_risk=Decimal("0.0025"),
        candidate_rules_sha256=rules,
        net_expectancy=Decimal("0.5"),
        max_daily_loss=Decimal("0.01"),
        max_drawdown=Decimal("0.05"),
        notional_fraction=Decimal("0.5"),
        cost_stress_passed=True,
        operational_controls_passed=True,
        concentration_passed=True,
    )


def _promoted_evaluation(rules: str, *, max_drawdown: str = "0.08") -> RiskEvaluation:
    return RiskEvaluation(
        position_risk=Decimal("0.005"),
        candidate_rules_sha256=rules,
        net_expectancy=Decimal("0.6"),
        max_daily_loss=Decimal("0.015"),
        max_drawdown=Decimal(max_drawdown),
        notional_fraction=Decimal("0.5"),
        cost_stress_passed=True,
        operational_controls_passed=True,
        concentration_passed=True,
    )


def _with_experiment_manifest(
    store: LocalObjectStore,
    evaluation: RiskEvaluation,
    *,
    candidate_identity: str,
) -> RiskEvaluation:
    payload = {
        "schema_version": 1,
        "code_commit": "test-commit",
        "candidate_identity": candidate_identity,
        "candidate_rules_sha256": evaluation.candidate_rules_sha256,
        "configuration": {"candidate_parameters": {"window": 2}},
        "commission_snapshot": {
            "symbol": "BTCUSDT",
            "maker": "0.0002",
            "taker": "0.0004",
            "captured_ms": 1,
        },
        "execution_cost_model": {
            "identity": evaluation.execution_cost_model_sha256,
            "half_spread_rate": "0",
            "slippage_rate": "0",
        },
        "period": ["2020-01", "2025-09"],
        "random_seed": 7,
        "position_risk": str(evaluation.position_risk),
        "notional_fraction": str(evaluation.notional_fraction),
        "daily_loss_limit": str(evaluation.daily_loss_limit),
        "drawdown_limit": str(evaluation.drawdown_limit),
        "risk_controls": list(evaluation.risk_controls),
        "dataset_checksums": list(evaluation.dataset_checksums),
        "execution_cost_model_sha256": evaluation.execution_cost_model_sha256,
        "historical_stress_artifact_key": (
            evaluation.historical_stress.artifact_key if evaluation.historical_stress else None
        ),
        "testnet_stress_artifact_key": (
            evaluation.testnet_stress.artifact_key if evaluation.testnet_stress else None
        ),
    }
    content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    identity = hashlib.sha256(content).hexdigest()
    key = f"promoted-risk/experiments/{candidate_identity}/{identity}.json"
    store.put_immutable(key, content)
    return replace(evaluation, experiment_manifest_key=key)


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
    gate.resolve_divergence("feature values differ", resolution="fixed", regression="reg-live-2")
    gate.resolve_divergence("signal differs", resolution="fixed", regression="reg-live-3")
    result = gate.evaluate(now_ms=14 * DAY)
    assert result.approved
    assert "regression: reg-live-1" in result.report


def test_missing_live_dependency_enters_degraded_mode_without_hypothetical_entry() -> None:
    gate = LiveShadowGate(candidate_identity="momentum-001-v1", started_ms=0)

    decision = gate.missing_dependency(event_time_ms=1, dependency="aggTrades")

    assert decision.mode == "DEGRADED"
    assert not decision.hypothetical_entry


def test_live_shadow_detects_unannotated_drift_and_any_order_submission() -> None:
    gate = LiveShadowGate(
        candidate_identity="momentum-001-v1",
        started_ms=0,
        candidate_manifest_sha256="candidate-manifest",
    )
    for day in range(1, 15):
        gate.record(
            LiveReplayComparison(
                event_time_ms=day * DAY,
                live_features={"return": "0.02" if day == 14 else "0.01"},
                replay_features={"return": "0.01"},
                live_signal=day != 14,
                replay_signal=True,
                expiry_match=day != 14,
                latency_ms=20,
                data_quality="valid",
                hypothetical_cost="1.3" if day == 14 else "1.2",
                replay_hypothetical_cost="1.2",
                replay_latency_ms=10 if day == 14 else 20,
            )
        )
    gate.record_order_submission(event_time_ms=14 * DAY, client_order_id="shadow-order-1")

    result = gate.evaluate(now_ms=14 * DAY)

    assert not result.approved
    assert "feature values" in " ".join(result.reasons)
    assert "signal" in " ".join(result.reasons)
    assert "expiry" in " ".join(result.reasons)
    assert "hypothetical cost" in " ".join(result.reasons)
    assert "latency" in " ".join(result.reasons)
    assert "submitted orders" in " ".join(result.reasons)
    assert "candidate-manifest" in result.report


def test_live_shadow_report_can_be_saved_as_an_immutable_artifact(tmp_path: Path) -> None:
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

    result = gate.evaluate(now_ms=14 * DAY)
    key = gate.persist_report(result, LocalObjectStore(tmp_path))

    assert (tmp_path / key).read_text(encoding="utf-8") == result.report


def test_live_shadow_runner_emits_hypothetical_commands_without_an_exchange() -> None:
    gate = LiveShadowGate(candidate_identity="momentum-001-v1", started_ms=0)
    runner = LiveShadowRunner(
        policy=MomentumPolicy(
            candidate_version="momentum-001-v1",
            window=2,
            threshold=Decimal("0.01"),
        ),
        initial_equity=Decimal("10000"),
        gate=gate,
    )

    runner.process(
        CandleClosed(
            event_id="candle-1",
            event_time_ms=1_000,
            sequence=1,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            close=Decimal("100"),
            complete=True,
        )
    )
    runner.process(
        CandleClosed(
            event_id="candle-2",
            event_time_ms=61_000,
            sequence=2,
            open=Decimal("100"),
            high=Decimal("102"),
            low=Decimal("100"),
            close=Decimal("102"),
            complete=True,
        )
    )
    result = runner.process(
        MarketTick(event_id="tick-1", event_time_ms=61_001, price=Decimal("103"))
    )

    assert result.hypothetical_commands == ("hypothetical:SubmitEntry",)
    shadow_report = gate.evaluate(now_ms=61_001).report
    assert "Orders submitted: 0" in shadow_report
    assert "Hypothetical decisions: 3" in shadow_report


def test_soak_requires_supervision_then_verified_incident_free_stability() -> None:
    gate = SoakGate(candidate_identity="momentum-001-v1", started_ms=0)
    gate.record_supervised_window(start_ms=0, end_ms=7 * DAY, alerts_verified=True)
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


def test_soak_rejects_wrong_risk_and_unresolved_operational_divergences() -> None:
    gate = SoakGate(candidate_identity="momentum-001-v1", started_ms=0)
    gate.record_supervised_window(start_ms=0, end_ms=7 * DAY, alerts_verified=True)
    gate.begin_continuous(now_ms=7 * DAY, alerts_verified=True)
    gate.record_scalp(
        event_time_ms=7 * DAY,
        protected=True,
        duplicate_order=False,
        reconciled=False,
        position_risk=Decimal("0.005"),
        unresolved_divergences=("exchange/local mismatch",),
        testnet_pnl=Decimal("100"),
    )

    result = gate.evaluate(now_ms=30 * DAY)

    assert not result.complete
    assert "0.25%" in " ".join(result.reasons)
    assert "divergence" in " ".join(result.reasons)
    assert "Operational fidelity" in result.report


def test_soak_requires_explicit_supervised_coverage_when_windows_are_recorded() -> None:
    gate = SoakGate(candidate_identity="momentum-001-v1", started_ms=0)
    gate.record_supervised_window(start_ms=0, end_ms=3 * DAY, alerts_verified=True)

    with pytest.raises(ValueError, match="supervised"):
        gate.begin_continuous(now_ms=7 * DAY, alerts_verified=True)


def test_soak_records_supervised_scalps_and_rejects_future_execution_evidence() -> None:
    gate = SoakGate(candidate_identity="momentum-001-v1", started_ms=0)
    gate.record_supervised_window(start_ms=0, end_ms=7 * DAY, alerts_verified=True)
    gate.record_scalp(
        event_time_ms=6 * DAY,
        protected=True,
        duplicate_order=False,
        reconciled=True,
        testnet_pnl=Decimal("1"),
    )
    gate.begin_continuous(now_ms=7 * DAY, alerts_verified=True)
    gate.record_scalp(
        event_time_ms=31 * DAY,
        protected=True,
        duplicate_order=False,
        reconciled=True,
        testnet_pnl=Decimal("1"),
    )

    result = gate.evaluate(now_ms=30 * DAY)

    assert not result.complete
    assert "future timestamps" in " ".join(result.reasons)


def test_critical_soak_incident_requires_regression_and_restarts_stability() -> None:
    gate = SoakGate(candidate_identity="momentum-001-v1", started_ms=0)
    gate.record_supervised_window(start_ms=6 * DAY, end_ms=12 * DAY, alerts_verified=True)
    gate.record_supervised_window(start_ms=12 * DAY, end_ms=13 * DAY, alerts_verified=True)

    with pytest.raises(ValueError, match="regression"):
        gate.record_incident(event_time_ms=6 * DAY, critical=True, regression=None)
    gate.record_incident(event_time_ms=6 * DAY, critical=True, regression="reg-incident-1")
    with pytest.raises(ValueError, match="seven"):
        gate.begin_continuous(now_ms=12 * DAY, alerts_verified=True)
    gate.begin_continuous(now_ms=13 * DAY, alerts_verified=True)


def test_promoted_risk_is_isolated_and_never_rescues_weak_expectancy(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    promotion_content = json.dumps(
        {
            "candidate_identity": "momentum-001-v1",
            "candidate_rules_sha256": "rules-immutable",
            "decision": "PROMOTED",
        },
        sort_keys=True,
    ).encode()
    promotion_key = f"promotion/{hashlib.sha256(promotion_content).hexdigest()}.json"
    store.put_immutable(
        promotion_key,
        promotion_content,
    )
    soak_content = (
        "# Testnet Soak — momentum-001-v1\nDecision: COMPLETE\nExecuted scalps: 300/300\n"
    ).encode()
    soak_key = f"testnet-soak/{hashlib.sha256(soak_content).hexdigest()}.md"
    store.put_immutable(
        soak_key,
        soak_content,
    )
    gate = PromotedRiskGate(store)
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
        historical_stress=_stress_evidence(
            tmp_path,
            "stress/historical-baseline.json",
            candidate_identity="momentum-001-v1",
            candidate_rules_sha256="rules-immutable",
            sample="historical",
            position_risk="0.0025",
            net_expectancy="0.2",
            max_daily_loss="0.01",
            max_drawdown="0.05",
        ),
        testnet_stress=_stress_evidence(
            tmp_path,
            "stress/testnet-baseline.json",
            candidate_identity="momentum-001-v1",
            candidate_rules_sha256="rules-immutable",
            sample="testnet",
            position_risk="0.0025",
            net_expectancy="0.3",
            max_daily_loss="0.01",
            max_drawdown="0.05",
        ),
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
        historical_stress=_stress_evidence(
            tmp_path,
            "stress/historical-promoted.json",
            candidate_identity="momentum-001-v1",
            candidate_rules_sha256="rules-immutable",
            sample="historical",
            position_risk="0.005",
            net_expectancy="0.15",
            max_daily_loss="0.019",
            max_drawdown="0.09",
        ),
        testnet_stress=_stress_evidence(
            tmp_path,
            "stress/testnet-promoted.json",
            candidate_identity="momentum-001-v1",
            candidate_rules_sha256="rules-immutable",
            sample="testnet",
            position_risk="0.005",
            net_expectancy="0.2",
            max_daily_loss="0.019",
            max_drawdown="0.09",
        ),
    )
    baseline = _with_experiment_manifest(
        store, baseline, candidate_identity="momentum-001-v1"
    )
    promoted = _with_experiment_manifest(
        store, promoted, candidate_identity="momentum-001-v1"
    )

    result = gate.evaluate(
        "momentum-001-v1",
        baseline=baseline,
        promoted=promoted,
        baseline_promoted=True,
        soak_complete=True,
        promotion_artifact_key=promotion_key,
        soak_report_key=soak_key,
    )

    assert result.accepted
    assert result.proven_baseline_risk == Decimal("0.0025")
    assert (tmp_path / result.manifest_key).exists()
    assert result.baseline_preserved
    assert "0.25%" in result.report


def test_promoted_risk_rejects_changed_rules_or_deteriorated_limits(tmp_path: Path) -> None:
    gate = PromotedRiskGate(LocalObjectStore(tmp_path))
    baseline = _baseline_evaluation("rules-a")
    changed = _promoted_evaluation("rules-b")
    with pytest.raises(ValueError, match="rules"):
        gate.evaluate(
            "candidate-v1",
            baseline=baseline,
            promoted=changed,
            baseline_promoted=True,
            soak_complete=True,
        )

    deteriorated = _promoted_evaluation("rules-a", max_drawdown="0.101")
    result = gate.evaluate(
        "candidate-v1",
        baseline=baseline,
        promoted=deteriorated,
        baseline_promoted=True,
        soak_complete=True,
    )
    assert not result.accepted
    assert any("drawdown" in reason for reason in result.reasons)


def test_promoted_risk_requires_both_stress_results_and_rejects_control_deterioration(
    tmp_path: Path,
) -> None:
    gate = PromotedRiskGate(LocalObjectStore(tmp_path))
    baseline = _baseline_evaluation("rules-a")
    promoted = RiskEvaluation(
        position_risk=Decimal("0.005"),
        candidate_rules_sha256="rules-a",
        net_expectancy=Decimal("0.6"),
        max_daily_loss=Decimal("0.01"),
        max_drawdown=Decimal("0.08"),
        notional_fraction=Decimal("0.5"),
        cost_stress_passed=True,
        operational_controls_passed=False,
        concentration_passed=True,
        historical_stress=None,
        testnet_stress=None,
    )

    result = gate.evaluate(
        "candidate-v1",
        baseline=baseline,
        promoted=promoted,
        baseline_promoted=True,
        soak_complete=True,
    )

    assert not result.accepted
    assert any("stress" in reason for reason in result.reasons)
    assert any("operational" in reason for reason in result.reasons)
    manifest = (tmp_path / result.manifest_key).read_text(encoding="utf-8")
    assert '"proven_baseline_risk":"0.0025"' in manifest
