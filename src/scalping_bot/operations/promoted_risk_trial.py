from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from scalping_bot.data.storage import ObjectStore
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.operations.promotion import (
    DEFAULT_RISK_CONTROLS,
    INITIAL_POSITION_RISK,
    MAX_DAILY_LOSS,
    MAX_DRAWDOWN,
    MAX_NOTIONAL_FRACTION,
    PROMOTED_POSITION_RISK,
    PromotedRiskGate,
    PromotedRiskResult,
    RiskEvaluation,
    StressEvaluation,
)
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.costs import ExecutionCostModel
from scalping_bot.research.evidence import (
    CandidateRecord,
    EvidenceProtocol,
    EvidenceResult,
    ScalpResult,
)
from scalping_bot.research.execution import AggTrade, canonical_aggregate_trade_bytes
from scalping_bot.research.runner import ResearchBatchRunner, load_aggtrades


@dataclass(frozen=True)
class PromotedRiskTrial:
    """The immutable output of one engine-backed 0.5% risk comparison."""

    result: PromotedRiskResult
    baseline: RiskEvaluation
    promoted: RiskEvaluation
    baseline_experiment_manifest_key: str
    promoted_experiment_manifest_key: str
    historical_dataset_checksum: str


def run_promoted_risk_trial(
    *,
    store: ObjectStore,
    candidate_manifest_path: Path,
    historical_parquet: tuple[Path, ...],
    execution_cost_model_key: str,
    promotion_artifact_key: str,
    soak_report_key: str,
    testnet_baseline_path: Path,
    testnet_promoted_path: Path,
    code_commit: str,
) -> PromotedRiskTrial:
    """Run both risk levels through the shared research engine.

    Historical evidence is generated here. Testnet stress is deliberately an input: this
    command verifies the completed operational evidence, binds it to the same candidate,
    dataset, cost model, and experiment manifest, and never fabricates exchange activity.
    """

    candidate_manifest = PromotedCandidateManifest.load(candidate_manifest_path)
    candidate = _frozen_candidate(candidate_manifest)
    gate = PromotedRiskGate(store)
    baseline_promoted = gate.promotion_evidence_valid(
        promotion_artifact_key,
        candidate.identity,
        candidate_manifest.rules_sha256,
    )
    if not baseline_promoted:
        raise ValueError("Promoted Risk requires checksum-verified 0.25% promotion evidence")
    soak_complete = gate.soak_evidence_valid(soak_report_key, candidate.identity)
    if not soak_complete:
        raise ValueError("Promoted Risk requires a checksum-verified completed Testnet Soak")

    cost_model = ExecutionCostModel.load(store, execution_cost_model_key)
    if candidate_manifest.execution_cost_model_identity != cost_model.identity:
        raise ValueError("candidate and execution cost model identities do not match")
    historical_trades = load_aggtrades(historical_parquet)
    _require_historical_stress_window(historical_trades)
    historical_dataset_checksum = hashlib.sha256(
        canonical_aggregate_trade_bytes(historical_trades)
    ).hexdigest()
    dataset_checksums = (historical_dataset_checksum, cost_model.identity)

    baseline_evidence, baseline_trades = _run_historical_engine(
        candidate,
        historical_trades,
        cost_model=cost_model,
        position_risk=INITIAL_POSITION_RISK,
        dataset_checksum=historical_dataset_checksum,
    )
    promoted_evidence, promoted_trades = _run_historical_engine(
        candidate,
        historical_trades,
        cost_model=cost_model,
        position_risk=PROMOTED_POSITION_RISK,
        dataset_checksum=historical_dataset_checksum,
    )

    baseline_testnet = _persist_testnet_stress(
        store,
        path=testnet_baseline_path,
        candidate_identity=candidate.identity,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        expected_position_risk=INITIAL_POSITION_RISK,
        soak_report_key=soak_report_key,
    )
    promoted_testnet = _persist_testnet_stress(
        store,
        path=testnet_promoted_path,
        candidate_identity=candidate.identity,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        expected_position_risk=PROMOTED_POSITION_RISK,
        soak_report_key=soak_report_key,
    )

    baseline_historical = _persist_historical_stress(
        store,
        evidence=baseline_evidence,
        trades=baseline_trades,
        candidate_identity=candidate.identity,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        position_risk=INITIAL_POSITION_RISK,
        dataset_checksum=historical_dataset_checksum,
        cost_model_identity=cost_model.identity,
    )
    promoted_historical = _persist_historical_stress(
        store,
        evidence=promoted_evidence,
        trades=promoted_trades,
        candidate_identity=candidate.identity,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        position_risk=PROMOTED_POSITION_RISK,
        dataset_checksum=historical_dataset_checksum,
        cost_model_identity=cost_model.identity,
    )

    baseline_manifest_key = _persist_experiment_manifest(
        store,
        candidate_identity=candidate.identity,
        candidate_parameters=candidate.parameters,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        position_risk=INITIAL_POSITION_RISK,
        dataset_checksums=dataset_checksums,
        cost_model_identity=cost_model.identity,
        cost_model=cost_model,
        code_commit=code_commit,
        historical_stress=baseline_historical,
        testnet_stress=baseline_testnet,
    )
    promoted_manifest_key = _persist_experiment_manifest(
        store,
        candidate_identity=candidate.identity,
        candidate_parameters=candidate.parameters,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        position_risk=PROMOTED_POSITION_RISK,
        dataset_checksums=dataset_checksums,
        cost_model_identity=cost_model.identity,
        cost_model=cost_model,
        code_commit=code_commit,
        historical_stress=promoted_historical,
        testnet_stress=promoted_testnet,
    )

    baseline = _risk_evaluation(
        position_risk=INITIAL_POSITION_RISK,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        evidence=baseline_evidence,
        historical_stress=baseline_historical,
        testnet_stress=baseline_testnet,
        dataset_checksums=dataset_checksums,
        cost_model_identity=cost_model.identity,
        experiment_manifest_key=baseline_manifest_key,
    )
    promoted = _risk_evaluation(
        position_risk=PROMOTED_POSITION_RISK,
        candidate_rules_sha256=candidate_manifest.rules_sha256,
        evidence=promoted_evidence,
        historical_stress=promoted_historical,
        testnet_stress=promoted_testnet,
        dataset_checksums=dataset_checksums,
        cost_model_identity=cost_model.identity,
        experiment_manifest_key=promoted_manifest_key,
    )
    result = gate.evaluate(
        candidate.identity,
        baseline=baseline,
        promoted=promoted,
        baseline_promoted=baseline_promoted,
        soak_complete=soak_complete,
        promotion_artifact_key=promotion_artifact_key,
        soak_report_key=soak_report_key,
    )
    return PromotedRiskTrial(
        result=result,
        baseline=baseline,
        promoted=promoted,
        baseline_experiment_manifest_key=baseline_manifest_key,
        promoted_experiment_manifest_key=promoted_manifest_key,
        historical_dataset_checksum=historical_dataset_checksum,
    )


def _frozen_candidate(manifest: PromotedCandidateManifest) -> CandidateRecord:
    candidate = next(
        item for item in first_batch().candidates if item.identity == manifest.candidate_identity
    )
    return CandidateRecord(
        candidate_id=candidate.candidate_id,
        version=candidate.version,
        family=candidate.family,
        source_uri=candidate.source_uri,
        source_license=candidate.source_license,
        thesis=candidate.thesis,
        original_rules=candidate.original_rules,
        modifications=candidate.modifications,
        parameters=dict(manifest.parameters),
        rejection_criteria=candidate.rejection_criteria,
        lifecycle=candidate.lifecycle,
        history=candidate.history,
        frozen=True,
        market_data_dependencies=candidate.market_data_dependencies,
    )


def _run_historical_engine(
    candidate: CandidateRecord,
    trades: tuple[AggTrade, ...],
    *,
    cost_model: ExecutionCostModel,
    position_risk: Decimal,
    dataset_checksum: str,
) -> tuple[EvidenceResult, tuple[ScalpResult, ...]]:
    runner = ResearchBatchRunner(
        execution_costs=cost_model.as_engine_costs(),
        code_commit="promoted-risk-trial",
        dataset_checksums=(dataset_checksum, cost_model.identity),
        position_risk=position_risk,
    )
    return runner.evaluate_frozen_candidate(
        candidate,
        trades,
        dataset_checksum=dataset_checksum,
        execution_cost_model_identity=cost_model.identity,
    )


def _persist_historical_stress(
    store: ObjectStore,
    *,
    evidence: EvidenceResult,
    trades: tuple[ScalpResult, ...],
    candidate_identity: str,
    candidate_rules_sha256: str,
    position_risk: Decimal,
    dataset_checksum: str,
    cost_model_identity: str,
) -> StressEvaluation:
    scenario_metrics = evidence.cost_scenario_metrics
    required = (Decimal("1"), Decimal("1.5"), Decimal("2"))
    expectancy_values: list[Decimal | None] = []
    for multiplier in required:
        metric = scenario_metrics.get(multiplier)
        expectancy_values.append(metric.expectancy if metric is not None else None)
    cost_stress_passed = len(expectancy_values) == len(required) and all(
        value is not None and value > 0 for value in expectancy_values
    )
    operational_controls_passed = bool(trades) and all(
        trade.risk_compliant and trade.market_fidelity == "aggTrades" for trade in trades
    )
    concentration_passed = not evidence.concentration_failed
    return _persist_stress(
        store,
        {
            "schema_version": 1,
            "candidate_identity": candidate_identity,
            "candidate_rules_sha256": candidate_rules_sha256,
            "sample": "historical",
            "position_risk": str(position_risk),
            "net_expectancy": str(evidence.metrics.expectancy or Decimal(0)),
            "max_daily_loss": str(evidence.metrics.maximum_daily_loss),
            "max_drawdown": str(evidence.metrics.max_drawdown),
            "cost_stress_passed": cost_stress_passed,
            "operational_controls_passed": operational_controls_passed,
            "concentration_passed": concentration_passed,
            "dataset_checksum": dataset_checksum,
            "execution_cost_model_sha256": cost_model_identity,
            "evidence": "ResearchBatchRunner.evaluate_frozen_candidate",
        },
    )


def _persist_testnet_stress(
    store: ObjectStore,
    *,
    path: Path,
    candidate_identity: str,
    candidate_rules_sha256: str,
    expected_position_risk: Decimal,
    soak_report_key: str,
) -> StressEvaluation:
    raw_bytes = path.read_bytes()
    try:
        raw = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Testnet stress evidence is not valid JSON: {path}") from error
    if not isinstance(raw, dict):
        raise ValueError("Testnet stress evidence must be a JSON object")
    if raw.get("candidate_identity") != candidate_identity:
        raise ValueError("Testnet stress evidence targets another candidate")
    if raw.get("candidate_rules_sha256") != candidate_rules_sha256:
        raise ValueError("Testnet stress evidence targets another ruleset")
    if raw.get("sample") != "testnet":
        raise ValueError("Testnet stress evidence must identify the testnet sample")
    try:
        position_risk = Decimal(str(raw["position_risk"]))
    except (InvalidOperation, KeyError, TypeError, ValueError) as error:
        raise ValueError("Testnet stress evidence has no valid position risk") from error
    if position_risk != expected_position_risk:
        raise ValueError("Testnet stress evidence has the wrong position risk")
    source_evidence = raw.get("source_evidence")
    if (
        not isinstance(source_evidence, list)
        or not source_evidence
        or any(not isinstance(item, str) or not item for item in source_evidence)
    ):
        raise ValueError("Testnet stress evidence requires immutable source_evidence")
    resolved_sources: list[str] = []
    for source_reference in source_evidence:
        source_key, declared_checksum = (
            source_reference.rsplit("#", 1) if "#" in source_reference else (source_reference, None)
        )
        try:
            source_content = store.read(source_key)
        except (KeyError, OSError) as error:
            raise ValueError(f"Testnet source evidence is unavailable: {source_key}") from error
        source_checksum = hashlib.sha256(source_content).hexdigest()
        if declared_checksum is not None and declared_checksum.lower() != source_checksum:
            raise ValueError(f"Testnet source evidence checksum does not match: {source_key}")
        resolved_sources.append(f"{source_key}#{source_checksum}")
    try:
        soak_content = store.read(soak_report_key)
    except (KeyError, OSError) as error:
        raise ValueError("Testnet Soak source evidence is unavailable") from error
    resolved_sources.append(f"{soak_report_key}#{hashlib.sha256(soak_content).hexdigest()}")
    payload = {
        "schema_version": 1,
        "candidate_identity": candidate_identity,
        "candidate_rules_sha256": candidate_rules_sha256,
        "sample": "testnet",
        "position_risk": str(position_risk),
        "net_expectancy": str(_required_decimal(raw, "net_expectancy")),
        "max_daily_loss": str(_required_decimal(raw, "max_daily_loss")),
        "max_drawdown": str(_required_decimal(raw, "max_drawdown")),
        "cost_stress_passed": _required_bool(raw, "cost_stress_passed"),
        "operational_controls_passed": _required_bool(raw, "operational_controls_passed"),
        "concentration_passed": _required_bool(raw, "concentration_passed"),
        "source_evidence": sorted(set(resolved_sources)),
        "source_sha256": hashlib.sha256(raw_bytes).hexdigest(),
    }
    return _persist_stress(store, payload)


def _persist_stress(store: ObjectStore, payload: dict[str, object]) -> StressEvaluation:
    content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    identity = hashlib.sha256(content).hexdigest()
    candidate = str(payload["candidate_identity"])
    sample = str(payload["sample"])
    position = str(payload["position_risk"]).replace(".", "_")
    key = f"promoted-risk/stress/{candidate}/{sample}/{position}/{identity}.json"
    store.put_immutable(key, content)
    return StressEvaluation(
        net_expectancy=Decimal(str(payload["net_expectancy"])),
        max_daily_loss=Decimal(str(payload["max_daily_loss"])),
        max_drawdown=Decimal(str(payload["max_drawdown"])),
        cost_stress_passed=bool(payload["cost_stress_passed"]),
        operational_controls_passed=bool(payload["operational_controls_passed"]),
        concentration_passed=bool(payload["concentration_passed"]),
        artifact_key=key,
        artifact_sha256=identity,
    )


def _persist_experiment_manifest(
    store: ObjectStore,
    *,
    candidate_identity: str,
    candidate_parameters: Mapping[str, object],
    candidate_rules_sha256: str,
    position_risk: Decimal,
    dataset_checksums: tuple[str, ...],
    cost_model_identity: str,
    cost_model: ExecutionCostModel,
    code_commit: str,
    historical_stress: StressEvaluation,
    testnet_stress: StressEvaluation,
) -> str:
    payload = {
        "schema_version": 1,
        "code_commit": code_commit,
        "candidate_identity": candidate_identity,
        "candidate_rules_sha256": candidate_rules_sha256,
        "configuration": {
            "candidate_parameters": dict(candidate_parameters),
            "position_risk": str(position_risk),
            "notional_fraction": str(MAX_NOTIONAL_FRACTION),
            "daily_loss_limit": str(MAX_DAILY_LOSS),
            "drawdown_limit": str(MAX_DRAWDOWN),
            "risk_controls": list(DEFAULT_RISK_CONTROLS),
        },
        "commission_snapshot": {
            "symbol": cost_model.commission.symbol,
            "maker": str(cost_model.commission.maker),
            "taker": str(cost_model.commission.taker),
            "captured_ms": cost_model.commission.captured_ms,
            "account_context": cost_model.commission.account_context,
        },
        "execution_cost_model": {
            "identity": cost_model_identity,
            "half_spread_rate": str(cost_model.as_engine_costs().half_spread_rate),
            "slippage_rate": str(cost_model.as_engine_costs().slippage_rate),
        },
        "period": [
            f"{EvidenceProtocol.PRIMARY_START[0]:04d}-{EvidenceProtocol.PRIMARY_START[1]:02d}",
            f"{EvidenceProtocol.PRIMARY_END[0]:04d}-{EvidenceProtocol.PRIMARY_END[1]:02d}",
        ],
        "random_seed": 7,
        "position_risk": str(position_risk),
        "notional_fraction": str(MAX_NOTIONAL_FRACTION),
        "daily_loss_limit": str(MAX_DAILY_LOSS),
        "drawdown_limit": str(MAX_DRAWDOWN),
        "risk_controls": list(DEFAULT_RISK_CONTROLS),
        "dataset_checksums": list(dataset_checksums),
        "execution_cost_model_sha256": cost_model_identity,
        "historical_stress_artifact_key": historical_stress.artifact_key,
        "testnet_stress_artifact_key": testnet_stress.artifact_key,
    }
    content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    identity = hashlib.sha256(content).hexdigest()
    key = f"promoted-risk/experiments/{candidate_identity}/{identity}.json"
    store.put_immutable(key, content)
    return key


def _risk_evaluation(
    *,
    position_risk: Decimal,
    candidate_rules_sha256: str,
    evidence: EvidenceResult,
    historical_stress: StressEvaluation,
    testnet_stress: StressEvaluation,
    dataset_checksums: tuple[str, ...],
    cost_model_identity: str,
    experiment_manifest_key: str,
) -> RiskEvaluation:
    return RiskEvaluation(
        position_risk=position_risk,
        candidate_rules_sha256=candidate_rules_sha256,
        net_expectancy=evidence.metrics.expectancy or Decimal(0),
        max_daily_loss=evidence.metrics.maximum_daily_loss,
        max_drawdown=evidence.metrics.max_drawdown,
        notional_fraction=MAX_NOTIONAL_FRACTION,
        cost_stress_passed=historical_stress.cost_stress_passed,
        operational_controls_passed=historical_stress.operational_controls_passed,
        concentration_passed=historical_stress.concentration_passed,
        daily_loss_limit=MAX_DAILY_LOSS,
        drawdown_limit=MAX_DRAWDOWN,
        risk_controls=DEFAULT_RISK_CONTROLS,
        historical_stress=historical_stress,
        testnet_stress=testnet_stress,
        dataset_checksums=dataset_checksums,
        execution_cost_model_sha256=cost_model_identity,
        experiment_manifest_key=experiment_manifest_key,
    )


def _require_historical_stress_window(trades: tuple[AggTrade, ...]) -> None:
    months = {_month(item.time_ms) for item in trades}
    expected = {
        f"{year:04d}-{month:02d}"
        for year in (2017, 2018, 2019)
        for month in range(1, 13)
        if (year, month) >= (2017, 8) and (year, month) <= (2019, 12)
    }
    if months != expected:
        raise ValueError(
            "Historical Stress must cover every month from August 2017 through December 2019"
        )


def _month(time_ms: int) -> str:
    from datetime import UTC, datetime

    value = datetime.fromtimestamp(time_ms / 1000, tz=UTC)
    return f"{value.year:04d}-{value.month:02d}"


def _required_decimal(value: Mapping[str, object], key: str) -> Decimal:
    try:
        parsed = Decimal(str(value[key]))
    except (InvalidOperation, KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Testnet stress evidence has no valid {key}") from error
    if not parsed.is_finite():
        raise ValueError(f"Testnet stress evidence has a non-finite {key}")
    return parsed


def _required_bool(value: Mapping[str, object], key: str) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        raise ValueError(f"Testnet stress evidence has no boolean {key}")
    return item
