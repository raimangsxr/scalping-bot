from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.operations.promoted_risk_trial import run_promoted_risk_trial
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.costs import (
    AccountCommissionSnapshot,
    CalibrationObservation,
    ExecutionCostCalibrator,
    ExecutionCostModelPublisher,
)
from scalping_bot.research.evidence import CandidateLifecycle


def _ready_candidate_manifest(
    path: Path, execution_cost_model_identity: str
) -> PromotedCandidateManifest:
    candidate = first_batch().candidates[0]
    for lifecycle in (
        CandidateLifecycle.SPECIFIED,
        CandidateLifecycle.SCREENED,
        CandidateLifecycle.WALK_FORWARD_VALIDATED,
        CandidateLifecycle.HOLDOUT_TESTED,
        CandidateLifecycle.TESTNET_READY,
    ):
        candidate = candidate.transition(lifecycle, evidence="test")
    manifest = PromotedCandidateManifest.from_candidate(
        candidate,
        parameters={"window_bars": 3, "threshold": "0.005"},
        execution_cost_model_identity=execution_cost_model_identity,
    )
    manifest.persist(path)
    return manifest


def _promotion_and_soak(
    store: LocalObjectStore, manifest: PromotedCandidateManifest
) -> tuple[str, str]:
    promotion = json.dumps(
        {
            "candidate_identity": manifest.candidate_identity,
            "candidate_rules_sha256": manifest.rules_sha256,
            "decision": "PROMOTED",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    promotion_key = f"promotion/{hashlib.sha256(promotion).hexdigest()}.json"
    store.put_immutable(promotion_key, promotion)
    soak = (
        f"# Testnet Soak — {manifest.candidate_identity}\n"
        "Decision: COMPLETE\nExecuted scalps: 300/300\n"
    ).encode()
    soak_key = f"testnet-soak/{hashlib.sha256(soak).hexdigest()}.md"
    store.put_immutable(soak_key, soak)
    return promotion_key, soak_key


def _historical_parquet(path: Path) -> None:
    rows: list[dict[str, object]] = []
    aggregate_trade_id = 1
    for year, month in (
        (year, month)
        for year in (2017, 2018, 2019)
        for month in range(1, 13)
        if (year, month) >= (2017, 8)
    ):
        if year == 2019 and month > 12:
            continue
        timestamp = int(datetime(year, month, 1, tzinfo=UTC).timestamp() * 1000)
        rows.append(
            {
                "aggregate_trade_id": aggregate_trade_id,
                "time_ms": timestamp,
                "price": "100",
                "quantity": "1",
                "buyer_maker": False,
            }
        )
        aggregate_trade_id += 1
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _testnet_stress(
    path: Path, manifest: PromotedCandidateManifest, risk: str, source_key: str
) -> None:
    path.write_text(
        json.dumps(
            {
                "candidate_identity": manifest.candidate_identity,
                "candidate_rules_sha256": manifest.rules_sha256,
                "sample": "testnet",
                "position_risk": risk,
                "net_expectancy": "1",
                "max_daily_loss": "0.01",
                "max_drawdown": "0.05",
                "cost_stress_passed": True,
                "operational_controls_passed": True,
                "concentration_passed": True,
                "source_evidence": [source_key],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_promoted_risk_trial_runs_both_risks_and_binds_immutable_manifests(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    candidate_path = tmp_path / "candidate.json"
    store.put_immutable("capture/immutable-segment", b"immutable capture segment\n")
    source_key = "testnet/source-evidence.json"
    store.put_immutable(source_key, b"immutable testnet source\n")
    cost_model = ExecutionCostCalibrator().fit(
        (
            CalibrationObservation(
                time_ms=0,
                hour_utc=0,
                volatility_bucket="low",
                size_bucket="small",
                bid=Decimal("99"),
                ask=Decimal("100"),
                simulated_buy_price=Decimal("100"),
            ),
        ),
        commission=AccountCommissionSnapshot(
            symbol="BTCUSDT",
            maker=Decimal("0.0001"),
            taker=Decimal("0.0004"),
            captured_ms=1,
        ),
    )
    cost_key = ExecutionCostModelPublisher(store).publish(
        cost_model, source_evidence=("capture/immutable-segment",)
    )
    cost_identity = json.loads(store.read(cost_key).decode())["sha256"]
    manifest = _ready_candidate_manifest(candidate_path, cost_identity)
    promotion_key, soak_key = _promotion_and_soak(store, manifest)
    historical_path = tmp_path / "historical.parquet"
    _historical_parquet(historical_path)
    baseline_path = tmp_path / "testnet-baseline.json"
    promoted_path = tmp_path / "testnet-promoted.json"
    _testnet_stress(baseline_path, manifest, "0.0025", source_key)
    _testnet_stress(promoted_path, manifest, "0.005", source_key)

    trial = run_promoted_risk_trial(
        store=store,
        candidate_manifest_path=candidate_path,
        historical_parquet=(historical_path,),
        execution_cost_model_key=cost_key,
        promotion_artifact_key=promotion_key,
        soak_report_key=soak_key,
        testnet_baseline_path=baseline_path,
        testnet_promoted_path=promoted_path,
        code_commit="test-commit",
    )

    assert not trial.result.accepted
    assert trial.baseline.position_risk == Decimal("0.0025")
    assert trial.promoted.position_risk == Decimal("0.005")
    assert trial.baseline_experiment_manifest_key != trial.promoted_experiment_manifest_key
    for key in (
        trial.baseline_experiment_manifest_key,
        trial.promoted_experiment_manifest_key,
        trial.baseline.historical_stress.artifact_key,
        trial.promoted.historical_stress.artifact_key,
        trial.baseline.testnet_stress.artifact_key,
        trial.promoted.testnet_stress.artifact_key,
    ):
        assert key is not None
        assert (tmp_path / "objects" / key).exists()
