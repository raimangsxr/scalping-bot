from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.capture import CaptureLedger, RawSegmentWriter
from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.costs import (
    AccountCommissionSnapshot,
    CalibrationObservation,
    ExecutionCostCalibrator,
    ExecutionCostModelPublisher,
)
from scalping_bot.research.features import BookFlowFeatures
from scalping_bot.research.workflow import (
    PersistentAttemptLedger,
    ResearchArtifactWriter,
    ResearchRunManifest,
)
from scalping_bot.strategies.book_shadow import (
    BookShadowEvaluator,
    BookShadowObservation,
)


def test_research_manifest_binds_verified_inputs_and_seed(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    dataset = b"verified-catalog"
    cost = b"verified-cost-model"
    dataset_key = f"catalogs/{hashlib.sha256(dataset).hexdigest()}.json"
    cost_key = f"cost-models/{hashlib.sha256(cost).hexdigest()}.json"
    store.put_immutable(dataset_key, dataset)
    store.put_immutable(cost_key, cost)

    manifest = ResearchRunManifest.create(
        store=store,
        code_commit="abc123",
        dataset_catalog_key=dataset_key,
        execution_cost_model_key=cost_key,
        candidate_batch=first_batch(),
        configuration={"initial_equity": "10000"},
        period=("2020-01", "2025-09"),
        random_seed=7,
    )
    key = manifest.persist(store)

    assert key == manifest.persist(store)
    assert ResearchRunManifest.load(store, key) == manifest
    assert manifest.dataset_checksums == (
        hashlib.sha256(dataset).hexdigest(),
        hashlib.sha256(cost).hexdigest(),
    )
    assert manifest.verify(store)


def test_persistent_attempt_ledger_is_idempotent_and_rejects_mutation(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    ledger = PersistentAttemptLedger(store, run_identity="run-1")
    first = ledger.record(
        candidate_identity="momentum-001-v1",
        parameters={"window": 3},
        net_expectancy=Decimal("0.2"),
        baseline=Decimal("0.1"),
    )
    assert (
        ledger.record(
            candidate_identity="momentum-001-v1",
            parameters={"window": 3},
            net_expectancy=Decimal("0.2"),
            baseline=Decimal("0.1"),
        )
        == first
    )
    with pytest.raises(ValueError, match="immutable"):
        ledger.record(
            candidate_identity="momentum-001-v1",
            parameters={"window": 3},
            net_expectancy=Decimal("0.3"),
            baseline=Decimal("0.1"),
        )


def test_research_manifest_accepts_the_published_runtime_cost_artifact(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    dataset = b"verified-catalog"
    dataset_key = f"catalogs/{hashlib.sha256(dataset).hexdigest()}.json"
    store.put_immutable(dataset_key, dataset)
    model = ExecutionCostCalibrator().fit(
        (
            CalibrationObservation(
                time_ms=1,
                hour_utc=0,
                volatility_bucket="low",
                size_bucket="small",
                bid=Decimal("99"),
                ask=Decimal("100"),
                simulated_buy_price=Decimal("101"),
            ),
        ),
        commission=AccountCommissionSnapshot(
            symbol="BTCUSDT",
            maker=Decimal("0.001"),
            taker=Decimal("0.001"),
            captured_ms=1,
        ),
    )
    cost_key = ExecutionCostModelPublisher(store).publish(model)

    manifest = ResearchRunManifest.create(
        store=store,
        code_commit="abc123",
        dataset_catalog_key=dataset_key,
        execution_cost_model_key=cost_key,
        candidate_batch=first_batch(),
        configuration={"initial_equity": "10000"},
        period=("2020-01", "2025-09"),
        random_seed=7,
    )

    assert manifest.verify(store)


def test_research_artifact_bundle_contains_machine_and_human_reports(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    writer = ResearchArtifactWriter(store, run_identity="run-1")
    keys = writer.publish_empty_batch(first_batch())

    assert keys["candidate-records"].startswith("research-runs/run-1/")
    assert keys["attempt-ledger"].endswith("attempts.json")
    assert keys["markdown"].endswith("report.md")
    assert keys["html"].endswith("report.html")


def test_batch_evaluation_bundle_contains_primary_and_historical_sections(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    writer = ResearchArtifactWriter(store, run_identity="run-2")
    keys = writer.publish_batch_evaluation(
        first_batch(),
        evidence={},
        attempts=(),
        trades={},
        historical_evidence={},
        historical_trades={},
    )

    assert "Historical Stress" in store.read(keys["markdown"]).decode()
    assert '"candidates":' in store.read(keys["evidence"]).decode()


def test_book_shadow_evidence_is_persisted_as_non_promotable_artifact(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    evaluator = BookShadowEvaluator()
    evaluator.record(
        BookShadowObservation(
            event_time_ms=1_000,
            features=BookFlowFeatures(
                valid=True,
                best_quote_imbalance=Decimal("0.2"),
                depth_imbalance=Decimal("0.3"),
                market_data_quality="valid",
                segment_id="segment-1",
            ),
            baseline_entry=True,
            hypothetical_entry=True,
        )
    )

    key = evaluator.persist_report(store)
    report = store.read(key).decode()

    assert "Shadow Evaluation" in report
    assert "Promotion: prohibited" in report


def test_book_shadow_can_derive_valid_observations_from_capture_segments(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    ledger = CaptureLedger(store)
    trade_writer = RawSegmentWriter(store, stream="btcusdt-aggTrade", segment_started_ms=1_000)
    trade_writer.append(
        {
            "event_type": "aggTrade",
            "exchange_time_ms": 1_001,
            "receipt_time_ms": 1_002,
            "payload": {"a": 1, "T": 1_001, "p": "100", "q": "1", "m": False},
        }
    )
    trade_segment = trade_writer.close(valid=True)
    ledger.record(trade_segment, health={})

    depth_writer = RawSegmentWriter(store, stream="btcusdt-depth-100ms", segment_started_ms=1_000)
    depth_writer.append(
        {
            "event_type": "depth-snapshot",
            "exchange_time_ms": 1_000,
            "receipt_time_ms": 1_000,
            "payload": {
                "lastUpdateId": 100,
                "bids": [["99", "1"]],
                "asks": [["101", "1"]],
            },
        }
    )
    depth_writer.append(
        {
            "event_type": "depth@100ms",
            "exchange_time_ms": 1_001,
            "receipt_time_ms": 1_002,
            "payload": {"U": 101, "u": 101, "b": [["99", "2"]], "a": []},
        }
    )
    depth_segment = depth_writer.close(valid=True)
    ledger.record(depth_segment, health={})

    evaluator = BookShadowEvaluator.from_capture(store)
    report = evaluator.report()

    assert report.observations == 1
    assert report.valid_observations == 1
    assert report.gaps == 0
    assert evaluator.observations[0].hypothetical_entry
