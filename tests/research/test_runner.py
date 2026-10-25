from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.domain.models import ExecutionCosts
from scalping_bot.research.candidates import CandidateBatch
from scalping_bot.research.evidence import CandidateLifecycle, CandidateRecord
from scalping_bot.research.execution import AggTrade
from scalping_bot.research.runner import ResearchBatchRunner


def test_batch_runner_derives_attempt_evidence_through_the_shared_engine() -> None:
    development_start = int(datetime(2020, 12, 2, tzinfo=UTC).timestamp() * 1000)
    validation_start = int(datetime(2021, 1, 2, tzinfo=UTC).timestamp() * 1000)
    trades = tuple(
        AggTrade(
            aggregate_trade_id=index,
            time_ms=development_start + index * 60_000,
            price=Decimal("100") + index * Decimal("2"),
            quantity=Decimal("1"),
            buyer_maker=False,
        )
        for index in range(8)
    ) + tuple(
        AggTrade(
            aggregate_trade_id=100 + index,
            time_ms=validation_start + index * 60_000,
            price=Decimal("100") + index * Decimal("2"),
            quantity=Decimal("1"),
            buyer_maker=False,
        )
        for index in range(8)
    )
    candidate = CandidateRecord(
        candidate_id="momentum-test",
        version=1,
        family="momentum",
        source_uri="test://preregistered",
        source_license="test",
        thesis="test",
        original_rules={"signal": "return"},
        modifications={},
        parameters={"window_bars": [2], "threshold": ["0.01"]},
        rejection_criteria=("non-positive expectancy",),
    )

    evaluation = ResearchBatchRunner(
        execution_costs=ExecutionCosts(commission_rate=Decimal("0.001"))
    ).run(trades, CandidateBatch((candidate,)))

    assert len(evaluation.ledger.attempts) == 1
    assert evaluation.ledger.attempts[0].candidate_identity == candidate.identity
    assert candidate.identity in evaluation.evidence
    assert not evaluation.evidence[candidate.identity].promoted
    assert evaluation.ledger.baseline_attempts
    assert evaluation.evidence[candidate.identity].baseline_metrics is not None
    assert set(evaluation.evidence[candidate.identity].baseline_cost_scenario_metrics) == {
        Decimal("1"),
        Decimal("1.5"),
        Decimal("2"),
    }
    assert evaluation.candidate_records[candidate.identity].lifecycle is CandidateLifecycle.REJECTED


def test_batch_runner_persists_an_immutable_experiment_manifest(tmp_path) -> None:
    candidate = CandidateRecord(
        candidate_id="momentum-manifest",
        version=1,
        family="momentum",
        source_uri="https://example.test/research",
        source_license="open",
        thesis="completed returns may persist",
        original_rules={"signal": "return"},
        modifications={},
        parameters={"window_bars": [2], "threshold": ["0.01"]},
        rejection_criteria=("negative expectancy",),
    )
    evaluation = ResearchBatchRunner(
        execution_costs=ExecutionCosts(
            commission_rate=Decimal("0.001"),
            commission_snapshot={
                "symbol": "BTCUSDT",
                "maker": "0.0002",
                "taker": "0.001",
                "captured_ms": "1",
            },
            execution_cost_model_identity="cost-model-test",
            calibration_method="test-calibration",
        ),
        manifest_store=LocalObjectStore(tmp_path),
        code_commit="abc123",
        dataset_checksums=("sha256:data",),
    ).run((), CandidateBatch((candidate,)))

    key = evaluation.manifest_keys[candidate.identity]
    assert (tmp_path / key).exists()
    assert '"code_commit":"abc123"' in (tmp_path / key).read_text()
