from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.research.evidence import (
    CandidateLifecycle,
    CandidateRecord,
    EvidenceProtocol,
    ExperimentManifest,
    HoldoutAccessError,
    HoldoutRegistry,
    ScalpResult,
)
from scalping_bot.research.reporting import render_evidence_html, render_evidence_markdown


def _ms(year: int, month: int, day: int = 1) -> int:
    return int(datetime(year, month, day, tzinfo=UTC).timestamp() * 1000)


def _candidate() -> CandidateRecord:
    return CandidateRecord(
        candidate_id="momentum-001",
        version=1,
        family="momentum",
        source_uri="https://papers.ssrn.com/example",
        source_license="open-access",
        thesis="recent positive returns may persist briefly",
        original_rules={"window": 3},
        modifications={},
        parameters={"threshold": "0.01"},
        rejection_criteria=("negative net expectancy",),
    )


def test_candidate_lifecycle_is_forward_only_and_rejection_is_terminal() -> None:
    candidate = _candidate()
    specified = candidate.transition(CandidateLifecycle.SPECIFIED, evidence="preregistered")
    screened = specified.transition(CandidateLifecycle.SCREENED, evidence="screening-1")
    rejected = screened.transition(CandidateLifecycle.REJECTED, evidence="negative expectancy")

    with pytest.raises(ValueError, match="terminal"):
        rejected.transition(CandidateLifecycle.SPECIFIED, evidence="changed our mind")
    assert candidate.lifecycle is CandidateLifecycle.PROPOSED
    assert len(rejected.history) == 3


def test_experiment_manifest_and_rejected_variant_are_immutable(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    manifest = ExperimentManifest(
        code_commit="abc123",
        candidate_identity="momentum-001-v1",
        configuration={"window": 3},
        dataset_checksums=("sha256:data",),
        commission_snapshot={"taker": "0.001"},
        execution_cost_model={"spread_bps": "1", "slippage_bps": "2"},
        period=("2020-01-01", "2025-09-30"),
        random_seed=7,
    )

    first = manifest.persist(store)
    second = manifest.persist(store)

    assert first == second
    assert json.loads(store.read(first))["candidate_identity"] == "momentum-001-v1"


def test_walk_forward_folds_are_twelve_development_then_three_unseen_months() -> None:
    folds = EvidenceProtocol().walk_forward_folds()

    assert folds[0].development == ("2020-01", "2020-12")
    assert folds[0].validation == ("2021-01", "2021-03")
    assert folds[1].development == ("2020-04", "2021-03")
    assert folds[1].validation == ("2021-04", "2021-06")
    assert folds[-1].validation[1] <= "2025-09"


def test_daily_block_bootstrap_and_concentration_feed_promotion_decision() -> None:
    trades = tuple(
        ScalpResult(
            scalp_id=f"s-{index}",
            opened_ms=_ms(2024, 1 + (index // 25) % 12, 1 + index % 20),
            closed_ms=_ms(2024, 1 + (index // 25) % 12, 1 + index % 20) + 60_000,
            entry_price=Decimal("100"),
            exit_price=Decimal("101"),
            quantity=Decimal("1"),
            gross_pnl=Decimal("1"),
            costs=Decimal("0.20"),
            fold=index % 10,
        )
        for index in range(300)
    )
    result = EvidenceProtocol().evaluate(trades, initial_equity=Decimal("10000"), attempts=8)

    assert result.metrics.trade_count == 300
    assert result.metrics.expectancy == Decimal("0.80")
    assert result.bootstrap_lower_95 == Decimal("0.8")
    assert result.positive_fold_fraction == Decimal("1")
    assert result.promoted
    assert not result.concentration_failed


def test_final_holdout_requires_frozen_finalist_and_can_be_revealed_once(tmp_path: Path) -> None:
    candidate = _candidate().transition(CandidateLifecycle.SPECIFIED, evidence="registered")
    registry = HoldoutRegistry(LocalObjectStore(tmp_path))

    with pytest.raises(HoldoutAccessError, match="frozen"):
        registry.reveal(candidate, command="reveal-final-holdout", actor="operator")

    finalist = (
        candidate.transition(CandidateLifecycle.SCREENED, evidence="screen")
        .transition(CandidateLifecycle.WALK_FORWARD_VALIDATED, evidence="wf")
        .freeze()
    )
    audit_key = registry.reveal(finalist, command="reveal-final-holdout", actor="operator")
    assert json.loads((tmp_path / audit_key).read_text())["period"] == [
        "2025-10-01",
        "2026-09-30",
    ]

    with pytest.raises(HoldoutAccessError, match="already"):
        registry.reveal(finalist, command="reveal-final-holdout", actor="operator")


def test_static_reports_state_decision_and_reconstruct_scalp() -> None:
    trade = ScalpResult(
        scalp_id="s-1",
        opened_ms=_ms(2024, 1),
        closed_ms=_ms(2024, 1) + 60_000,
        entry_price=Decimal("100"),
        exit_price=Decimal("101"),
        quantity=Decimal("1"),
        gross_pnl=Decimal("1"),
        costs=Decimal("0.2"),
        fold=0,
        trace_event_ids=("signal-1", "order-1", "fill-1"),
    )
    evidence = EvidenceProtocol().evaluate((trade,), initial_equity=Decimal("10000"), attempts=1)

    markdown = render_evidence_markdown("candidate-v1", evidence, (trade,))
    html = render_evidence_html("candidate-v1", evidence, (trade,))

    assert "REJECTED" in markdown
    assert "signal-1 → order-1 → fill-1" in markdown
    assert "REJECTED" in html
    assert "candidate-v1" in html
