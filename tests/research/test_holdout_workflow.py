from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.research.evidence import (
    CandidateLifecycle,
    CandidateRecord,
    EvidenceMetrics,
    EvidenceResult,
    HoldoutRegistry,
)
from scalping_bot.research.holdout import FinalHoldoutGate


def _finalist() -> CandidateRecord:
    return CandidateRecord(
        candidate_id="momentum-001",
        version=1,
        family="momentum",
        source_uri="https://example.test/source",
        source_license="open",
        thesis="completed returns may persist",
        original_rules={"window": 3},
        modifications={},
        parameters={"window": [3]},
        rejection_criteria=("negative expectancy",),
        lifecycle=CandidateLifecycle.WALK_FORWARD_VALIDATED,
        frozen=True,
    )


def _evidence(promoted: bool) -> EvidenceResult:
    metrics = EvidenceMetrics(
        net_return=Decimal("0.1"),
        max_drawdown=Decimal("0.03"),
        maximum_daily_loss=Decimal("0.01"),
        expectancy=Decimal("1"),
        profit_factor=Decimal("2"),
        daily_sharpe=None,
        daily_sortino=None,
        win_rate=Decimal("0.6"),
        exposure=Decimal("0.1"),
        turnover=Decimal("1"),
        trade_count=300,
    )
    return EvidenceResult(
        metrics=metrics,
        bootstrap_lower_95=Decimal("0.1"),
        positive_fold_fraction=Decimal("1"),
        concentration_failed=False,
        promoted=promoted,
        selection_adjusted_alpha=Decimal("0.00625"),
        cost_stress_multipliers=(Decimal("1"), Decimal("1.5"), Decimal("2")),
        market_fidelity_verified=True,
    )


def test_final_holdout_reveal_creates_one_decision_and_ready_manifest(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    decision = FinalHoldoutGate(store, HoldoutRegistry(store)).evaluate(
        _finalist(),
        evidence=_evidence(True),
        holdout_dataset_checksum="a" * 64,
        actor="operator",
    )

    assert decision.decision == "Testnet Ready"
    assert decision.candidate_manifest_key is not None
    assert store.read(decision.report_key).decode().find("Research Target") >= 0
