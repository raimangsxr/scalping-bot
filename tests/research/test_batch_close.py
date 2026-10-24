from decimal import Decimal
from pathlib import Path

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.research.batch import BatchCloser
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.evidence import EvidenceProtocol, HoldoutRegistry, ScalpResult
from scalping_bot.research.screening import AttemptLedger


def test_batch_close_accounts_for_every_candidate_and_can_reject_all(tmp_path: Path) -> None:
    batch = first_batch()
    ledger = AttemptLedger()
    for candidate in batch.candidates:
        ledger.record(
            candidate.identity,
            candidate.parameters,
            net_expectancy=Decimal("-0.1"),
            baseline=Decimal("0"),
        )

    review = BatchCloser(HoldoutRegistry(LocalObjectStore(tmp_path))).close(
        batch, ledger=ledger, evidence={}
    )

    assert len(review.inventory) == 8
    assert not review.testnet_ready_candidates
    assert "No candidate is Testnet Ready" in review.markdown
    assert "Research Target is aspirational" in review.markdown
    assert "Final Holdout was not revealed" in review.markdown


def test_only_prior_gate_passers_receive_a_single_holdout_reveal(tmp_path: Path) -> None:
    batch = first_batch()
    candidate = batch.candidates[0]
    ledger = AttemptLedger()
    ledger.record(
        candidate.identity,
        candidate.parameters,
        net_expectancy=Decimal("0.8"),
        baseline=Decimal("0.1"),
    )
    trades = tuple(
        ScalpResult(
            scalp_id=f"s-{index}",
            opened_ms=1_700_000_000_000 + index * 86_400_000,
            closed_ms=1_700_000_060_000 + index * 86_400_000,
            entry_price=Decimal("100"),
            exit_price=Decimal("101"),
            quantity=Decimal("1"),
            gross_pnl=Decimal("1"),
            costs=Decimal("0.2"),
            fold=index % 10,
        )
        for index in range(300)
    )
    evidence = EvidenceProtocol().evaluate(trades, initial_equity=Decimal("10000"), attempts=8)

    review = BatchCloser(HoldoutRegistry(LocalObjectStore(tmp_path))).close(
        batch, ledger=ledger, evidence={candidate.identity: evidence}
    )

    assert review.holdout_reveals == (f"holdout-access/{candidate.identity}.json",)
    assert candidate.identity not in review.testnet_ready_candidates
    assert "awaits Final Holdout evidence" in review.markdown
