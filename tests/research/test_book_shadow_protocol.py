from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.research.features import BookFlowFeatures
from scalping_bot.strategies.book_shadow import (
    BookShadowEvaluator,
    BookShadowObservation,
    BookShadowProtocol,
)


def test_book_shadow_protocol_is_content_addressed_and_round_trips(tmp_path: Path) -> None:
    protocol = BookShadowProtocol(
        candidate_identities=("momentum-001-v1", "trade-flow-001-v1"),
        start_ms=1_700_000_000_000,
        end_ms=1_700_086_400_000,
        min_imbalance=Decimal("0.10"),
        max_gap_ms=60_000,
        minimum_valid_coverage=Decimal("0.80"),
    )
    store = LocalObjectStore(tmp_path)

    key = protocol.persist(store)
    loaded = BookShadowProtocol.load(store, key)

    assert key == f"shadow/protocols/{protocol.identity}.json"
    assert loaded == protocol


def test_book_shadow_protocol_rejects_invalid_windows_and_coverage() -> None:
    with pytest.raises(ValueError, match="ordered"):
        BookShadowProtocol(
            candidate_identities=("candidate-v1",),
            start_ms=2,
            end_ms=1,
        )
    with pytest.raises(ValueError, match="coverage"):
        BookShadowProtocol(
            candidate_identities=("candidate-v1",),
            start_ms=1,
            end_ms=2,
            minimum_valid_coverage=Decimal("1.1"),
        )


def test_book_shadow_evidence_binds_protocol_and_rejects_out_of_window_observations(
    tmp_path: Path,
) -> None:
    protocol = BookShadowProtocol(candidate_identities=("candidate-v1",), start_ms=100, end_ms=200)
    evaluator = BookShadowEvaluator(protocol=protocol)
    observation = BookShadowObservation(
        event_time_ms=150,
        features=BookFlowFeatures(
            valid=True,
            best_quote_imbalance=Decimal("0.2"),
            depth_imbalance=Decimal("0.2"),
            liquidity_change_confirmed_by_trades=True,
            market_data_quality="valid",
            segment_id="segment-1",
        ),
        baseline_entry=True,
        hypothetical_entry=True,
    )

    evaluator.record(observation)
    key = evaluator.persist_report(LocalObjectStore(tmp_path))
    payload = (tmp_path / key).read_text(encoding="utf-8")

    assert protocol.identity in payload
    identity = key.rsplit("/", 1)[-1].removesuffix(".json")
    assert (tmp_path / f"shadow/book-flow/{identity}.md").is_file()
    assert (tmp_path / f"shadow/book-flow/{identity}.html").is_file()
    with pytest.raises(ValueError, match="protocol window"):
        evaluator.record(replace(observation, event_time_ms=201))
