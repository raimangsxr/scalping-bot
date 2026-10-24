from __future__ import annotations

from decimal import Decimal

import pytest

from scalping_bot.domain.events import CandleClosed
from scalping_bot.research.candidates import CandidateBatch, first_batch
from scalping_bot.research.features import (
    BookFlowInput,
    TradeFlowFeatureBuilder,
    TradePrint,
    book_flow_features,
)
from scalping_bot.research.screening import AttemptLedger
from scalping_bot.strategies.book_shadow import (
    BookShadowEvaluator,
    BookShadowObservation,
    ShadowBookConfirmation,
)
from scalping_bot.strategies.breakout import BreakoutPolicy
from scalping_bot.strategies.mean_reversion import RegimeVwapPolicy
from scalping_bot.strategies.trade_flow import TradeFlowConfirmation


def _candle(
    sequence: int, close: str, *, high: str | None = None, low: str | None = None
) -> CandleClosed:
    price = Decimal(close)
    return CandleClosed(
        event_id=f"c-{sequence}",
        event_time_ms=sequence * 60_000,
        sequence=sequence,
        open=price,
        high=Decimal(high or close),
        low=Decimal(low or close),
        close=price,
        complete=True,
    )


def test_first_batch_is_preregistered_and_capped_at_two_per_family() -> None:
    batch = first_batch()

    assert len(batch.candidates) == 8
    assert {candidate.family for candidate in batch.candidates} == {
        "momentum",
        "volatility-breakout",
        "regime-conditioned-mean-reversion",
        "executed-trade-pressure",
    }
    for family in {candidate.family for candidate in batch.candidates}:
        assert len([candidate for candidate in batch.candidates if candidate.family == family]) == 2
    assert all(candidate.source_uri.startswith("https://") for candidate in batch.candidates)
    assert all(candidate.rejection_criteria for candidate in batch.candidates)

    with pytest.raises(ValueError, match="eight"):
        CandidateBatch((*batch.candidates, batch.candidates[0]))


def test_breakout_uses_only_previous_completed_observations() -> None:
    policy = BreakoutPolicy(candidate_version="breakout-v1", lookback=3, buffer=Decimal("0"))
    assert policy.observe(_candle(1, "100", high="101", low="99")) is None
    assert policy.observe(_candle(2, "101", high="102", low="100")) is None
    assert policy.observe(_candle(3, "101", high="102", low="100")) is None

    proposal = policy.observe(_candle(4, "103", high="104", low="102"))

    assert proposal is not None
    assert proposal.source_event_id == "c-4"
    assert proposal.feature_snapshot["prior_range_high"] == "102"
    assert proposal.event_time_ms == 240_000


def test_mean_reversion_requires_objectively_lateral_regime_and_causal_vwap() -> None:
    policy = RegimeVwapPolicy(
        candidate_version="mean-reversion-v1",
        window=4,
        max_efficiency_ratio=Decimal("0.40"),
        deviation=Decimal("0.01"),
    )
    for item in (
        _candle(1, "100"),
        _candle(2, "101"),
        _candle(3, "99"),
        _candle(4, "100"),
    ):
        assert policy.observe(item, volume=Decimal("1")) is None

    proposal = policy.observe(_candle(5, "98"), volume=Decimal("1"))

    assert proposal is not None
    assert proposal.feature_snapshot["regime"] == "lateral"
    assert Decimal(proposal.feature_snapshot["price_to_vwap"]) < Decimal("-0.01")


def test_trade_flow_features_are_chronological_deterministic_and_fail_on_gaps() -> None:
    prints = (
        TradePrint(1, 1_000, Decimal("100"), Decimal("2"), buyer_maker=False),
        TradePrint(2, 1_100, Decimal("100.1"), Decimal("1"), buyer_maker=True),
    )
    builder = TradeFlowFeatureBuilder(window_ms=1_000)

    features = builder.build(prints, as_of_ms=1_200)

    assert features.valid
    assert features.executed_buy_volume == Decimal("2")
    assert features.executed_sell_volume == Decimal("1")
    assert features.cumulative_delta == Decimal("1")
    assert features.trade_intensity == Decimal("2")
    assert builder.build(prints, as_of_ms=1_200) == features

    invalid = builder.build(
        (prints[0], TradePrint(3, 1_100, Decimal("100"), Decimal("1"), True)), as_of_ms=1_200
    )
    assert not invalid.valid
    out_of_order = builder.build(
        (prints[0], TradePrint(3, 900, Decimal("100"), Decimal("1"), True)), as_of_ms=1_200
    )
    assert not out_of_order.valid


def test_trade_flow_can_confirm_but_missing_data_blocks_entry() -> None:
    confirmation = TradeFlowConfirmation(min_delta=Decimal("0.5"), min_intensity=Decimal("1"))
    features = TradeFlowFeatureBuilder(window_ms=1_000).build(
        (TradePrint(1, 1_000, Decimal("100"), Decimal("2"), False),), as_of_ms=1_100
    )

    assert confirmation.allows(features)
    assert not confirmation.allows(features.__class__(valid=False))


def test_book_flow_is_shadow_only_and_never_authorizes_an_entry() -> None:
    features = book_flow_features(
        BookFlowInput(
            valid_segment=True,
            best_bid_quantity=Decimal("4"),
            best_ask_quantity=Decimal("1"),
            bid_depth=Decimal("10"),
            ask_depth=Decimal("5"),
            previous_bid_depth=Decimal("8"),
            previous_ask_depth=Decimal("7"),
            executed_buy_volume=Decimal("2"),
            executed_sell_volume=Decimal("1"),
            segment_id="segment-1",
        )
    )
    decision = ShadowBookConfirmation(min_imbalance=Decimal("0.2")).evaluate(features)

    assert decision.hypothetical_allow
    assert not decision.executable
    assert features.liquidity_change_confirmed_by_trades

    visible_only = book_flow_features(
        BookFlowInput(
            valid_segment=True,
            segment_id="segment-1",
            best_bid_quantity=Decimal("4"),
            best_ask_quantity=Decimal("1"),
            bid_depth=Decimal("10"),
            ask_depth=Decimal("5"),
            previous_bid_depth=Decimal("8"),
            previous_ask_depth=Decimal("7"),
        )
    )
    assert not ShadowBookConfirmation(Decimal("0.2")).evaluate(visible_only).hypothetical_allow

    invalid = book_flow_features(BookFlowInput.invalid())
    assert not ShadowBookConfirmation(Decimal("0")).evaluate(invalid).hypothetical_allow


def test_book_shadow_report_exposes_coverage_gaps_and_cannot_promote() -> None:
    features = book_flow_features(
        BookFlowInput(
            valid_segment=True,
            segment_id="segment-1",
            best_bid_quantity=Decimal("4"),
            best_ask_quantity=Decimal("1"),
            bid_depth=Decimal("10"),
            ask_depth=Decimal("5"),
            previous_bid_depth=Decimal("8"),
            previous_ask_depth=Decimal("7"),
            executed_buy_volume=Decimal("2"),
            executed_sell_volume=Decimal("1"),
        )
    )
    evaluator = BookShadowEvaluator()
    evaluator.record(BookShadowObservation(1_000, features, True, True))
    evaluator.record_gap()
    evaluator.record(
        BookShadowObservation(62_000, book_flow_features(BookFlowInput.invalid()), True, True)
    )

    report = evaluator.report()

    assert report.coverage == Decimal("0.5")
    assert report.gaps == 2
    assert report.incremental_shadow_effect == Decimal("0")
    assert not report.promoted
    assert "Promotion: prohibited" in report.markdown
    assert "Incremental shadow effect" in report.markdown


def test_attempt_ledger_retains_every_parameter_variant_and_baseline_comparison() -> None:
    ledger = AttemptLedger()
    ledger.record(
        "momentum-001-v1", {"window": 3}, net_expectancy=Decimal("0.2"), baseline=Decimal("0.1")
    )
    ledger.record(
        "momentum-001-v1", {"window": 5}, net_expectancy=Decimal("-0.1"), baseline=Decimal("0.1")
    )

    assert len(ledger.attempts) == 2
    assert ledger.attempts[0].incremental_expectancy == Decimal("0.1")
    assert ledger.attempts[1].rejected
