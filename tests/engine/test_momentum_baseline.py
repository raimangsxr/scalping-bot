from __future__ import annotations

from decimal import Decimal

from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import CandleClosed, FillReceived, MarketTick
from scalping_bot.domain.models import PlaceProtection, SubmitEntry
from scalping_bot.domain.state import InMemoryState
from scalping_bot.research.report import render_scalp_report
from scalping_bot.strategies.momentum import MomentumPolicy


def candle(sequence: int, close: str) -> CandleClosed:
    return CandleClosed(
        event_id=f"candle-{sequence}",
        event_time_ms=sequence * 60_000,
        sequence=sequence,
        open=Decimal(close) - Decimal("0.5"),
        high=Decimal(close) + Decimal("1"),
        low=Decimal(close) - Decimal("1"),
        close=Decimal(close),
        complete=True,
    )


def test_confirmed_momentum_signal_executes_only_on_a_subsequent_event() -> None:
    state = InMemoryState(initial_equity=Decimal("10000"))
    engine = Engine(state, MomentumPolicy(candidate_version="momentum-baseline-v1", window=3))

    assert not engine.handle(candle(1, "100")).commands
    assert not engine.handle(candle(2, "101")).commands
    signal = engine.handle(candle(3, "103"))

    assert not signal.commands
    assert signal.decisions[-1].kind == "entry-proposed"
    rationale = signal.decisions[-1].details["rationale"]
    assert rationale == {
        "condition": "three-bar return >= 1%",
        "observed_return": "0.03",
        "invalidation": "100",
    }

    execution = engine.handle(
        MarketTick(event_id="tick-1", event_time_ms=180_001, price=Decimal("104"))
    )

    assert len(execution.commands) == 1
    command = execution.commands[0]
    assert isinstance(command, SubmitEntry)
    assert command.price_reference == Decimal("104")
    assert command.quantity > 0
    assert command.rationale["condition"] == "three-bar return >= 1%"


def test_identical_event_sequence_has_identical_commands_and_audit() -> None:
    events = [candle(1, "100"), candle(2, "101"), candle(3, "103")]
    events.append(MarketTick(event_id="tick-1", event_time_ms=180_001, price=Decimal("104")))

    def run() -> tuple[object, ...]:
        engine = Engine(
            InMemoryState(initial_equity=Decimal("10000")),
            MomentumPolicy(candidate_version="momentum-baseline-v1", window=3),
        )
        return tuple(engine.handle(event) for event in events)

    assert run() == run()


def test_fill_is_protected_and_report_traces_source_to_position() -> None:
    state = InMemoryState(initial_equity=Decimal("10000"))
    engine = Engine(state, MomentumPolicy(candidate_version="momentum-baseline-v1", window=3))
    for event in (candle(1, "100"), candle(2, "101"), candle(3, "103")):
        engine.handle(event)
    entry = engine.handle(
        MarketTick(event_id="tick-1", event_time_ms=180_001, price=Decimal("104"))
    ).commands[0]
    assert isinstance(entry, SubmitEntry)

    result = engine.handle(
        FillReceived(
            event_id="fill-1",
            event_time_ms=180_002,
            client_order_id=entry.client_order_id,
            price=Decimal("104.10"),
            quantity=entry.quantity,
            commission=Decimal("0.52"),
            commission_asset="USDT",
        )
    )

    assert isinstance(result.commands[0], PlaceProtection)
    report = render_scalp_report(state)
    assert "momentum-baseline-v1" in report
    assert "candle-3" in report
    assert "fill-1" in report
    assert "104.10" in report
