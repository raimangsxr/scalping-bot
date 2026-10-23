from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import (
    CandleClosed,
    ExitFillReceived,
    FillReceived,
    MarketTick,
    OperatorConfirmedNewDay,
)
from scalping_bot.domain.models import ExitMarket, SubmitEntry
from scalping_bot.domain.risk import position_quantity
from scalping_bot.domain.state import InMemoryState
from scalping_bot.strategies.momentum import MomentumPolicy


@given(
    equity=st.decimals(min_value="100", max_value="20000", places=2),
    entry=st.decimals(min_value="10", max_value="100000", places=2),
    distance=st.decimals(min_value="0.01", max_value="1000", places=2),
)
def test_position_risk_and_notional_never_exceed_caps(
    equity: Decimal, entry: Decimal, distance: Decimal
) -> None:
    invalidation = entry - distance
    quantity = position_quantity(
        equity=equity,
        initial_capital=Decimal("10000"),
        entry_price=entry,
        invalidation_price=invalidation,
        step_size=Decimal("0.00001"),
    )
    constrained = min(equity, Decimal("10000"))

    assert quantity * distance <= constrained * Decimal("0.0025")
    assert quantity * entry <= constrained * Decimal("0.50")
    assert quantity % Decimal("0.00001") == 0


def _candle(sequence: int, close: str) -> CandleClosed:
    price = Decimal(close)
    return CandleClosed(
        event_id=f"c-{sequence}",
        event_time_ms=sequence * 60_000,
        sequence=sequence,
        open=price,
        high=price,
        low=price,
        close=price,
        complete=True,
    )


def _engine_with_entry() -> tuple[Engine, SubmitEntry]:
    engine = Engine(
        InMemoryState(initial_equity=Decimal("10000")),
        MomentumPolicy(candidate_version="momentum-v1", window=3),
    )
    for event in (_candle(1, "100"), _candle(2, "101"), _candle(3, "103")):
        engine.handle(event)
    command = engine.handle(
        MarketTick(event_id="entry-tick", event_time_ms=180_001, price=Decimal("104"))
    ).commands[0]
    assert isinstance(command, SubmitEntry)
    return engine, command


def _engine_with_position() -> Engine:
    engine, entry = _engine_with_entry()
    engine.handle(
        FillReceived(
            event_id="entry-fill",
            event_time_ms=180_002,
            client_order_id=entry.client_order_id,
            price=Decimal("104"),
            quantity=entry.quantity,
            commission=Decimal("1"),
            commission_asset="USDT",
        )
    )
    return engine


def test_stale_proposal_never_executes() -> None:
    engine = Engine(
        InMemoryState(initial_equity=Decimal("10000")),
        MomentumPolicy(candidate_version="momentum-v1", window=3),
    )
    for event in (_candle(1, "100"), _candle(2, "101"), _candle(3, "103")):
        engine.handle(event)

    result = engine.handle(MarketTick(event_id="late", event_time_ms=240_001, price=Decimal("104")))

    assert not result.commands
    assert result.decisions[0].kind == "proposal-expired"


def test_stop_has_priority_and_closes_the_complete_position() -> None:
    engine = _engine_with_position()

    result = engine.handle(
        MarketTick(event_id="stop-tick", event_time_ms=181_000, price=Decimal("99"))
    )

    exit_command = result.commands[0]
    assert isinstance(exit_command, ExitMarket)
    assert exit_command.reason == "protective-stop"
    assert exit_command.quantity == engine.state.position.quantity


def test_minute_25_break_even_and_minute_30_forced_exit_are_deterministic() -> None:
    engine = _engine_with_position()
    early = engine.handle(
        MarketTick(event_id="minute-24", event_time_ms=180_002 + 24 * 60_000, price=Decimal("105"))
    )
    assert not early.commands

    break_even = engine.handle(
        MarketTick(event_id="minute-25", event_time_ms=180_002 + 25 * 60_000, price=Decimal("105"))
    )
    assert isinstance(break_even.commands[0], ExitMarket)
    assert break_even.commands[0].reason == "timed-break-even"

    engine = _engine_with_position()
    forced = engine.handle(
        MarketTick(event_id="minute-30", event_time_ms=180_002 + 30 * 60_000, price=Decimal("101"))
    )
    assert isinstance(forced.commands[0], ExitMarket)
    assert forced.commands[0].reason == "timed-force"


def test_daily_loss_flattens_and_requires_next_day_operator_confirmation() -> None:
    engine = _engine_with_position()
    engine.state.current_equity = Decimal("9800")
    result = engine.handle(
        MarketTick(event_id="daily-loss", event_time_ms=200_000, price=Decimal("104"))
    )
    exit_command = result.commands[0]
    assert isinstance(exit_command, ExitMarket)
    assert exit_command.reason == "daily-circuit-breaker"

    engine.handle(
        ExitFillReceived(
            event_id="daily-exit-fill",
            event_time_ms=200_001,
            client_order_id=exit_command.client_order_id,
            price=Decimal("90"),
            quantity=exit_command.quantity,
            commission=Decimal("1"),
        )
    )
    assert engine.state.daily_locked
    engine.handle(
        OperatorConfirmedNewDay(event_id="confirm-day", event_time_ms=86_400_000, utc_day=1)
    )
    assert not engine.state.daily_locked


def test_ten_percent_drawdown_flattens_and_locks_indefinitely() -> None:
    engine = _engine_with_position()
    engine.state.high_water_mark = Decimal("12000")
    engine.state.current_equity = Decimal("10800")

    result = engine.handle(
        MarketTick(event_id="drawdown", event_time_ms=200_000, price=Decimal("104"))
    )

    assert isinstance(result.commands[0], ExitMarket)
    assert result.commands[0].reason == "drawdown-circuit-breaker"
    assert engine.state.drawdown_locked


def test_one_bar_cooldown_blocks_a_new_entry_after_exit() -> None:
    engine = _engine_with_position()
    result = engine.handle(MarketTick(event_id="stop", event_time_ms=181_000, price=Decimal("99")))
    exit_command = result.commands[0]
    assert isinstance(exit_command, ExitMarket)
    engine.handle(
        ExitFillReceived(
            event_id="exit-fill",
            event_time_ms=181_001,
            client_order_id=exit_command.client_order_id,
            price=Decimal("99"),
            quantity=exit_command.quantity,
            commission=Decimal("1"),
        )
    )
    for sequence, close in ((4, "104"), (5, "105"), (6, "107")):
        event = _candle(sequence, close)
        event = CandleClosed(
            event_id=event.event_id,
            event_time_ms=181_001 + sequence,
            sequence=event.sequence,
            open=event.open,
            high=event.high,
            low=event.low,
            close=event.close,
            complete=True,
        )
        engine.handle(event)

    blocked = engine.handle(
        MarketTick(event_id="cooldown", event_time_ms=181_010, price=Decimal("108"))
    )

    assert not blocked.commands
