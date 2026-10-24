from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import CandleClosed, MarketTick
from scalping_bot.domain.models import ExecutionCosts
from scalping_bot.domain.state import InMemoryState
from scalping_bot.execution.executor import (
    ExecutionFill,
    ExecutionLimits,
    ExecutionService,
    OrderResult,
)
from scalping_bot.execution.live import LiveEngineRunner
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.strategies.momentum import MomentumPolicy


class _Exchange:
    def __init__(self) -> None:
        self.actions: list[str] = []
        self.orders: dict[str, OrderResult] = {}

    def submit_market_buy(self, *, client_order_id: str, quantity: str) -> OrderResult:
        self.actions.append("buy")
        result = OrderResult(
            "FILLED",
            "entry-1",
            (
                ExecutionFill(
                    "entry-fill", Decimal("103"), Decimal(quantity), Decimal("0.1"), "USDT"
                ),
            ),
        )
        self.orders[client_order_id] = result
        return result

    def query_order(self, *, client_order_id: str) -> OrderResult:
        return self.orders[client_order_id]

    def place_oco_protection(
        self,
        *,
        client_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
    ) -> str:
        self.actions.append("protect")
        return "oco-1"

    def cancel_protection(self, *, client_order_id: str) -> None:
        self.actions.append("cancel-protection")

    def flatten_market_sell(self, *, client_order_id: str, quantity: str) -> OrderResult:
        self.actions.append("sell")
        result = OrderResult(
            "FILLED",
            "exit-1",
            (ExecutionFill("exit-fill", Decimal("99"), Decimal(quantity), Decimal("0.1"), "USDT"),),
        )
        self.orders[client_order_id] = result
        return result


def _candle(identity: str, time_ms: int, close: str) -> CandleClosed:
    price = Decimal(close)
    return CandleClosed(
        event_id=identity,
        event_time_ms=time_ms,
        sequence=time_ms,
        open=price,
        high=price,
        low=price,
        close=price,
        complete=True,
    )


def test_live_runner_drives_shared_engine_through_a_complete_protected_scalp(
    tmp_path: Path,
) -> None:
    durable = SQLiteState(tmp_path / "state.db")
    durable.set_entries_allowed(True)
    exchange = _Exchange()
    engine_state = InMemoryState(initial_equity=Decimal("10000"))
    runner = LiveEngineRunner(
        engine=Engine(
            engine_state,
            MomentumPolicy(candidate_version="candidate-v1", window=2, threshold=Decimal("0.01")),
            execution_costs=ExecutionCosts(commission_rate=Decimal("0.001")),
        ),
        execution=ExecutionService(
            durable,
            exchange,
            ExecutionLimits(step_size=Decimal("0.00001"), min_notional=Decimal("5")),
        ),
        durable_state=durable,
    )
    try:
        runner.process(_candle("candle-1", 1_000, "100"))
        runner.process(_candle("candle-2", 61_000, "102"))
        runner.process(
            MarketTick(event_id="trade-entry", event_time_ms=61_001, price=Decimal("103"))
        )
        runner.process(MarketTick(event_id="trade-stop", event_time_ms=61_002, price=Decimal("99")))

        assert exchange.actions == ["buy", "protect", "cancel-protection", "sell"]
        assert durable.position() is None
        assert engine_state.position is None
        assert {"entry-filled", "position-closed"}.issubset(durable.audit_kinds())
    finally:
        durable.close()
