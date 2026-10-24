from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.domain.models import ExitMarket, PlaceProtection, SubmitEntry
from scalping_bot.execution.executor import (
    ExecutionFill,
    ExecutionLimits,
    ExecutionService,
    OrderResult,
)
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.safety import OperationalMode

LIMITS = ExecutionLimits(step_size=Decimal("0.00001"), min_notional=Decimal("5"))


def _entry() -> SubmitEntry:
    return SubmitEntry(
        client_order_id="scalp-deterministic-1",
        candidate_version="momentum-v1",
        source_event_id="candle-1",
        quantity=Decimal("0.01000"),
        price_reference=Decimal("90000"),
        invalidation_price=Decimal("89500"),
        rationale={"condition": "test"},
    )


def _protection(quantity: str = "0.01000") -> PlaceProtection:
    return PlaceProtection(
        client_order_id="scalp-deterministic-1-protect",
        parent_client_order_id="scalp-deterministic-1",
        quantity=Decimal(quantity),
        stop_price=Decimal("89500"),
        take_profit_price=Decimal("91030"),
    )


class FakeExecutionExchange:
    def __init__(self, state: SQLiteState) -> None:
        self.state = state
        self.actions: list[str] = []
        self.timeout_after_submit = False
        self.protection_fails = False
        self.query_result = OrderResult(
            status="FILLED",
            exchange_order_id="42",
            fills=(
                ExecutionFill(
                    "fill-1", Decimal("90010"), Decimal("0.01000"), Decimal("0.90"), "USDT"
                ),
            ),
        )

    def submit_market_buy(self, *, client_order_id: str, quantity: str) -> OrderResult:
        assert self.state.order_intent(client_order_id)["status"] == "SUBMITTING"
        self.actions.append("submit")
        if self.timeout_after_submit:
            raise TimeoutError("response lost")
        return self.query_result

    def query_order(self, *, client_order_id: str) -> OrderResult:
        self.actions.append("query")
        return self.query_result

    def place_oco_protection(
        self,
        *,
        client_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
    ) -> str:
        self.actions.append(f"protect:{quantity}")
        if self.protection_fails:
            raise RuntimeError("protection rejected")
        return "protection-1"

    def flatten_market_sell(self, *, client_order_id: str, quantity: str) -> OrderResult:
        self.actions.append(f"flatten:{quantity}")
        return OrderResult(
            status="FILLED",
            exchange_order_id="44",
            fills=(
                ExecutionFill(
                    "flatten-fill", Decimal("89900"), Decimal(quantity), Decimal("0.90"), "USDT"
                ),
            ),
        )

    def cancel_protection(self, *, client_order_id: str) -> None:
        self.actions.append(f"cancel:{client_order_id}")


def test_intent_is_durable_before_submission_and_complete_scalp_is_auditable(
    tmp_path: Path,
) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    try:
        service = ExecutionService(state, exchange, LIMITS)
        result = service.submit_entry(_entry(), event_time_ms=100)
        service.place_protection(_protection(), event_time_ms=101)

        assert result.status == "FILLED"
        assert exchange.actions == ["submit", "protect:0.01000"]
        assert state.order_intent("scalp-deterministic-1")["status"] == "PROTECTED"
        trace = state.reconstruct_order("scalp-deterministic-1")
        assert trace["intent"]["candidate_version"] == "momentum-v1"
        assert trace["intent"]["source_event_id"] == "candle-1"
        assert trace["fills"][0]["fill_id"] == "fill-1"
        assert trace["fills"][0]["commission"] == "0.90"
        assert trace["protection"]["exchange_order_id"] == "protection-1"

        service.submit_entry(_entry(), event_time_ms=102)
        assert exchange.actions == ["submit", "protect:0.01000", "query"]
        assert state.position()["protected"] == 1
    finally:
        state.close()


def test_timeout_becomes_unknown_and_is_queried_before_any_retry(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    exchange.timeout_after_submit = True
    try:
        service = ExecutionService(state, exchange, LIMITS)
        result = service.submit_entry(_entry(), event_time_ms=100)
        service.place_protection(_protection(), event_time_ms=101)

        assert result.status == "FILLED"
        assert exchange.actions == ["submit", "query", "protect:0.01000"]
        audit = state.audit_kinds()
        assert "unknown-order-outcome" in audit
        assert audit.index("unknown-order-outcome") < audit.index("order-reconciled")
    finally:
        state.close()


def test_partial_fill_is_accounted_and_only_executed_quantity_is_protected(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    exchange.query_result = OrderResult(
        status="PARTIALLY_FILLED",
        exchange_order_id="42",
        fills=(
            ExecutionFill(
                "fill-partial", Decimal("90010"), Decimal("0.00400"), Decimal("0.36"), "USDT"
            ),
        ),
    )
    try:
        service = ExecutionService(state, exchange, LIMITS)
        service.submit_entry(_entry(), event_time_ms=100)
        service.place_protection(_protection("0.00400"), event_time_ms=101)

        assert exchange.actions == ["submit", "protect:0.00400"]
        assert state.order_intent("scalp-deterministic-1")["executed_quantity"] == "0.00400"
    finally:
        state.close()


def test_partial_entry_remains_reconcilable_after_its_partial_protection(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    exchange.query_result = OrderResult(
        status="PARTIALLY_FILLED",
        exchange_order_id="42",
        fills=(
            ExecutionFill(
                "fill-partial", Decimal("90010"), Decimal("0.00400"), Decimal("0.36"), "USDT"
            ),
        ),
    )
    try:
        service = ExecutionService(state, exchange, LIMITS)
        service.submit_entry(_entry(), event_time_ms=100)
        service.place_protection(_protection("0.00400"), event_time_ms=101)
        assert state.order_intent("scalp-deterministic-1")["status"] == "PARTIALLY_FILLED"

        exchange.query_result = OrderResult(
            status="FILLED",
            exchange_order_id="42",
            fills=(
                ExecutionFill(
                    "fill-partial",
                    Decimal("90010"),
                    Decimal("0.00400"),
                    Decimal("0.36"),
                    "USDT",
                ),
                ExecutionFill(
                    "fill-final", Decimal("90020"), Decimal("0.00600"), Decimal("0.54"), "USDT"
                ),
            ),
        )
        updates = service.refresh_partial_entries(event_time_ms=102)

        assert updates
        assert state.order_intent("scalp-deterministic-1")["status"] == "FILLED"
        assert state.position()["quantity"] == "0.01000"
    finally:
        state.close()


def test_protection_failure_flattens_actual_fill_and_locks_entries(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    exchange.protection_fails = True
    try:
        service = ExecutionService(state, exchange, LIMITS)
        service.submit_entry(_entry(), event_time_ms=100)
        service.place_protection(_protection(), event_time_ms=101)

        assert exchange.actions == ["submit", "protect:0.01000", "flatten:0.01000"]
        assert state.operating_mode is OperationalMode.FLATTEN_AND_LOCK
        assert not state.entries_allowed
        assert "protection-failed" in state.audit_kinds()
    finally:
        state.close()


def test_dynamic_filters_normalize_quantity_and_reject_sub_notional_entries(
    tmp_path: Path,
) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    try:
        command = _entry()
        normalized = replace(command, quantity=Decimal("0.010009"))
        ExecutionService(state, exchange, LIMITS).submit_entry(normalized, event_time_ms=100)
        assert state.order_intent(command.client_order_id)["quantity"] == "0.01000"

        too_small = replace(
            command,
            client_order_id="scalp-too-small",
            quantity=Decimal("0.00001"),
            price_reference=Decimal("100"),
        )
        with pytest.raises(ValueError, match="filters"):
            ExecutionService(state, exchange, LIMITS).submit_entry(too_small, event_time_ms=101)
    finally:
        state.close()


def test_reconstruct_scalp_includes_exit_fills_commissions_and_pnl(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    try:
        service = ExecutionService(state, exchange, LIMITS)
        service.submit_entry(_entry(), event_time_ms=100)
        service.place_protection(_protection(), event_time_ms=101)
        service.submit_exit(
            ExitMarket(
                client_order_id="scalp-exit-1",
                quantity=Decimal("0.01000"),
                reason="protective-stop",
            ),
            event_time_ms=102,
        )

        trace = state.reconstruct_scalp("scalp-deterministic-1")

        assert trace["exit_orders"]
        assert Decimal(trace["pnl"]["net_pnl"]) == Decimal("-2.90")
        assert any(item["kind"] == "position-closed" for item in trace["audit"])
    finally:
        state.close()
