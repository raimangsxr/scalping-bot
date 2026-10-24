from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from scalping_bot.domain.models import SubmitEntry
from scalping_bot.execution.executor import ExecutionService, OrderResult
from scalping_bot.execution.sqlite_state import SQLiteState


def _entry() -> SubmitEntry:
    return SubmitEntry(
        client_order_id="scalp-deterministic-1",
        candidate_version="momentum-v1",
        quantity=Decimal("0.01000"),
        price_reference=Decimal("90000"),
        invalidation_price=Decimal("89500"),
        rationale={"condition": "test"},
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
            fills=(("fill-1", "90010", "0.01000", "0.90", "USDT"),),
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
            fills=(("flatten-fill", "89900", quantity, "0.90", "USDT"),),
        )


def test_intent_is_durable_before_submission_and_complete_scalp_is_auditable(
    tmp_path: Path,
) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    try:
        result = ExecutionService(state, exchange).submit_entry(_entry(), event_time_ms=100)

        assert result.status == "FILLED"
        assert exchange.actions == ["submit", "protect:0.01000"]
        assert state.order_intent("scalp-deterministic-1")["status"] == "PROTECTED"
        trace = state.reconstruct_order("scalp-deterministic-1")
        assert trace["intent"]["candidate_version"] == "momentum-v1"
        assert trace["fills"][0]["fill_id"] == "fill-1"
        assert trace["fills"][0]["commission"] == "0.90"
        assert trace["protection"]["exchange_order_id"] == "protection-1"
    finally:
        state.close()


def test_timeout_becomes_unknown_and_is_queried_before_any_retry(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    exchange.timeout_after_submit = True
    try:
        result = ExecutionService(state, exchange).submit_entry(_entry(), event_time_ms=100)

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
        fills=(("fill-partial", "90010", "0.00400", "0.36", "USDT"),),
    )
    try:
        ExecutionService(state, exchange).submit_entry(_entry(), event_time_ms=100)

        assert exchange.actions == ["submit", "protect:0.00400"]
        assert state.order_intent("scalp-deterministic-1")["executed_quantity"] == "0.00400"
    finally:
        state.close()


def test_protection_failure_flattens_actual_fill_and_locks_entries(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    state.set_entries_allowed(True)
    exchange = FakeExecutionExchange(state)
    exchange.protection_fails = True
    try:
        ExecutionService(state, exchange).submit_entry(_entry(), event_time_ms=100)

        assert exchange.actions == ["submit", "protect:0.01000", "flatten:0.01000"]
        assert state.operating_mode == "FLATTEN_AND_LOCK"
        assert not state.entries_allowed
        assert "protection-failed" in state.audit_kinds()
    finally:
        state.close()
