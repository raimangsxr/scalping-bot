from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.execution.binance import (
    BinanceCapabilities,
    BinanceSpotTestnetAdapter,
    InMemorySpotTransport,
    UnsafeEndpointError,
)
from scalping_bot.execution.reconcile import Reconciler
from scalping_bot.execution.sqlite_state import SQLiteState, WriterAlreadyActive


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def exchange_info(self, symbol: str) -> dict[str, object]:
        self.calls.append(("exchange_info", symbol))
        return {
            "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "limit": 6000}],
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "orderTypes": ["MARKET", "LIMIT", "STOP_LOSS_LIMIT"],
                    "filters": [
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.00001000",
                            "maxQty": "100.00000000",
                        },
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                        {
                            "filterType": "NOTIONAL",
                            "minNotional": "5.00000000",
                            "maxNotional": "1000000.00000000",
                        },
                    ],
                }
            ],
        }

    def account_commission(self, symbol: str) -> dict[str, object]:
        self.calls.append(("account_commission", symbol))
        return {"standardCommission": {"maker": "0.001", "taker": "0.001"}}

    def account(self) -> dict[str, object]:
        return {"balances": [{"asset": "USDT", "free": "10000", "locked": "0"}]}

    def open_orders(self, symbol: str) -> list[dict[str, object]]:
        return []

    def trades(self, symbol: str) -> list[dict[str, object]]:
        return []


def test_sqlite_uses_durable_single_writer_settings_and_versioned_migrations(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    state = SQLiteState(path)
    try:
        assert state.pragma("journal_mode") == "wal"
        assert state.pragma("foreign_keys") == 1
        assert state.pragma("synchronous") == 2
        assert state.schema_version == 5
        with pytest.raises(WriterAlreadyActive):
            SQLiteState(path)
    finally:
        state.close()


def test_audit_rows_are_append_only(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    try:
        event_id = state.append_audit(
            kind="operator-pause", event_time_ms=1, details={"reason": "test"}
        )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            state.connection.execute(
                "UPDATE audit_events SET kind = 'changed' WHERE id = ?", (event_id,)
            )
    finally:
        state.close()


def test_engine_risk_locks_and_equity_survive_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    state = SQLiteState(path)
    engine_state = state.restore_engine_state(initial_equity="10000")
    engine_state.current_equity = Decimal("9750")
    engine_state.high_water_mark = Decimal("11000")
    engine_state.day_open_equity = Decimal("10000")
    engine_state.utc_day = 20_000
    engine_state.daily_locked = True
    engine_state.drawdown_locked = True
    state.persist_engine_state(engine_state, event_time_ms=100)
    state.close()

    reopened = SQLiteState(path)
    try:
        restored = reopened.restore_engine_state(initial_equity="10000")
        assert restored.current_equity == Decimal("9750")
        assert restored.high_water_mark == Decimal("11000")
        assert restored.daily_locked
        assert restored.drawdown_locked
    finally:
        reopened.close()


def test_adapter_refuses_mainnet_and_loads_capabilities_dynamically() -> None:
    with pytest.raises(UnsafeEndpointError, match="Testnet"):
        BinanceSpotTestnetAdapter(endpoint="https://api.binance.com", transport=FakeTransport())

    adapter = BinanceSpotTestnetAdapter(
        endpoint="https://testnet.binance.vision", transport=FakeTransport()
    )
    capabilities = adapter.load_capabilities()

    assert capabilities == BinanceCapabilities(
        symbol_status="TRADING",
        order_types=("MARKET", "LIMIT", "STOP_LOSS_LIMIT"),
        step_size="0.00001000",
        min_notional="5.00000000",
        tick_size="0.01000000",
        max_quantity="100.00000000",
        max_notional="1000000.00000000",
        maker_commission="0.001",
        taker_commission="0.001",
        request_weight_limit=6000,
    )


def test_in_memory_transport_covers_signed_adapter_order_and_protection_contract() -> None:
    transport = InMemorySpotTransport()
    adapter = BinanceSpotTestnetAdapter(
        endpoint="https://testnet.binance.vision", transport=transport
    )

    result = adapter.submit_market_buy(client_order_id="scalp-entry", quantity="0.01")
    assert result.status == "FILLED"
    assert adapter.query_order(client_order_id="scalp-entry") == result
    protection_id = adapter.place_oco_protection(
        client_order_id="scalp-entry-protect",
        quantity="0.01",
        stop_price="89000",
        take_profit_price="91000",
    )
    assert protection_id == "1"
    assert adapter.reconciliation_snapshot()["open_orders"]
    adapter.cancel_protection(client_order_id="scalp-entry-protect")
    exit_result = adapter.flatten_market_sell(client_order_id="scalp-exit", quantity="0.01")
    assert exit_result.status == "FILLED"
    assert len(adapter.reconciliation_snapshot()["trades"]) == 2


def test_reconciliation_queries_unknown_order_and_rebuilds_exposure_after_restart(
    tmp_path: Path,
) -> None:
    transport = InMemorySpotTransport()
    adapter = BinanceSpotTestnetAdapter(
        endpoint="https://testnet.binance.vision", transport=transport
    )
    adapter.submit_market_buy(client_order_id="scalp-restart", quantity="0.01")
    state = SQLiteState(tmp_path / "state.db")
    try:
        state.persist_order_intent(
            client_order_id="scalp-restart",
            side="BUY",
            quantity="0.01",
            status="UNKNOWN",
            created_ms=1,
            candidate_version="momentum-v1",
            source_event_id="candle-1",
            invalidation_price="89000",
        )

        result = Reconciler(state, adapter).run(reason="restart", event_time_ms=2)

        assert result.consistent
        assert state.entries_allowed
        assert state.order_intent("scalp-restart")["status"] == "FILLED"
        assert state.position() is not None
        assert state.position()["quantity"] == "0.01"
    finally:
        state.close()


def test_startup_reconciliation_blocks_entries_until_exchange_and_local_state_agree(
    tmp_path: Path,
) -> None:
    state = SQLiteState(tmp_path / "state.db")
    adapter = BinanceSpotTestnetAdapter(
        endpoint="https://testnet.binance.vision", transport=FakeTransport()
    )
    try:
        reconciler = Reconciler(state, adapter)
        assert not state.entries_allowed

        result = reconciler.run(reason="startup", event_time_ms=100)

        assert result.consistent
        assert state.entries_allowed
        assert state.latest_reconciliation()["reason"] == "startup"
    finally:
        state.close()


def test_unreconciled_local_intent_keeps_entries_blocked(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    adapter = BinanceSpotTestnetAdapter(
        endpoint="https://testnet.binance.vision", transport=FakeTransport()
    )
    try:
        state.persist_order_intent(
            client_order_id="scalp-1",
            side="BUY",
            quantity="0.01",
            status="SUBMITTING",
            created_ms=1,
        )

        result = Reconciler(state, adapter).run(reason="reconnect", event_time_ms=100)

        assert not result.consistent
        assert not state.entries_allowed
        assert "scalp-1" in result.divergences[0]
    finally:
        state.close()
