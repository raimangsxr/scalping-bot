from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scalping_bot.execution.binance import (
    BinanceCapabilities,
    BinanceSpotTestnetAdapter,
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
                        {"filterType": "LOT_SIZE", "stepSize": "0.00001000"},
                        {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
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
        assert state.schema_version == 1
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
        maker_commission="0.001",
        taker_commission="0.001",
        request_weight_limit=6000,
    )


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
