from __future__ import annotations

from pathlib import Path

from scalping_bot.execution.events import (
    ExchangeEventApplier,
    parse_user_data_message,
)
from scalping_bot.execution.sqlite_state import SQLiteState


def test_execution_reports_and_account_updates_are_idempotent_and_durable(tmp_path: Path) -> None:
    database = tmp_path / "state.db"
    with SQLiteState(database) as state:
        state.persist_order_intent(
            client_order_id="scalp-1",
            side="BUY",
            quantity="0.01",
            status="SUBMITTING",
            created_ms=1,
            candidate_version="candidate-v1",
            source_event_id="signal-1",
            invalidation_price="89000",
        )
        applier = ExchangeEventApplier(state)
        fill_event = parse_user_data_message(
            {
                "e": "executionReport",
                "E": 2,
                "i": 7,
                "c": "scalp-1",
                "X": "FILLED",
                "t": 11,
                "L": "90000",
                "l": "0.01",
                "n": "0.9",
                "N": "USDT",
            }
        )
        applier.apply(fill_event)
        applier.apply(fill_event)
        applier.apply(
            parse_user_data_message(
                {
                    "e": "outboundAccountPosition",
                    "E": 3,
                    "B": [
                        {"a": "USDT", "f": "9000", "l": "0"},
                        {"a": "BTC", "f": "0.01", "l": "0"},
                    ],
                }
            )
        )

        assert len(state.fills_for_order("scalp-1")) == 1
        assert state.order_intent("scalp-1")["status"] == "FILLED"
        assert state.position() is not None
        assert {item["asset"] for item in state.balances()} == {"BTC", "USDT"}


def test_user_data_stream_loss_blocks_entries_until_reconciliation(tmp_path: Path) -> None:
    with SQLiteState(tmp_path / "state.db") as state:
        applier = ExchangeEventApplier(state)
        applier.stream_lost(event_time_ms=10, reason="websocket closed")

        assert not state.entries_allowed
        assert state.operating_mode.value == "DEGRADED"
        assert "exchange-stream-lost" in state.audit_kinds()


def test_user_data_stream_reconciliation_reopens_entries_only_after_consistency(
    tmp_path: Path,
) -> None:
    with SQLiteState(tmp_path / "state.db") as state:
        applier = ExchangeEventApplier(state)
        applier.stream_lost(event_time_ms=10, reason="websocket closed")

        assert not applier.stream_reconciled(event_time_ms=11, reconcile=lambda: False)
        assert not state.entries_allowed

        assert applier.stream_reconciled(event_time_ms=12, reconcile=lambda: True)
        assert state.entries_allowed
