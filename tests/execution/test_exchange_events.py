from __future__ import annotations

import json
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


def test_streamed_sell_fill_closes_the_durable_position(tmp_path: Path) -> None:
    with SQLiteState(tmp_path / "state.db") as state:
        state.persist_order_intent(
            client_order_id="entry-1",
            side="BUY",
            quantity="0.01",
            status="FILLED",
            created_ms=1,
            candidate_version="candidate-v1",
            source_event_id="signal-1",
            invalidation_price="89000",
        )
        state.record_fill(
            fill_id="entry-fill",
            client_order_id="entry-1",
            price="90000",
            quantity="0.01",
            commission="0.9",
            commission_asset="USDT",
            event_time_ms=2,
        )
        state.rebuild_position_from_order("entry-1")
        state.persist_order_intent(
            client_order_id="exit-1",
            side="SELL",
            quantity="0.01",
            status="SUBMITTING",
            created_ms=3,
            candidate_version="candidate-v1",
            source_event_id="signal-1",
            invalidation_price="89000",
        )
        applier = ExchangeEventApplier(state)
        applier.apply(
            parse_user_data_message(
                {
                    "e": "executionReport",
                    "E": 4,
                    "i": 8,
                    "c": "exit-1",
                    "X": "FILLED",
                    "t": 12,
                    "L": "90100",
                    "l": "0.01",
                    "n": "0.91",
                    "N": "USDT",
                }
            )
        )

        assert state.position() is None
        assert state.order_intent("entry-1")["status"] == "CLOSED"


def test_oco_list_status_is_durable_and_duplicate_events_are_idempotent(tmp_path: Path) -> None:
    with SQLiteState(tmp_path / "state.db") as state:
        state.persist_order_intent(
            client_order_id="entry-1",
            side="BUY",
            quantity="0.01",
            status="FILLED",
            created_ms=1,
            candidate_version="candidate-v1",
            source_event_id="signal-1",
            invalidation_price="89000",
        )
        state.record_protection(
            client_order_id="entry-1-protect",
            entry_client_order_id="entry-1",
            exchange_order_id="77",
            quantity="0.01",
            stop_price="89000",
            take_profit_price="91000",
            created_ms=2,
        )
        event = parse_user_data_message(
            {
                "e": "listStatus",
                "E": 3,
                "g": 77,
                "l": "ALL_DONE",
                "C": "entry-1-protect",
                "O": [{"i": 101, "c": "entry-1-protect", "s": "SELL", "X": "FILLED"}],
            }
        )
        applier = ExchangeEventApplier(state)
        applier.apply(event)
        applier.apply(event)

        assert state.protection("entry-1") is None
        history = state.protection_history("entry-1")
        assert history[0]["status"] == "TRIGGERED"
        lifecycle = state.protection_lifecycle("entry-1")
        assert len(lifecycle) == 1
        assert json.loads(str(lifecycle[0]["details_json"]))["order_reports"][0]["i"] == "101"
