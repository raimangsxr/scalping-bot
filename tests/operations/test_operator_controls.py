from __future__ import annotations

from pathlib import Path

import pytest

from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.operator import OperatorControls
from scalping_bot.operations.safety import OperationalMode


def test_operator_controls_are_durable_idempotent_and_require_reconciliation_to_unlock(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state.db"
    with SQLiteState(database) as state:
        controls = OperatorControls(state)
        paused = controls.pause(reason="maintenance", event_time_ms=1)
        repeated = controls.pause(reason="maintenance", event_time_ms=2)
        assert paused.mode is OperationalMode.PAUSED
        assert repeated.mode is OperationalMode.PAUSED
        assert state.audit_kinds()[-2:] == ("operator-pause-request", "operator-pause-request")

    with SQLiteState(database) as state:
        assert state.operating_mode is OperationalMode.PAUSED
        controls = OperatorControls(state)
        with pytest.raises(ValueError, match="reconciliation"):
            controls.unlock(event_time_ms=3, operator_confirmed=True)
        state.record_reconciliation(
            reason="operator",
            event_time_ms=4,
            consistent=True,
            divergences=(),
            exchange_snapshot={"balances": []},
        )
        result = controls.unlock(event_time_ms=5, operator_confirmed=True)
        assert result.mode is OperationalMode.RUNNING
        assert state.entries_allowed


def test_flatten_and_lock_is_distinct_from_pause_and_cannot_be_unlocked_implicitly(
    tmp_path: Path,
) -> None:
    with SQLiteState(tmp_path / "state.db") as state:
        controls = OperatorControls(state)
        result = controls.flatten_and_lock(reason="operator emergency", event_time_ms=1)

        assert result.mode is OperationalMode.FLATTEN_AND_LOCK
        assert not state.entries_allowed
        with pytest.raises(ValueError, match="Flatten and Lock"):
            controls.unlock(event_time_ms=2, operator_confirmed=True)
