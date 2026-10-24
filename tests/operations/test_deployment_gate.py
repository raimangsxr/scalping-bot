from pathlib import Path

import pytest

from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.deployment import DeploymentBlocked, DeploymentGate


def test_planned_deployment_requires_flat_reconciled_state_and_backup(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    backup = tmp_path / "backup.fernet"
    backup.write_bytes(b"encrypted")
    try:
        state.set_entries_allowed(True)
        state.record_reconciliation(
            reason="pre-deploy",
            event_time_ms=1,
            consistent=True,
            divergences=(),
            exchange_snapshot={"open_orders": [], "trades": [], "account": {}},
        )
        DeploymentGate(state).assert_safe(backup=backup, target_schema=state.schema_version)

        state.persist_order_intent(
            client_order_id="open-1",
            side="BUY",
            quantity="0.01",
            status="PROTECTED",
            created_ms=1,
        )
        with pytest.raises(DeploymentBlocked, match="open"):
            DeploymentGate(state).assert_safe(backup=backup, target_schema=state.schema_version)
    finally:
        state.close()


def test_rollback_rejects_missing_backup_or_incompatible_schema(tmp_path: Path) -> None:
    state = SQLiteState(tmp_path / "state.db")
    try:
        gate = DeploymentGate(state)
        with pytest.raises(DeploymentBlocked, match="backup"):
            gate.assert_safe(backup=tmp_path / "missing", target_schema=state.schema_version)
        backup = tmp_path / "backup.fernet"
        backup.write_bytes(b"encrypted")
        with pytest.raises(DeploymentBlocked, match="schema"):
            gate.assert_safe(backup=backup, target_schema=state.schema_version - 1)
    finally:
        state.close()
