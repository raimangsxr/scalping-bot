from __future__ import annotations

from pathlib import Path

from cryptography.fernet import Fernet

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.backup import EncryptedSQLiteBackup
from scalping_bot.operations.drills import OperationalDrillRunner


def test_backup_restore_drill_records_rpo_rto_and_immutable_report(tmp_path: Path) -> None:
    database = tmp_path / "state.db"
    with SQLiteState(database) as state:
        state.append_audit(kind="heartbeat", event_time_ms=1, details={})
    backup = EncryptedSQLiteBackup(Fernet.generate_key())
    artifact = backup.create(database, destination=tmp_path / "backups", created_ms=1_000)
    runner = OperationalDrillRunner(LocalObjectStore(tmp_path / "artifacts"))

    outcome = runner.backup_restore(
        backup,
        artifact,
        destination=tmp_path / "restore",
        backup_created_ms=1_000,
        observed_ms=1_000 + 60_000,
        restore_started_ms=2_000,
        restore_completed_ms=2_000 + 60_000,
    )
    key = runner.persist(outcome)

    assert outcome.passed
    assert outcome.measured_rpo_seconds == 60
    assert outcome.measured_rto_seconds == 60
    assert (tmp_path / "artifacts" / key).exists()
    assert "PASS" in (tmp_path / "artifacts" / key.replace(".json", ".md")).read_text()
