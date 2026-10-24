from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scalping_bot.execution.sqlite_state import SQLiteState


class DeploymentBlocked(RuntimeError):
    pass


class DeploymentGate:
    def __init__(self, state: SQLiteState) -> None:
        self.state = state

    def assert_safe(self, *, backup: Path, target_schema: int) -> None:
        _assert_backup_and_schema(backup, target_schema, self.state.schema_version)
        if self.state.active_intents():
            raise DeploymentBlocked(
                "planned deployment is blocked while orders or exposure are open"
            )
        try:
            reconciliation = self.state.latest_reconciliation()
        except LookupError as error:
            raise DeploymentBlocked(
                "planned deployment requires a successful reconciliation"
            ) from error
        if not reconciliation["consistent"]:
            raise DeploymentBlocked("planned deployment requires a consistent reconciliation")

    @staticmethod
    def assert_path_safe(database: Path, *, backup: Path, target_schema: int) -> None:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            version = int(
                connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0]
            )
            _assert_backup_and_schema(backup, target_schema, version)
            active = connection.execute(
                "SELECT client_order_id FROM order_intents "
                "WHERE status NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'FLATTENED_LOCKED') LIMIT 1"
            ).fetchone()
            if active is not None:
                raise DeploymentBlocked(
                    "planned deployment is blocked while orders or exposure are open"
                )
            reconciliation = connection.execute(
                "SELECT consistent, divergences_json FROM reconciliations ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if reconciliation is None or not reconciliation["consistent"]:
                raise DeploymentBlocked("planned deployment requires a successful reconciliation")
            if json.loads(reconciliation["divergences_json"]):
                raise DeploymentBlocked(
                    "planned deployment requires zero reconciliation divergences"
                )
        finally:
            connection.close()


def _assert_backup_and_schema(backup: Path, target_schema: int, current_schema: int) -> None:
    if not backup.is_file() or backup.stat().st_size == 0:
        raise DeploymentBlocked("a verified encrypted backup is required before deployment")
    if target_schema != current_schema:
        raise DeploymentBlocked(
            f"target schema {target_schema} is incompatible with current schema {current_schema}"
        )
