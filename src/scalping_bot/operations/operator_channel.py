"""Durable command queue used by operators while the executor owns its state DB."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

_MIGRATIONS = (
    """
    CREATE TABLE IF NOT EXISTS commands (
        command_id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        event_time_ms INTEGER NOT NULL,
        reason TEXT,
        operator_confirmed INTEGER,
        status TEXT NOT NULL DEFAULT 'PENDING',
        error TEXT
    );
    CREATE TABLE IF NOT EXISTS command_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        command_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        event_time_ms INTEGER NOT NULL,
        details_json TEXT NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS command_events_no_update
    BEFORE UPDATE ON command_events BEGIN
        SELECT RAISE(ABORT, 'command_events are append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS command_events_no_delete
    BEFORE DELETE ON command_events BEGIN
        SELECT RAISE(ABORT, 'command_events are append-only');
    END;
    """,
    "ALTER TABLE commands ADD COLUMN claimed_ms INTEGER;",
)


@dataclass(frozen=True)
class OperatorCommand:
    command_id: int
    kind: str
    event_time_ms: int
    reason: str | None
    operator_confirmed: bool | None


class OperatorCommandStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        applied = {
            int(row[0])
            for row in self.connection.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for version, migration in enumerate(_MIGRATIONS, start=1):
            if version in applied:
                continue
            try:
                self.connection.executescript(
                    f"BEGIN IMMEDIATE;\n{migration}\n"
                    f"INSERT INTO schema_migrations(version) VALUES ({version});\nCOMMIT;"
                )
            except Exception:
                self.connection.execute("ROLLBACK")
                raise
        # A process crash can leave a claimed command between its state change and
        # the terminal acknowledgement.  The command id is the idempotency token;
        # return it to the durable queue for a new owner on restart.
        self.connection.execute(
            "UPDATE commands SET status = 'PENDING', claimed_ms = NULL WHERE status = 'PROCESSING'"
        )

    def enqueue(
        self,
        *,
        kind: str,
        event_time_ms: int,
        reason: str | None = None,
        operator_confirmed: bool | None = None,
    ) -> int:
        if kind not in {"pause", "flatten-and-lock", "reconcile", "unlock"}:
            raise ValueError("unsupported operator command")
        if kind in {"pause", "flatten-and-lock"} and not (reason or "").strip():
            raise ValueError("this operator command requires a reason")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.connection.execute(
                "INSERT INTO commands(kind, event_time_ms, reason, operator_confirmed) "
                "VALUES (?, ?, ?, ?)",
                (kind, event_time_ms, reason, operator_confirmed),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("SQLite did not return an operator command identifier")
            command_id = int(cursor.lastrowid)
            self._append_event(command_id, "ENQUEUED", event_time_ms, {})
            self.connection.execute("COMMIT")
            return command_id
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def pending(self) -> tuple[OperatorCommand, ...]:
        return self._read("PENDING")

    def claim_pending(self) -> tuple[OperatorCommand, ...]:
        """Atomically claim the pending commands for the executor writer loop."""
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            commands = self._read("PENDING")
            if commands:
                self.connection.executemany(
                    "UPDATE commands SET status = 'PROCESSING', claimed_ms = ? "
                    "WHERE command_id = ? AND status = 'PENDING'",
                    ((time.time_ns() // 1_000_000, command.command_id) for command in commands),
                )
                claimed_ms = time.time_ns() // 1_000_000
                for command in commands:
                    self._append_event(command.command_id, "CLAIMED", claimed_ms, {})
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return commands

    def _read(self, status: str) -> tuple[OperatorCommand, ...]:
        return tuple(
            OperatorCommand(
                command_id=int(row["command_id"]),
                kind=str(row["kind"]),
                event_time_ms=int(row["event_time_ms"]),
                reason=(str(row["reason"]) if row["reason"] is not None else None),
                operator_confirmed=(
                    bool(row["operator_confirmed"])
                    if row["operator_confirmed"] is not None
                    else None
                ),
            )
            for row in self.connection.execute(
                "SELECT command_id, kind, event_time_ms, reason, operator_confirmed "
                "FROM commands WHERE status = ? ORDER BY command_id",
                (status,),
            ).fetchall()
        )

    def complete(self, command_id: int, *, error: str | None = None) -> None:
        status = "FAILED" if error else "APPLIED"
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                "SELECT status, error FROM commands WHERE command_id = ?", (command_id,)
            ).fetchone()
            if row is None:
                raise LookupError(f"unknown operator command: {command_id}")
            current_status = str(row["status"])
            if current_status in {"APPLIED", "FAILED"}:
                if current_status == status and row["error"] == error:
                    self.connection.execute("COMMIT")
                    return
                raise ValueError(f"operator command {command_id} is already terminal")
            self.connection.execute(
                "UPDATE commands SET status = ?, error = ?, claimed_ms = NULL "
                "WHERE command_id = ? AND status IN ('PENDING', 'PROCESSING')",
                (status, error, command_id),
            )
            self._append_event(
                command_id,
                status,
                time.time_ns() // 1_000_000,
                {"error": error} if error else {},
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def _append_event(
        self, command_id: int, event_type: str, event_time_ms: int, details: dict[str, object]
    ) -> None:
        self.connection.execute(
            "INSERT INTO command_events(command_id, event_type, event_time_ms, details_json) "
            "VALUES (?, ?, ?, ?)",
            (command_id, event_type, event_time_ms, json.dumps(details, sort_keys=True)),
        )

    def close(self) -> None:
        self.connection.close()
