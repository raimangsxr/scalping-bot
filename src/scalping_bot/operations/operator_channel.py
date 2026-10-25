"""Durable command queue used by operators while the executor owns its state DB."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


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
            """
            CREATE TABLE IF NOT EXISTS commands (
                command_id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                event_time_ms INTEGER NOT NULL,
                reason TEXT,
                operator_confirmed INTEGER,
                status TEXT NOT NULL DEFAULT 'PENDING',
                error TEXT
            )
            """
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
        cursor = self.connection.execute(
            "INSERT INTO commands(kind, event_time_ms, reason, operator_confirmed) "
            "VALUES (?, ?, ?, ?)",
            (kind, event_time_ms, reason, operator_confirmed),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("SQLite did not return an operator command identifier")
        return int(cursor.lastrowid)

    def pending(self) -> tuple[OperatorCommand, ...]:
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
                "FROM commands WHERE status = 'PENDING' ORDER BY command_id"
            ).fetchall()
        )

    def complete(self, command_id: int, *, error: str | None = None) -> None:
        self.connection.execute(
            "UPDATE commands SET status = ?, error = ? WHERE command_id = ?",
            ("FAILED" if error else "APPLIED", error, command_id),
        )

    def close(self) -> None:
        self.connection.close()
