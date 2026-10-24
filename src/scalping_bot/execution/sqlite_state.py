from __future__ import annotations

import fcntl
import json
import sqlite3
from pathlib import Path
from typing import Any


class WriterAlreadyActive(RuntimeError):
    pass


MIGRATIONS = (
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS runtime_state (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        entries_allowed INTEGER NOT NULL DEFAULT 0 CHECK (entries_allowed IN (0, 1))
    );
    INSERT OR IGNORE INTO runtime_state(singleton, entries_allowed) VALUES (1, 0);
    CREATE TABLE IF NOT EXISTS order_intents (
        client_order_id TEXT PRIMARY KEY,
        side TEXT NOT NULL,
        quantity TEXT NOT NULL,
        status TEXT NOT NULL,
        created_ms INTEGER NOT NULL,
        exchange_order_id TEXT
    );
    CREATE TABLE IF NOT EXISTS fills (
        fill_id TEXT PRIMARY KEY,
        client_order_id TEXT NOT NULL REFERENCES order_intents(client_order_id),
        price TEXT NOT NULL,
        quantity TEXT NOT NULL,
        commission TEXT NOT NULL,
        commission_asset TEXT NOT NULL,
        event_time_ms INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS audit_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        event_time_ms INTEGER NOT NULL,
        details_json TEXT NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS audit_events_no_update
    BEFORE UPDATE ON audit_events BEGIN SELECT RAISE(ABORT, 'audit_events are append-only'); END;
    CREATE TRIGGER IF NOT EXISTS audit_events_no_delete
    BEFORE DELETE ON audit_events BEGIN SELECT RAISE(ABORT, 'audit_events are append-only'); END;
    CREATE TABLE IF NOT EXISTS reconciliations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reason TEXT NOT NULL,
        event_time_ms INTEGER NOT NULL,
        consistent INTEGER NOT NULL CHECK (consistent IN (0, 1)),
        divergences_json TEXT NOT NULL,
        exchange_snapshot_json TEXT NOT NULL
    );
    """,
    """
    ALTER TABLE runtime_state ADD COLUMN operating_mode TEXT NOT NULL DEFAULT 'RUNNING';
    ALTER TABLE order_intents ADD COLUMN candidate_version TEXT;
    ALTER TABLE order_intents ADD COLUMN invalidation_price TEXT;
    ALTER TABLE order_intents ADD COLUMN executed_quantity TEXT NOT NULL DEFAULT '0';
    CREATE TABLE protections (
        client_order_id TEXT PRIMARY KEY,
        entry_client_order_id TEXT NOT NULL REFERENCES order_intents(client_order_id),
        exchange_order_id TEXT NOT NULL,
        quantity TEXT NOT NULL,
        stop_price TEXT NOT NULL,
        take_profit_price TEXT NOT NULL,
        created_ms INTEGER NOT NULL
    );
    """,
)


class SQLiteState:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock_file = path.with_suffix(path.suffix + ".writer.lock").open("a+b")
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self._lock_file.close()
            raise WriterAlreadyActive(f"another executor owns {path}") from error
        self.connection = sqlite3.connect(path, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self._migrate()

    def _migrate(self) -> None:
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        applied = {
            int(row[0])
            for row in self.connection.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for version, migration in enumerate(MIGRATIONS, start=1):
            if version in applied:
                continue
            self.connection.executescript("BEGIN IMMEDIATE;\n" + migration)
            self.connection.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))
            self.connection.execute("COMMIT")

    @property
    def schema_version(self) -> int:
        row = self.connection.execute("SELECT max(version) FROM schema_migrations").fetchone()
        return int(row[0])

    @property
    def entries_allowed(self) -> bool:
        row = self.connection.execute(
            "SELECT entries_allowed FROM runtime_state WHERE singleton = 1"
        ).fetchone()
        return bool(row[0])

    def set_entries_allowed(self, allowed: bool) -> None:
        self.connection.execute(
            "UPDATE runtime_state SET entries_allowed = ? WHERE singleton = 1",
            (int(allowed),),
        )

    @property
    def operating_mode(self) -> str:
        row = self.connection.execute(
            "SELECT operating_mode FROM runtime_state WHERE singleton = 1"
        ).fetchone()
        return str(row[0])

    def set_operating_mode(self, mode: str) -> None:
        self.connection.execute(
            "UPDATE runtime_state SET operating_mode = ?, entries_allowed = 0 WHERE singleton = 1",
            (mode,),
        )

    def pragma(self, name: str) -> str | int:
        if name not in {"journal_mode", "foreign_keys", "synchronous", "busy_timeout"}:
            raise ValueError("pragma is not allowlisted")
        value: str | int = self.connection.execute(f"PRAGMA {name}").fetchone()[0]
        return value

    def append_audit(self, *, kind: str, event_time_ms: int, details: dict[str, object]) -> int:
        cursor = self.connection.execute(
            "INSERT INTO audit_events(kind, event_time_ms, details_json) VALUES (?, ?, ?)",
            (kind, event_time_ms, json.dumps(details, sort_keys=True, separators=(",", ":"))),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("SQLite did not return an audit event identifier")
        return cursor.lastrowid

    def persist_order_intent(
        self,
        *,
        client_order_id: str,
        side: str,
        quantity: str,
        status: str,
        created_ms: int,
        candidate_version: str | None = None,
        invalidation_price: str | None = None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO order_intents(client_order_id, side, quantity, status, created_ms, "
            "candidate_version, invalidation_price) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                client_order_id,
                side,
                quantity,
                status,
                created_ms,
                candidate_version,
                invalidation_price,
            ),
        )

    def order_intent(self, client_order_id: str) -> dict[str, object]:
        row = self.connection.execute(
            "SELECT * FROM order_intents WHERE client_order_id = ?", (client_order_id,)
        ).fetchone()
        if row is None:
            raise LookupError(f"unknown order intent: {client_order_id}")
        return dict(row)

    def update_order_intent(
        self,
        client_order_id: str,
        *,
        status: str,
        exchange_order_id: str | None = None,
        executed_quantity: str | None = None,
    ) -> None:
        self.connection.execute(
            "UPDATE order_intents SET status = ?, "
            "exchange_order_id = COALESCE(?, exchange_order_id), "
            "executed_quantity = COALESCE(?, executed_quantity) WHERE client_order_id = ?",
            (status, exchange_order_id, executed_quantity, client_order_id),
        )

    def record_fill(
        self,
        *,
        fill_id: str,
        client_order_id: str,
        price: str,
        quantity: str,
        commission: str,
        commission_asset: str,
        event_time_ms: int,
    ) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO fills(fill_id, client_order_id, price, quantity, commission, "
            "commission_asset, event_time_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                fill_id,
                client_order_id,
                price,
                quantity,
                commission,
                commission_asset,
                event_time_ms,
            ),
        )

    def record_protection(
        self,
        *,
        client_order_id: str,
        entry_client_order_id: str,
        exchange_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
        created_ms: int,
    ) -> None:
        self.connection.execute(
            "INSERT INTO protections(client_order_id, entry_client_order_id, exchange_order_id, "
            "quantity, stop_price, take_profit_price, created_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                client_order_id,
                entry_client_order_id,
                exchange_order_id,
                quantity,
                stop_price,
                take_profit_price,
                created_ms,
            ),
        )

    def audit_kinds(self) -> tuple[str, ...]:
        rows = self.connection.execute("SELECT kind FROM audit_events ORDER BY id").fetchall()
        return tuple(str(row[0]) for row in rows)

    def reconstruct_order(self, client_order_id: str) -> dict[str, object]:
        intent = self.order_intent(client_order_id)
        fills = tuple(
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM fills WHERE client_order_id = ? ORDER BY event_time_ms, fill_id",
                (client_order_id,),
            ).fetchall()
        )
        protection_row = self.connection.execute(
            "SELECT * FROM protections WHERE entry_client_order_id = ? "
            "ORDER BY created_ms DESC LIMIT 1",
            (client_order_id,),
        ).fetchone()
        return {
            "intent": intent,
            "fills": fills,
            "protection": dict(protection_row) if protection_row is not None else None,
        }

    def active_intents(self) -> tuple[dict[str, object], ...]:
        rows = self.connection.execute(
            "SELECT client_order_id, side, quantity, status, created_ms, exchange_order_id "
            "FROM order_intents WHERE status NOT IN ('FILLED', 'CANCELED', 'REJECTED')"
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def record_reconciliation(
        self,
        *,
        reason: str,
        event_time_ms: int,
        consistent: bool,
        divergences: tuple[str, ...],
        exchange_snapshot: dict[str, object],
    ) -> None:
        self.connection.execute(
            "INSERT INTO reconciliations(reason, event_time_ms, consistent, divergences_json, "
            "exchange_snapshot_json) VALUES (?, ?, ?, ?, ?)",
            (
                reason,
                event_time_ms,
                int(consistent),
                json.dumps(divergences, sort_keys=True),
                json.dumps(exchange_snapshot, sort_keys=True, separators=(",", ":")),
            ),
        )
        self.set_entries_allowed(consistent)

    def latest_reconciliation(self) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT reason, event_time_ms, consistent, divergences_json, exchange_snapshot_json "
            "FROM reconciliations ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise LookupError("no reconciliation has been recorded")
        return {
            "reason": row["reason"],
            "event_time_ms": row["event_time_ms"],
            "consistent": bool(row["consistent"]),
            "divergences": json.loads(row["divergences_json"]),
            "exchange_snapshot": json.loads(row["exchange_snapshot_json"]),
        }

    def close(self) -> None:
        self.connection.close()
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
        self._lock_file.close()

    def __enter__(self) -> SQLiteState:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
