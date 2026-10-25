from __future__ import annotations

import fcntl
import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

from scalping_bot.domain.models import Position
from scalping_bot.domain.state import InMemoryState
from scalping_bot.operations.safety import OperationalMode


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
    """
    ALTER TABLE order_intents ADD COLUMN source_event_id TEXT;
    CREATE TABLE positions (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        candidate_version TEXT NOT NULL,
        source_event_id TEXT NOT NULL,
        entry_fill_id TEXT NOT NULL,
        entry_time_ms INTEGER NOT NULL,
        entry_price TEXT NOT NULL,
        quantity TEXT NOT NULL,
        invalidation_price TEXT NOT NULL,
        commission TEXT NOT NULL,
        protected INTEGER NOT NULL CHECK (protected IN (0, 1)),
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE circuit_breakers (
        kind TEXT PRIMARY KEY,
        active INTEGER NOT NULL CHECK (active IN (0, 1)),
        triggered_ms INTEGER NOT NULL,
        reason TEXT NOT NULL,
        operator_confirmation_required INTEGER NOT NULL CHECK (
            operator_confirmation_required IN (0, 1)
        )
    );
    """,
    """
    ALTER TABLE runtime_state ADD COLUMN current_equity TEXT;
    ALTER TABLE runtime_state ADD COLUMN high_water_mark TEXT;
    ALTER TABLE runtime_state ADD COLUMN day_open_equity TEXT;
    ALTER TABLE runtime_state ADD COLUMN utc_day INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE runtime_state ADD COLUMN daily_locked INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE runtime_state ADD COLUMN drawdown_locked INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE runtime_state ADD COLUMN operator_confirmed_day INTEGER;
    """,
    """
    ALTER TABLE runtime_state ADD COLUMN rate_limit_blocked INTEGER NOT NULL DEFAULT 0
        CHECK (rate_limit_blocked IN (0, 1));
    ALTER TABLE protections ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE';
    ALTER TABLE protections ADD COLUMN closed_ms INTEGER;
    """,
    """
    CREATE TABLE IF NOT EXISTS account_balances (
        asset TEXT PRIMARY KEY,
        free TEXT NOT NULL,
        locked TEXT NOT NULL,
        updated_ms INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS protection_lifecycle (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        list_id TEXT NOT NULL,
        client_order_id TEXT,
        status TEXT NOT NULL,
        event_time_ms INTEGER NOT NULL,
        details_json TEXT NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS protection_lifecycle_no_update
    BEFORE UPDATE ON protection_lifecycle BEGIN
        SELECT RAISE(ABORT, 'protection_lifecycle is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS protection_lifecycle_no_delete
    BEFORE DELETE ON protection_lifecycle BEGIN
        SELECT RAISE(ABORT, 'protection_lifecycle is append-only');
    END;
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
        if allowed and self.operating_mode is not OperationalMode.RUNNING:
            allowed = False
        self.connection.execute(
            "UPDATE runtime_state SET entries_allowed = ? WHERE singleton = 1",
            (int(allowed),),
        )

    @property
    def operating_mode(self) -> OperationalMode:
        row = self.connection.execute(
            "SELECT operating_mode FROM runtime_state WHERE singleton = 1"
        ).fetchone()
        return OperationalMode(str(row[0]))

    @property
    def rate_limit_blocked(self) -> bool:
        row = self.connection.execute(
            "SELECT rate_limit_blocked FROM runtime_state WHERE singleton = 1"
        ).fetchone()
        return bool(row[0])

    def set_rate_limit_blocked(self, blocked: bool) -> None:
        self.connection.execute(
            "UPDATE runtime_state SET rate_limit_blocked = ? WHERE singleton = 1",
            (int(blocked),),
        )

    def persistence_healthy(self) -> bool:
        return str(self.connection.execute("PRAGMA quick_check").fetchone()[0]).lower() == "ok"

    def set_operating_mode(self, mode: OperationalMode) -> None:
        self.connection.execute(
            "UPDATE runtime_state SET operating_mode = ?, entries_allowed = 0 WHERE singleton = 1",
            (mode.value,),
        )

    def upsert_balance(self, *, asset: str, free: str, locked: str, updated_ms: int) -> None:
        if not asset:
            raise ValueError("account balance asset is required")
        self.connection.execute(
            "INSERT INTO account_balances(asset, free, locked, updated_ms) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(asset) DO UPDATE SET free = excluded.free, locked = excluded.locked, "
            "updated_ms = excluded.updated_ms",
            (asset, free, locked, updated_ms),
        )

    def balances(self) -> tuple[dict[str, object], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                "SELECT asset, free, locked, updated_ms FROM account_balances ORDER BY asset"
            ).fetchall()
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
        source_event_id: str | None = None,
        invalidation_price: str | None = None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO order_intents(client_order_id, side, quantity, status, created_ms, "
            "candidate_version, source_event_id, invalidation_price) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                client_order_id,
                side,
                quantity,
                status,
                created_ms,
                candidate_version,
                source_event_id,
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

    def order_intents_for_source(self, source_event_id: str) -> tuple[dict[str, object], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM order_intents WHERE source_event_id = ? "
                "ORDER BY created_ms, client_order_id",
                (source_event_id,),
            ).fetchall()
        )

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
        existing = self.connection.execute(
            "SELECT client_order_id, price, quantity, commission, commission_asset, event_time_ms "
            "FROM fills WHERE fill_id = ?",
            (fill_id,),
        ).fetchone()
        if existing is not None:
            expected = (client_order_id, price, quantity, commission, commission_asset)
            actual = tuple(existing[:5])
            if actual != expected:
                raise sqlite3.IntegrityError(f"fill {fill_id} was replayed with different data")
            return
        self.connection.execute(
            "INSERT INTO fills(fill_id, client_order_id, price, quantity, commission, "
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

    def fills_for_order(self, client_order_id: str) -> tuple[dict[str, object], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM fills WHERE client_order_id = ? ORDER BY event_time_ms, fill_id",
                (client_order_id,),
            ).fetchall()
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
            "INSERT OR REPLACE INTO protections(client_order_id, entry_client_order_id, "
            "exchange_order_id, "
            "quantity, stop_price, take_profit_price, created_ms, status, closed_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', NULL)",
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

    def protection(self, entry_client_order_id: str) -> dict[str, object] | None:
        row = self.connection.execute(
            "SELECT * FROM protections WHERE entry_client_order_id = ? AND status = 'ACTIVE' "
            "ORDER BY created_ms DESC LIMIT 1",
            (entry_client_order_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def protection_history(self, entry_client_order_id: str) -> tuple[dict[str, object], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM protections WHERE entry_client_order_id = ? "
                "ORDER BY created_ms, client_order_id",
                (entry_client_order_id,),
            ).fetchall()
        )

    def close_protection(self, client_order_id: str, *, closed_ms: int) -> None:
        self.connection.execute(
            "UPDATE protections SET status = 'CANCELED', closed_ms = ? "
            "WHERE client_order_id = ? AND status = 'ACTIVE'",
            (closed_ms, client_order_id),
        )

    def record_protection_list_status(
        self,
        *,
        list_id: str,
        status: str,
        client_order_id: str | None,
        event_time_ms: int,
        order_reports: tuple[dict[str, str], ...] = (),
    ) -> None:
        if not list_id or not status:
            raise ValueError("protection list lifecycle requires an id and status")
        details = json.dumps(
            {"order_reports": order_reports}, sort_keys=True, separators=(",", ":")
        )
        existing = self.connection.execute(
            "SELECT details_json FROM protection_lifecycle WHERE list_id = ? "
            "AND status = ? AND event_time_ms = ? "
            "AND (client_order_id = ? OR (client_order_id IS NULL AND ? IS NULL))",
            (list_id, status, event_time_ms, client_order_id, client_order_id),
        ).fetchone()
        if existing is not None:
            if str(existing["details_json"]) != details:
                raise ValueError("protection list lifecycle event is immutable")
            return
        self.connection.execute(
            "INSERT INTO protection_lifecycle(list_id, client_order_id, status, event_time_ms, "
            "details_json) VALUES (?, ?, ?, ?, ?)",
            (list_id, client_order_id, status, event_time_ms, details),
        )
        lifecycle_status = {
            "EXEC_STARTED": "ACTIVE",
            "EXECUTING": "ACTIVE",
            "ALL_DONE": "TRIGGERED",
            "CANCELED": "CANCELED",
            "REJECT": "REJECTED",
        }.get(status)
        if lifecycle_status is None:
            return
        closed_ms = event_time_ms if lifecycle_status != "ACTIVE" else None
        self.connection.execute(
            "UPDATE protections SET status = ?, closed_ms = COALESCE(?, closed_ms) "
            "WHERE exchange_order_id = ? OR client_order_id = ?",
            (lifecycle_status, closed_ms, list_id, client_order_id),
        )

    def upsert_position(
        self,
        *,
        candidate_version: str,
        source_event_id: str,
        entry_fill_id: str,
        entry_time_ms: int,
        entry_price: str,
        quantity: str,
        invalidation_price: str,
        commission: str,
        protected: bool,
    ) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO positions(singleton, candidate_version, source_event_id, "
            "entry_fill_id, entry_time_ms, entry_price, quantity, invalidation_price, commission, "
            "protected) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                candidate_version,
                source_event_id,
                entry_fill_id,
                entry_time_ms,
                entry_price,
                quantity,
                invalidation_price,
                commission,
                int(protected),
            ),
        )

    def position(self) -> dict[str, object] | None:
        row = self.connection.execute("SELECT * FROM positions WHERE singleton = 1").fetchone()
        return dict(row) if row is not None else None

    def rebuild_position_from_order(self, client_order_id: str) -> bool:
        """Reconstruct a filled BUY position after a crash between durable writes."""
        intent = self.order_intent(client_order_id)
        if intent["side"] != "BUY":
            return False
        candidate_version = intent["candidate_version"]
        source_event_id = intent["source_event_id"]
        invalidation_price = intent["invalidation_price"]
        if candidate_version is None or source_event_id is None or invalidation_price is None:
            return False
        fills = self.fills_for_order(client_order_id)
        if not fills:
            return False
        quantity = sum((Decimal(str(item["quantity"])) for item in fills), Decimal(0))
        notional = sum(
            (Decimal(str(item["price"])) * Decimal(str(item["quantity"])) for item in fills),
            Decimal(0),
        )
        commission = sum((Decimal(str(item["commission"])) for item in fills), Decimal(0))
        self.upsert_position(
            candidate_version=str(candidate_version),
            source_event_id=str(source_event_id),
            entry_fill_id=str(fills[0]["fill_id"]),
            entry_time_ms=int(str(fills[0]["event_time_ms"])),
            entry_price=str(notional / quantity),
            quantity=str(quantity),
            invalidation_price=str(invalidation_price),
            commission=str(commission),
            protected=self.protection(client_order_id) is not None,
        )
        return True

    def apply_exit_fills(self, client_order_id: str, *, event_time_ms: int) -> bool:
        """Apply cumulative streamed SELL fills to the durable open position."""
        intent = self.order_intent(client_order_id)
        if intent["side"] != "SELL":
            return False
        position = self.position()
        if position is None:
            return False
        fills = self.fills_for_order(client_order_id)
        executed = sum((Decimal(str(item["quantity"])) for item in fills), Decimal(0))
        open_quantity = Decimal(str(position["quantity"]))
        if executed <= 0:
            return False
        if executed >= open_quantity:
            entry_client_order_id = self.position_entry_client_order_id()
            self.update_order_intent(entry_client_order_id, status="CLOSED")
            self.clear_position()
            self.append_audit(
                kind="streamed-position-closed",
                event_time_ms=event_time_ms,
                details={
                    "entry_client_order_id": entry_client_order_id,
                    "exit_client_order_id": client_order_id,
                    "executed_quantity": str(executed),
                },
            )
            return True
        remaining = open_quantity - executed
        self.upsert_position(
            candidate_version=str(position["candidate_version"]),
            source_event_id=str(position["source_event_id"]),
            entry_fill_id=str(position["entry_fill_id"]),
            entry_time_ms=int(str(position["entry_time_ms"])),
            entry_price=str(position["entry_price"]),
            quantity=str(remaining),
            invalidation_price=str(position["invalidation_price"]),
            commission=str(position["commission"]),
            protected=False,
        )
        self.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
        self.append_audit(
            kind="streamed-partial-position-exit",
            event_time_ms=event_time_ms,
            details={
                "exit_client_order_id": client_order_id,
                "executed_quantity": str(executed),
                "remaining_quantity": str(remaining),
            },
        )
        return True

    def position_entry_client_order_id(self) -> str:
        row = self.connection.execute(
            "SELECT f.client_order_id FROM positions p "
            "JOIN fills f ON f.fill_id = p.entry_fill_id WHERE p.singleton = 1"
        ).fetchone()
        if row is None:
            raise LookupError("no durable open position")
        return str(row[0])

    def mark_position_protected(self) -> None:
        self.connection.execute("UPDATE positions SET protected = 1 WHERE singleton = 1")

    def clear_position(self) -> None:
        self.connection.execute("DELETE FROM positions WHERE singleton = 1")

    def set_circuit_breaker(
        self,
        *,
        kind: str,
        active: bool,
        triggered_ms: int,
        reason: str,
        operator_confirmation_required: bool,
    ) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO circuit_breakers(kind, active, triggered_ms, reason, "
            "operator_confirmation_required) VALUES (?, ?, ?, ?, ?)",
            (kind, int(active), triggered_ms, reason, int(operator_confirmation_required)),
        )

    def persist_engine_state(self, state: InMemoryState, *, event_time_ms: int) -> None:
        self.connection.execute(
            "UPDATE runtime_state SET current_equity = ?, high_water_mark = ?, "
            "day_open_equity = ?, utc_day = ?, daily_locked = ?, drawdown_locked = ?, "
            "operator_confirmed_day = ? WHERE singleton = 1",
            (
                str(state.current_equity),
                str(state.high_water_mark),
                str(state.day_open_equity),
                state.utc_day,
                int(state.daily_locked),
                int(state.drawdown_locked),
                state.operator_confirmed_day,
            ),
        )
        self.set_circuit_breaker(
            kind="daily-loss",
            active=state.daily_locked,
            triggered_ms=event_time_ms,
            reason="daily loss threshold",
            operator_confirmation_required=True,
        )
        self.set_circuit_breaker(
            kind="drawdown",
            active=state.drawdown_locked,
            triggered_ms=event_time_ms,
            reason="maximum drawdown threshold",
            operator_confirmation_required=True,
        )

    def restore_engine_state(self, *, initial_equity: str) -> InMemoryState:
        state = InMemoryState(initial_equity=Decimal(initial_equity))
        state.entries_allowed = self.entries_allowed
        row = self.connection.execute(
            "SELECT current_equity, high_water_mark, day_open_equity, utc_day, daily_locked, "
            "drawdown_locked, operator_confirmed_day FROM runtime_state WHERE singleton = 1"
        ).fetchone()
        if row is not None and row["current_equity"] is not None:
            state.current_equity = Decimal(str(row["current_equity"]))
            state.high_water_mark = Decimal(str(row["high_water_mark"]))
            state.day_open_equity = Decimal(str(row["day_open_equity"]))
            state.utc_day = int(row["utc_day"])
            state.daily_locked = bool(row["daily_locked"])
            state.drawdown_locked = bool(row["drawdown_locked"])
            state.operator_confirmed_day = (
                int(row["operator_confirmed_day"])
                if row["operator_confirmed_day"] is not None
                else None
            )
        position = self.position()
        if position is not None:
            state.position = Position(
                candidate_version=str(position["candidate_version"]),
                source_event_id=str(position["source_event_id"]),
                entry_fill_id=str(position["entry_fill_id"]),
                entry_time_ms=int(str(position["entry_time_ms"])),
                entry_price=Decimal(str(position["entry_price"])),
                quantity=Decimal(str(position["quantity"])),
                invalidation_price=Decimal(str(position["invalidation_price"])),
                commission=Decimal(str(position["commission"])),
                protected=bool(position["protected"]),
            )
        return state

    def audit_kinds(self) -> tuple[str, ...]:
        rows = self.connection.execute("SELECT kind FROM audit_events ORDER BY id").fetchall()
        return tuple(str(row[0]) for row in rows)

    def audit_events(self, client_order_id: str | None = None) -> tuple[dict[str, object], ...]:
        rows = self.connection.execute(
            "SELECT id, kind, event_time_ms, details_json FROM audit_events ORDER BY id"
        ).fetchall()
        events: list[dict[str, object]] = []
        for row in rows:
            details = json.loads(str(row["details_json"]))
            if client_order_id is not None and not _details_reference(details, client_order_id):
                continue
            events.append(
                {
                    "id": int(row["id"]),
                    "kind": str(row["kind"]),
                    "event_time_ms": int(row["event_time_ms"]),
                    "details": details,
                }
            )
        return tuple(events)

    def reconstruct_order(self, client_order_id: str) -> dict[str, object]:
        intent = self.order_intent(client_order_id)
        fills = self.fills_for_order(client_order_id)
        protection_row = self.connection.execute(
            "SELECT * FROM protections WHERE entry_client_order_id = ? "
            "ORDER BY created_ms DESC LIMIT 1",
            (client_order_id,),
        ).fetchone()
        return {
            "intent": intent,
            "fills": fills,
            "protection": dict(protection_row) if protection_row is not None else None,
            "protection_history": self.protection_history(client_order_id),
            "audit": self.audit_events(client_order_id),
        }

    def reconstruct_scalp(self, entry_client_order_id: str) -> dict[str, object]:
        """Return the durable proposal-to-P&L trace for one entry intent."""
        entry = self.order_intent(entry_client_order_id)
        exit_rows = self.connection.execute(
            "SELECT * FROM order_intents WHERE side = 'SELL' AND source_event_id = ? "
            "ORDER BY created_ms, client_order_id",
            (entry["source_event_id"],),
        ).fetchall()
        exit_orders = tuple(dict(row) for row in exit_rows)
        order_ids = (
            entry_client_order_id,
            *tuple(str(row["client_order_id"]) for row in exit_rows),
        )
        placeholders = ",".join("?" for _ in order_ids)
        fills = tuple(
            dict(row)
            for row in self.connection.execute(
                f"SELECT * FROM fills WHERE client_order_id IN ({placeholders}) "
                "ORDER BY event_time_ms, fill_id",
                order_ids,
            ).fetchall()
        )
        entry_fills = tuple(
            item for item in fills if item["client_order_id"] == entry_client_order_id
        )
        exit_fills = tuple(
            item for item in fills if item["client_order_id"] != entry_client_order_id
        )
        commission_by_asset: dict[str, Decimal] = {}
        for fill in fills:
            asset = str(fill["commission_asset"])
            commission_by_asset[asset] = commission_by_asset.get(asset, Decimal(0)) + Decimal(
                str(fill["commission"])
            )
        entry_notional = sum(
            (Decimal(str(item["price"])) * Decimal(str(item["quantity"])) for item in entry_fills),
            Decimal(0),
        )
        exit_notional = sum(
            (Decimal(str(item["price"])) * Decimal(str(item["quantity"])) for item in exit_fills),
            Decimal(0),
        )
        pnl: dict[str, object] = {
            "entry_notional": str(entry_notional),
            "exit_notional": str(exit_notional),
            "commission_by_asset": {key: str(value) for key, value in commission_by_asset.items()},
            "gross_pnl": str(exit_notional - entry_notional),
        }
        if set(commission_by_asset) <= {"USDT"}:
            pnl["net_pnl"] = str(
                exit_notional - entry_notional - sum(commission_by_asset.values(), Decimal(0))
            )
        else:
            pnl["net_pnl"] = "non-evaluable: commission assets require conversion"
        trace_ids = {entry_client_order_id, *(str(row["client_order_id"]) for row in exit_rows)}
        audit = tuple(
            item
            for item in self.audit_events()
            if any(_details_reference(item["details"], identity) for identity in trace_ids)
        )
        return {
            "entry": entry,
            "exit_orders": exit_orders,
            "fills": fills,
            "protection": self.protection(entry_client_order_id),
            "protection_history": self.protection_history(entry_client_order_id),
            "protection_lifecycle": self.protection_lifecycle(entry_client_order_id),
            "audit": audit,
            "pnl": pnl,
        }

    def protection_lifecycle(self, entry_client_order_id: str) -> tuple[dict[str, object], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM protection_lifecycle WHERE list_id IN ("
                "SELECT exchange_order_id FROM protections WHERE entry_client_order_id = ?) "
                "OR client_order_id IN (SELECT client_order_id FROM protections "
                "WHERE entry_client_order_id = ?) ORDER BY event_time_ms, id",
                (entry_client_order_id, entry_client_order_id),
            ).fetchall()
        )

    def active_intents(self) -> tuple[dict[str, object], ...]:
        rows = self.connection.execute(
            "SELECT client_order_id, side, quantity, status, created_ms, exchange_order_id "
            "FROM order_intents WHERE status NOT IN "
            "('FILLED', 'CANCELED', 'REJECTED', 'FLATTENED_LOCKED', 'CLOSED')"
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def reconciliation_intents(self) -> tuple[dict[str, object], ...]:
        return tuple(
            intent
            for intent in self.active_intents()
            if intent["status"] in {"SUBMITTING", "UNKNOWN", "NEW", "PARTIALLY_FILLED"}
        )

    def partial_entry_intents(self) -> tuple[dict[str, object], ...]:
        rows = self.connection.execute(
            "SELECT * FROM order_intents WHERE side = 'BUY' AND status = 'PARTIALLY_FILLED'"
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def fill_ids(self) -> frozenset[str]:
        rows = self.connection.execute("SELECT fill_id FROM fills").fetchall()
        return frozenset(str(row[0]) for row in rows)

    def exchange_orders(self) -> dict[str, str]:
        rows = self.connection.execute(
            "SELECT exchange_order_id, client_order_id FROM order_intents "
            "WHERE exchange_order_id IS NOT NULL"
        ).fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def active_protection_ids(self) -> frozenset[str]:
        rows = self.connection.execute(
            "SELECT exchange_order_id FROM protections "
            "WHERE status = 'ACTIVE' AND entry_client_order_id IN "
            "(SELECT client_order_id FROM order_intents "
            "WHERE status IN ('PROTECTED', 'PARTIALLY_FILLED'))"
        ).fetchall()
        return frozenset(str(row[0]) for row in rows)

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


def _details_reference(details: object, client_order_id: str) -> bool:
    if isinstance(details, dict):
        return any(
            value == client_order_id or _details_reference(value, client_order_id)
            for value in details.values()
        )
    if isinstance(details, (list, tuple)):
        return any(_details_reference(value, client_order_id) for value in details)
    return False
