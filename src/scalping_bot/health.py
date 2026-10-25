from __future__ import annotations

import json
import sqlite3
import sys
import time
from pathlib import Path

from scalping_bot.data.storage import LocalObjectStore


def check(database: Path, *, require_running: bool = False) -> None:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1)
    try:
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("SQLite quick check failed")
        runtime = connection.execute(
            "SELECT operating_mode, entries_allowed FROM runtime_state WHERE singleton = 1"
        ).fetchone()
        if runtime is None:
            raise RuntimeError("executor runtime state is missing")
        mode = str(runtime[0])
        if require_running and mode != "RUNNING":
            raise RuntimeError(f"executor operating mode is {mode}")
        if require_running and not bool(runtime[1]):
            raise RuntimeError("executor entries are not allowed")
        reconciliation = connection.execute(
            "SELECT consistent FROM reconciliations ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if reconciliation is None:
            raise RuntimeError("executor has no reconciliation evidence")
        if require_running and not bool(reconciliation[0]):
            raise RuntimeError("executor reconciliation is divergent")
        heartbeat = connection.execute(
            "SELECT event_time_ms FROM audit_events WHERE kind = 'heartbeat' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        now_ms = time.time_ns() // 1_000_000
        if heartbeat is None or not 0 <= now_ms - int(heartbeat[0]) <= 5_000:
            raise RuntimeError("executor heartbeat is stale")
        alert_failure = connection.execute(
            "SELECT 1 FROM audit_events WHERE kind = 'critical-health-alert-failed' "
            "AND id > COALESCE((SELECT id FROM audit_events WHERE kind = 'heartbeat' "
            "ORDER BY id DESC LIMIT 1), 0) LIMIT 1"
        ).fetchone()
        if alert_failure is not None:
            raise RuntimeError("executor critical notifications are unavailable")
    finally:
        connection.close()
    backup = database.parent / "backups" / "latest.sqlite.fernet"
    if not backup.exists():
        raise RuntimeError("executor backup is missing")
    age_seconds = time.time() - backup.stat().st_mtime
    if not 0 <= age_seconds <= 2 * 300:
        raise RuntimeError("executor backup is stale")


def check_live_shadow(database: Path) -> None:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1)
    try:
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("Live Shadow SQLite quick check failed")
        metadata = dict(connection.execute("SELECT key, value FROM metadata").fetchall())
        last_receipt = metadata.get("last_receipt_time_ms")
        now_ms = time.time_ns() // 1_000_000
        if last_receipt is None:
            raise RuntimeError("Live Shadow market data is stale")
        last_receipt_ms = int(str(last_receipt))
        freshness = now_ms - last_receipt_ms
        if not 0 <= freshness <= 5_000:
            raise RuntimeError("Live Shadow market data is stale")
        if connection.execute("SELECT 1 FROM continuity LIMIT 1").fetchone() is not None:
            raise RuntimeError("Live Shadow continuity is broken")
        dependency_failure = metadata.get("last_dependency_failure_ms")
        if dependency_failure is not None and int(str(dependency_failure)) > last_receipt_ms:
            raise RuntimeError("Live Shadow has a missing market dependency")
    finally:
        connection.close()


def check_collector(storage: Path) -> None:
    store = LocalObjectStore(storage)
    keys = store.list_keys("health/collector/")
    if not keys:
        raise RuntimeError("collector has not published a health heartbeat")
    payload = json.loads(store.read(keys[-1]))
    if not isinstance(payload, dict):
        raise RuntimeError("collector health heartbeat is malformed")
    recorded_ms = int(str(payload.get("recorded_ms", 0)))
    status = payload.get("status")
    if not isinstance(status, dict) or not bool(status.get("healthy")):
        raise RuntimeError("collector health is degraded")
    age_ms = time.time_ns() // 1_000_000 - recorded_ms
    if not 0 <= age_ms <= 5_000:
        raise RuntimeError("collector health heartbeat is stale")


if __name__ == "__main__":
    database = Path(sys.argv[1] if len(sys.argv) > 1 else "state/scalping-bot.db")
    if len(sys.argv) > 2 and sys.argv[2] == "--live-shadow":
        check_live_shadow(database)
    elif len(sys.argv) > 2 and sys.argv[2] == "--collector":
        check_collector(database)
    elif len(sys.argv) > 2 and sys.argv[2] == "--executor-ready":
        check(database, require_running=True)
    else:
        check(database)
