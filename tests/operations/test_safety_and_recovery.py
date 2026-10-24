from __future__ import annotations

import json
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.backup import EncryptedSQLiteBackup
from scalping_bot.operations.observability import OperationalMetrics, redact_event
from scalping_bot.operations.safety import (
    ActionKind,
    ClockGuard,
    OperationalController,
    OperationalMode,
    ReservedRateCapacity,
)


def test_structured_logs_redact_credentials_and_nested_secrets() -> None:
    event = {
        "event": "request",
        "api_key": "secret-key",
        "headers": {"X-MBX-APIKEY": "secret-key", "content-type": "json"},
        "url": "https://testnet.binance.vision/api?signature=abcdef&symbol=BTCUSDT",
    }

    redacted = redact_event(event)
    encoded = json.dumps(redacted)

    assert "secret-key" not in encoded
    assert "abcdef" not in encoded
    assert redacted["api_key"] == "[REDACTED]"
    assert "symbol=BTCUSDT" in redacted["url"]


def test_degraded_pause_and_flatten_lock_block_entries_but_preserve_exits() -> None:
    controller = OperationalController()
    assert controller.allowed(ActionKind.ENTRY)

    controller.degrade("market data stale")
    assert controller.mode is OperationalMode.DEGRADED
    assert not controller.allowed(ActionKind.ENTRY)
    assert controller.allowed(ActionKind.PROTECTIVE_EXIT)

    controller.pause("operator")
    assert controller.mode is OperationalMode.PAUSED
    assert controller.allowed(ActionKind.RECONCILIATION)

    controller.flatten_and_lock("protection failed")
    assert controller.mode is OperationalMode.FLATTEN_AND_LOCK
    assert controller.allowed(ActionKind.FLATTEN)
    assert not controller.allowed(ActionKind.ENTRY)


def test_rate_limit_reserves_capacity_for_protective_actions() -> None:
    capacity = ReservedRateCapacity(total=10, reserved_for_exits=3)

    for _ in range(7):
        assert capacity.consume(ActionKind.ENTRY)
    assert not capacity.consume(ActionKind.ENTRY)
    assert capacity.consume(ActionKind.PROTECTIVE_EXIT)
    assert capacity.consume(ActionKind.FLATTEN)
    assert capacity.retry_after(429, header_seconds=2) == 2
    assert capacity.retry_after(418, header_seconds=60) == 60


def test_clock_skew_blocks_entries_before_recv_window_is_at_risk() -> None:
    guard = ClockGuard(max_skew_ms=1_000)

    assert guard.observe(exchange_time_ms=10_000, local_time_ms=10_500)
    assert not guard.observe(exchange_time_ms=10_000, local_time_ms=11_001)


def test_metrics_cover_critical_operational_states() -> None:
    metrics = OperationalMetrics()
    names = metrics.metric_names

    assert {
        "market_data_freshness_ms",
        "market_data_gaps_total",
        "order_latency_seconds",
        "reconciliation_divergences",
        "open_exposure_usdt",
        "daily_pnl_usdt",
        "circuit_breaker_state",
        "sqlite_health",
        "storage_health",
        "heartbeat_timestamp_seconds",
    } <= names


def test_encrypted_backup_restores_in_isolation_and_passes_integrity_drill(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state.db"
    state = SQLiteState(database)
    try:
        state.append_audit(kind="heartbeat", event_time_ms=1, details={"healthy": True})
    finally:
        state.close()
    backup = EncryptedSQLiteBackup(Fernet.generate_key())

    artifact = backup.create(database, destination=tmp_path / "backups", created_ms=2)
    restored = backup.restore_and_verify(artifact, destination=tmp_path / "restore")

    assert restored.exists()
    restored_state = SQLiteState(restored)
    try:
        assert restored_state.audit_kinds() == ("heartbeat",)
    finally:
        restored_state.close()


def test_corrupt_backup_fails_closed(tmp_path: Path) -> None:
    artifact = tmp_path / "corrupt.db.fernet"
    artifact.write_bytes(b"not encrypted sqlite")

    with pytest.raises(ValueError, match="decrypt"):
        EncryptedSQLiteBackup(Fernet.generate_key()).restore_and_verify(
            artifact, destination=tmp_path / "restore"
        )
