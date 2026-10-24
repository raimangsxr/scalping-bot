from __future__ import annotations

import asyncio
import signal
import threading
import time
from decimal import Decimal

from prometheus_client import start_http_server

from scalping_bot.config import Settings
from scalping_bot.domain.engine import Engine
from scalping_bot.domain.models import ExecutionCosts
from scalping_bot.execution.binance import (
    BinanceSpotTestnetAdapter,
    OfficialSpotTransport,
)
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.execution.executor import ExecutionLimits, ExecutionService
from scalping_bot.execution.live import BinanceTestnetMarketStream, LiveEngineRunner
from scalping_bot.execution.reconcile import Reconciler
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.alerts import CriticalAlertFailure, TelegramNotifier
from scalping_bot.operations.backup import EncryptedSQLiteBackup
from scalping_bot.operations.observability import OperationalMetrics
from scalping_bot.operations.safety import OperationalMode


def run_executor(settings: Settings) -> None:
    if settings.binance_api_key is None or settings.binance_api_secret is None:
        raise RuntimeError("Binance Testnet credentials are required by the executor")
    if settings.candidate_manifest is None:
        raise RuntimeError("a promoted immutable candidate manifest is required by the executor")
    if (
        settings.calibrated_half_spread_rate is None
        or settings.calibrated_slippage_rate is None
        or settings.cost_model_snapshot is None
    ):
        raise RuntimeError("a versioned calibrated execution cost snapshot is required")
    candidate = PromotedCandidateManifest.load(settings.candidate_manifest)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    signal.signal(signal.SIGINT, lambda *_args: stop.set())
    metrics = OperationalMetrics()
    start_http_server(8000, registry=metrics.registry)
    notifier = (
        TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
        if settings.telegram_bot_token and settings.telegram_chat_id
        else None
    )
    backup = (
        EncryptedSQLiteBackup(settings.backup_encryption_key.encode())
        if settings.backup_encryption_key
        else None
    )
    with SQLiteState(settings.state_path) as state:
        transport = OfficialSpotTransport(
            api_key=settings.binance_api_key, api_secret=settings.binance_api_secret
        )
        exchange = BinanceSpotTestnetAdapter(
            endpoint=settings.binance_rest_url, transport=transport
        )
        capabilities = exchange.load_capabilities()
        if capabilities.symbol_status != "TRADING":
            raise RuntimeError("BTCUSDT is not currently available for Testnet trading")
        if not {"MARKET", "STOP_LOSS_LIMIT"}.issubset(capabilities.order_types):
            raise RuntimeError("Binance Testnet lacks required entry/protection order types")
        reconciliation = Reconciler(state, exchange).run(
            reason="startup", event_time_ms=time.time_ns() // 1_000_000
        )
        metrics.reconciliation_divergences.set(len(reconciliation.divergences))
        metrics.sqlite_health.set(1)
        last_backup_ms = 0
        last_heartbeat_ms = 0
        engine_state = state.restore_engine_state(initial_equity=settings.initial_equity)
        engine = Engine(
            engine_state,
            candidate.policy(),
            execution_costs=ExecutionCosts(
                commission_rate=Decimal(capabilities.taker_commission),
                half_spread_rate=Decimal(settings.calibrated_half_spread_rate),
                slippage_rate=Decimal(settings.calibrated_slippage_rate),
            ),
        )
        runner = LiveEngineRunner(
            engine=engine,
            execution=ExecutionService(
                state,
                exchange,
                ExecutionLimits(
                    step_size=Decimal(capabilities.step_size),
                    min_notional=Decimal(capabilities.min_notional),
                    tick_size=Decimal(capabilities.tick_size),
                    max_quantity=Decimal(capabilities.max_quantity),
                    max_notional=Decimal(capabilities.max_notional),
                ),
            ),
            durable_state=state,
        )
        stream = BinanceTestnetMarketStream(settings.binance_ws_url)

        def reconcile_after_reconnect() -> None:
            nonlocal reconciliation
            reconciliation = Reconciler(state, exchange).run(
                reason="market-stream-reconnect", event_time_ms=time.time_ns() // 1_000_000
            )
            metrics.reconciliation_divergences.set(len(reconciliation.divergences))

        for event in stream.events(stop, on_reconnect=reconcile_after_reconnect):
            runner.process(event)
            now_ms = event.event_time_ms
            state.append_audit(
                kind="heartbeat",
                event_time_ms=now_ms,
                details={
                    "entries_allowed": state.entries_allowed,
                    "operating_mode": state.operating_mode,
                },
            )
            metrics.heartbeat_timestamp_seconds.set(now_ms / 1000)
            if backup is not None and now_ms - last_backup_ms >= backup.RPO_SECONDS * 1000:
                artifact = backup.create(
                    settings.state_path,
                    destination=settings.state_path.parent / "backups",
                    created_ms=now_ms,
                )
                latest = artifact.parent / "latest.sqlite.fernet"
                latest.write_bytes(artifact.read_bytes())
                last_backup_ms = now_ms
            if notifier is not None and now_ms - last_heartbeat_ms >= 24 * 60 * 60 * 1000:
                try:
                    asyncio.run(
                        notifier.heartbeat(
                            mode=state.operating_mode.value,
                            reconciliation_ok=reconciliation.consistent,
                        )
                    )
                except CriticalAlertFailure:
                    state.set_operating_mode(OperationalMode.DEGRADED)
                last_heartbeat_ms = now_ms
