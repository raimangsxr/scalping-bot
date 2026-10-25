from __future__ import annotations

import asyncio
import queue
import signal
import threading
import time
from decimal import Decimal
from pathlib import Path

from prometheus_client import start_http_server

from scalping_bot.config import Settings
from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.domain.costs import ExecutionCostSnapshot
from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import CandleClosed, MarketTick
from scalping_bot.execution.binance import (
    BinanceSpotTestnetAdapter,
    OfficialSpotTransport,
)
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.execution.events import (
    BinanceTestnetUserDataStream,
    ExchangeEvent,
    ExchangeEventApplier,
)
from scalping_bot.execution.executor import ExecutionLimits, ExecutionService
from scalping_bot.execution.live import BinanceTestnetMarketStream, LiveEngineRunner
from scalping_bot.execution.reconcile import Reconciler
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.alerts import CriticalAlertFailure, TelegramNotifier
from scalping_bot.operations.backup import EncryptedSQLiteBackup
from scalping_bot.operations.durable_soak import DurableTestnetSoak
from scalping_bot.operations.health import HealthSignals, OperationalHealthController
from scalping_bot.operations.observability import OperationalMetrics
from scalping_bot.operations.operator import OperatorControls
from scalping_bot.operations.operator_channel import OperatorCommandStore
from scalping_bot.operations.safety import OperationalMode


def run_executor(settings: Settings) -> None:
    if not settings.binance_api_key or not settings.binance_api_secret:
        raise RuntimeError("Binance Testnet credentials are required by the executor")
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        raise RuntimeError("Telegram credentials are required for critical executor alerts")
    if not settings.backup_encryption_key:
        raise RuntimeError("an encrypted SQLite backup key is required by the executor")
    if settings.candidate_manifest is None:
        raise RuntimeError("a promoted immutable candidate manifest is required by the executor")
    if settings.cost_model_snapshot is None:
        raise RuntimeError("a versioned calibrated execution cost snapshot is required")
    cost_snapshot = ExecutionCostSnapshot.load(Path(settings.cost_model_snapshot))
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
    backup = EncryptedSQLiteBackup(settings.backup_encryption_key.encode())
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
        health = OperationalHealthController(state, metrics)
        metrics.reconciliation_divergences.set(len(reconciliation.divergences))
        metrics.sqlite_health.set(1)
        metrics.entries_allowed.set(int(state.entries_allowed))
        state.append_audit(
            kind="cost-model-loaded",
            event_time_ms=time.time_ns() // 1_000_000,
            details={"snapshot_id": cost_snapshot.snapshot_id},
        )
        last_backup_ms = 0
        last_heartbeat_ms = 0
        engine_state = state.restore_engine_state(initial_equity=settings.initial_equity)
        engine = Engine(
            engine_state,
            candidate.policy(),
            execution_costs=cost_snapshot.as_engine_costs(),
        )
        execution = ExecutionService(
            state,
            exchange,
            ExecutionLimits(
                step_size=Decimal(capabilities.step_size),
                min_notional=Decimal(capabilities.min_notional),
                tick_size=Decimal(capabilities.tick_size),
                max_quantity=Decimal(capabilities.max_quantity),
                max_notional=Decimal(capabilities.max_notional),
            ),
            metrics=metrics,
        )
        event_applier = ExchangeEventApplier(
            state,
            reprotect_position=lambda event_time_ms: execution.reprotect_position(
                event_time_ms=event_time_ms
            ),
        )
        operator_commands = OperatorCommandStore(
            settings.state_path.with_suffix(settings.state_path.suffix + ".operator.db")
        )
        runner = LiveEngineRunner(engine=engine, execution=execution, durable_state=state)
        soak = DurableTestnetSoak(
            settings.state_path.with_name("testnet-soak.db"),
            candidate_identity=candidate.candidate_identity,
            store=LocalObjectStore(settings.state_path.parent / "testnet-soak-artifacts"),
            started_ms=time.time_ns() // 1_000_000,
        )
        stream = BinanceTestnetMarketStream(settings.binance_ws_url)
        user_events: queue.SimpleQueue[tuple[str, object]] = queue.SimpleQueue()

        def user_stream_disconnected(reason: str) -> None:
            user_events.put(("lost", reason))

        def user_stream_reconnected() -> None:
            user_events.put(("reconnected", ""))

        def collect_user_events() -> None:
            user_stream = BinanceTestnetUserDataStream(
                settings.binance_ws_url,
                transport.create_listen_key,
                keepalive=transport.keepalive_listen_key,
            )
            for exchange_event in user_stream.events(
                stop,
                on_disconnect=user_stream_disconnected,
                on_reconnect=user_stream_reconnected,
            ):
                user_events.put(("event", exchange_event))

        user_thread = threading.Thread(
            target=collect_user_events, name="binance-user-stream", daemon=True
        )
        user_thread.start()

        def drain_user_events(event_time_ms: int) -> None:
            nonlocal reconciliation
            while True:
                try:
                    kind, payload = user_events.get_nowait()
                except queue.Empty:
                    return
                if kind == "event":
                    if not isinstance(payload, ExchangeEvent):
                        raise TypeError("User Data Stream emitted a non-exchange event")
                    event_applier.apply(payload)
                elif kind == "lost":
                    event_applier.stream_lost(event_time_ms=event_time_ms, reason=str(payload))
                elif kind == "reconnected":
                    try:
                        reconciliation = Reconciler(state, exchange).run(
                            reason="user-data-stream-reconnect", event_time_ms=event_time_ms
                        )
                        reconnected_consistent = reconciliation.consistent

                        def reconciliation_ok(
                            status: bool = reconnected_consistent,
                        ) -> bool:
                            return status

                        event_applier.stream_reconciled(
                            event_time_ms=event_time_ms,
                            reconcile=reconciliation_ok,
                        )
                    except Exception:
                        event_applier.stream_reconciled(
                            event_time_ms=event_time_ms,
                            reconcile=lambda: False,
                        )

        storage_ok = True
        notification_ok = True
        dependency_last_seen: dict[str, int | None] = {
            dependency: None for dependency in candidate.market_data_dependencies
        }

        def mark_dependencies_unavailable() -> None:
            for dependency in dependency_last_seen:
                dependency_last_seen[dependency] = None

        def mark_dependency_seen(dependency: str, event_time_ms: int) -> None:
            if dependency in dependency_last_seen:
                dependency_last_seen[dependency] = event_time_ms

        def dependency_status(now_ms: int) -> dict[str, bool]:
            return {
                dependency: last_seen is not None and now_ms - last_seen <= 5_000
                for dependency, last_seen in dependency_last_seen.items()
            }

        def reconcile_after_reconnect() -> None:
            nonlocal reconciliation
            mark_dependencies_unavailable()
            event_applier.stream_lost(
                event_time_ms=time.time_ns() // 1_000_000,
                reason="market-data-stream reconnect",
            )
            reconciliation = Reconciler(state, exchange).run(
                reason="market-stream-reconnect", event_time_ms=time.time_ns() // 1_000_000
            )
            metrics.reconciliation_divergences.set(len(reconciliation.divergences))
            event_applier.stream_reconciled(
                event_time_ms=time.time_ns() // 1_000_000,
                reconcile=lambda: reconciliation.consistent,
            )

        def market_stream_disconnected(reason: str) -> None:
            mark_dependencies_unavailable()
            event_applier.stream_lost(event_time_ms=time.time_ns() // 1_000_000, reason=reason)

        def drain_operator_commands(event_time_ms: int) -> None:
            nonlocal reconciliation
            for command in operator_commands.pending():
                try:
                    controls = OperatorControls(state, execution)
                    if command.kind == "pause":
                        controls.pause(reason=str(command.reason), event_time_ms=event_time_ms)
                    elif command.kind == "flatten-and-lock":
                        controls.flatten_and_lock(
                            reason=str(command.reason), event_time_ms=event_time_ms
                        )
                    elif command.kind == "reconcile":
                        controls.request_reconciliation(event_time_ms=event_time_ms)
                        reconciliation = Reconciler(state, exchange).run(
                            reason="operator-request", event_time_ms=event_time_ms
                        )
                        metrics.reconciliation_divergences.set(len(reconciliation.divergences))
                    elif command.kind == "unlock":
                        controls.unlock(
                            event_time_ms=event_time_ms,
                            operator_confirmed=bool(command.operator_confirmed),
                        )
                    operator_commands.complete(command.command_id)
                except Exception as error:
                    state.set_operating_mode(OperationalMode.DEGRADED)
                    state.append_audit(
                        kind="operator-command-failed",
                        event_time_ms=event_time_ms,
                        details={
                            "command_id": command.command_id,
                            "command_kind": command.kind,
                            "error_type": type(error).__name__,
                        },
                    )
                    operator_commands.complete(command.command_id, error=type(error).__name__)

        try:
            for event in stream.events(
                stop,
                on_disconnect=market_stream_disconnected,
                on_reconnect=reconcile_after_reconnect,
            ):
                now_ms = time.time_ns() // 1_000_000
                if isinstance(event, CandleClosed):
                    mark_dependency_seen("closed-1m-klines", now_ms)
                elif isinstance(event, MarketTick):
                    mark_dependency_seen("aggTrades", now_ms)
                    mark_dependency_seen("tradeTicks", now_ms)
                drain_operator_commands(now_ms)
                prior_entry_id: str | None = None
                prior_position = state.position()
                if prior_position is not None:
                    prior_entry_id = state.position_entry_client_order_id()
                drain_user_events(now_ms)
                try:
                    persistence_ok = state.persistence_healthy()
                except Exception:
                    persistence_ok = False
                prior_mode = state.operating_mode
                health_result = health.apply(
                    HealthSignals(
                        market_data_freshness_ms=max(0, now_ms - event.event_time_ms),
                        clock_drift_ms=now_ms - event.event_time_ms,
                        persistence_ok=persistence_ok,
                        reconciliation_ok=reconciliation.consistent,
                        storage_ok=storage_ok,
                        notification_ok=notification_ok,
                        rate_limit_blocked=state.rate_limit_blocked,
                        dependency_status=dependency_status(now_ms),
                    ),
                    event_time_ms=now_ms,
                )
                if (
                    health_result.mode is OperationalMode.DEGRADED
                    and prior_mode is not OperationalMode.DEGRADED
                    and notifier is not None
                ):
                    try:
                        asyncio.run(
                            notifier.send(
                                "CRITICAL scalping-bot health degraded: "
                                + "; ".join(health_result.reasons),
                                critical=True,
                            )
                        )
                    except CriticalAlertFailure as error:
                        notification_ok = False
                        metrics.alert_failures_total.inc()
                        state.append_audit(
                            kind="critical-health-alert-failed",
                            event_time_ms=now_ms,
                            details={"error_type": type(error).__name__},
                        )
                runner.process(event)
                if prior_entry_id is not None and state.position() is None:
                    try:
                        if prior_position is None:
                            raise RuntimeError("durable position disappeared before capture")
                        notional_fraction = (
                            Decimal(str(prior_position["entry_price"]))
                            * Decimal(str(prior_position["quantity"]))
                            / Decimal(settings.initial_equity)
                        )
                        soak.record_authoritative_scalp(
                            state,
                            entry_client_order_id=prior_entry_id,
                            event_time_ms=event.event_time_ms,
                            position_risk=Decimal(settings.position_risk),
                            notional_fraction=notional_fraction,
                        )
                        state.append_audit(
                            kind="testnet-soak-scalp-recorded",
                            event_time_ms=event.event_time_ms,
                            details={"entry_client_order_id": prior_entry_id},
                        )
                    except (LookupError, RuntimeError, ValueError) as error:
                        state.set_operating_mode(OperationalMode.DEGRADED)
                        state.append_audit(
                            kind="testnet-soak-evidence-failed",
                            event_time_ms=event.event_time_ms,
                            details={"error_type": type(error).__name__},
                        )
                state.append_audit(
                    kind="heartbeat",
                    event_time_ms=now_ms,
                    details={
                        "entries_allowed": state.entries_allowed,
                        "operating_mode": state.operating_mode,
                    },
                )
                metrics.heartbeat_timestamp_seconds.set(now_ms / 1000)
                if now_ms - last_backup_ms >= backup.RPO_SECONDS * 1000:
                    try:
                        artifact = backup.create(
                            settings.state_path,
                            destination=settings.state_path.parent / "backups",
                            created_ms=now_ms,
                        )
                        backup.publish_latest(artifact)
                        metrics.storage_health.set(1)
                        last_backup_ms = now_ms
                        storage_ok = True
                    except (OSError, ValueError) as error:
                        metrics.storage_health.set(0)
                        storage_ok = False
                        state.set_operating_mode(OperationalMode.DEGRADED)
                        state.append_audit(
                            kind="backup-failed",
                            event_time_ms=now_ms,
                            details={"error_type": type(error).__name__},
                        )
                if notifier is not None and now_ms - last_heartbeat_ms >= 24 * 60 * 60 * 1000:
                    try:
                        asyncio.run(
                            notifier.heartbeat(
                                mode=state.operating_mode.value,
                                reconciliation_ok=reconciliation.consistent,
                            )
                        )
                    except CriticalAlertFailure:
                        notification_ok = False
                        state.set_operating_mode(OperationalMode.DEGRADED)
                        metrics.alert_failures_total.inc()
                    last_heartbeat_ms = now_ms
                metrics.entries_allowed.set(int(state.entries_allowed))
                metrics.operating_mode.set(float(list(OperationalMode).index(state.operating_mode)))
        finally:
            stop.set()
            user_thread.join(timeout=5)
            soak.close()
            operator_commands.close()
