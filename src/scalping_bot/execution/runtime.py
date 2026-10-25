from __future__ import annotations

import asyncio
import contextlib
import hashlib
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
    if candidate.execution_cost_model_identity != cost_snapshot.snapshot_id:
        raise RuntimeError("candidate and execution cost model identities do not match")
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
        soak = DurableTestnetSoak(
            settings.state_path.with_name("testnet-soak.db"),
            candidate_identity=candidate.candidate_identity,
            store=LocalObjectStore(settings.state_path.parent / "testnet-soak-artifacts"),
            started_ms=time.time_ns() // 1_000_000,
            candidate_manifest_sha256=hashlib.sha256(
                settings.candidate_manifest.read_bytes()
            ).hexdigest(),
            execution_cost_model_identity=cost_snapshot.snapshot_id,
            release_digest=settings.release_digest,
        )

        def record_user_closed_scalp(
            entry_client_order_id: str, event_time_ms: int, position: dict[str, object]
        ) -> None:
            try:
                notional_fraction = (
                    Decimal(str(position["entry_price"]))
                    * Decimal(str(position["quantity"]))
                    / Decimal(settings.initial_equity)
                )
                soak.record_authoritative_scalp(
                    state,
                    entry_client_order_id=entry_client_order_id,
                    event_time_ms=event_time_ms,
                    position_risk=Decimal(settings.position_risk),
                    notional_fraction=notional_fraction,
                )
                state.append_audit(
                    kind="testnet-soak-scalp-recorded",
                    event_time_ms=event_time_ms,
                    details={"entry_client_order_id": entry_client_order_id},
                )
            except (LookupError, RuntimeError, ValueError) as error:
                state.set_operating_mode(OperationalMode.DEGRADED)
                state.append_audit(
                    kind="testnet-soak-evidence-failed",
                    event_time_ms=event_time_ms,
                    details={"error_type": type(error).__name__},
                )

        event_applier = ExchangeEventApplier(
            state,
            reprotect_position=lambda event_time_ms: execution.reprotect_position(
                event_time_ms=event_time_ms
            ),
            on_position_closed=record_user_closed_scalp,
        )
        operator_database_path = settings.state_path.with_suffix(
            settings.state_path.suffix + ".operator.db"
        )
        operator_commands = OperatorCommandStore(operator_database_path)
        runner = LiveEngineRunner(engine=engine, execution=execution, durable_state=state)
        stream = BinanceTestnetMarketStream(settings.binance_ws_url)
        market_events: queue.Queue[object] = queue.Queue()
        market_notifications: queue.SimpleQueue[tuple[str, str]] = queue.SimpleQueue()
        user_events: queue.SimpleQueue[tuple[str, object]] = queue.SimpleQueue()
        market_stream_connected = False
        user_stream_connected = False

        # Both exchange streams must be live before the executor can reopen
        # entries.  A restart therefore begins fail-closed even if the durable
        # database was previously RUNNING.
        state.set_operating_mode(OperationalMode.DEGRADED)

        def user_stream_disconnected(reason: str) -> None:
            user_events.put(("lost", reason))

        def user_stream_connected_callback() -> None:
            user_events.put(("connected", ""))

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
                on_connect=user_stream_connected_callback,
                on_reconnect=user_stream_reconnected,
            ):
                user_events.put(("event", exchange_event))

        user_thread = threading.Thread(
            target=collect_user_events, name="binance-user-stream", daemon=True
        )
        user_thread.start()

        def drain_user_events(event_time_ms: int) -> None:
            nonlocal reconciliation, user_stream_connected
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
                    user_stream_connected = False
                    event_applier.stream_lost(event_time_ms=event_time_ms, reason=str(payload))
                    send_critical_alert(
                        event_time_ms,
                        "CRITICAL scalping-bot User Data stream lost: " + str(payload),
                    )
                elif kind in {"connected", "reconnected"}:
                    user_stream_connected = True
                    try:
                        reconciliation = Reconciler(state, exchange).run(
                            reason="user-data-stream-connect", event_time_ms=event_time_ms
                        )
                        reconnected_consistent = reconciliation.consistent

                        def reconciliation_ok(
                            status: bool = reconnected_consistent,
                        ) -> bool:
                            return status

                        event_applier.stream_reconciled(
                            event_time_ms=event_time_ms,
                            reconcile=reconciliation_ok,
                            allow_entries=market_stream_connected and user_stream_connected,
                        )
                    except Exception:
                        event_applier.stream_reconciled(
                            event_time_ms=event_time_ms,
                            reconcile=lambda: False,
                            allow_entries=False,
                        )

        storage_ok = True
        notification_ok = True
        last_soak_incident_ms = (
            soak.gate.critical_incidents[-1][0] if soak.gate.critical_incidents else None
        )
        last_soak_regression: str | None = (
            soak.gate.critical_incidents[-1][1] if soak.gate.critical_incidents else None
        )
        dependency_last_seen: dict[str, int | None] = {
            dependency: None for dependency in candidate.market_data_dependencies
        }

        def record_soak_incident(event_time_ms: int, regression: str) -> None:
            nonlocal last_soak_incident_ms, last_soak_regression
            if regression == last_soak_regression:
                return
            if last_soak_incident_ms is not None:
                event_time_ms = max(event_time_ms, last_soak_incident_ms + 1)
            try:
                soak.record_incident(
                    event_time_ms=event_time_ms, critical=True, regression=regression
                )
                last_soak_incident_ms = event_time_ms
                last_soak_regression = regression
            except ValueError as error:
                state.set_operating_mode(OperationalMode.DEGRADED)
                state.append_audit(
                    kind="testnet-soak-incident-evidence-failed",
                    event_time_ms=event_time_ms,
                    details={"error_type": type(error).__name__},
                )

        executor_started_ms = time.time_ns() // 1_000_000
        supervision_cursor: int | None = soak.started_ms if not soak.supervised_windows else None
        if soak.supervised_windows:
            # A restart is an operational incident.  Do not bridge the gap
            # between two processes with synthetic supervised coverage.
            record_soak_incident(executor_started_ms, "executor-restart")

        def record_soak_supervision(event_time_ms: int) -> None:
            nonlocal supervision_cursor
            if supervision_cursor is None:
                supervision_cursor = event_time_ms
                return
            if event_time_ms > supervision_cursor:
                try:
                    soak.record_supervised_window(
                        start_ms=supervision_cursor,
                        end_ms=event_time_ms,
                        alerts_verified=notification_ok,
                    )
                    supervision_cursor = event_time_ms
                except ValueError as error:
                    state.set_operating_mode(OperationalMode.DEGRADED)
                    state.append_audit(
                        kind="testnet-soak-supervision-evidence-failed",
                        event_time_ms=event_time_ms,
                        details={"error_type": type(error).__name__},
                    )
            if (
                soak.continuous_started_ms is None
                and event_time_ms - soak.started_ms >= 7 * 86_400_000
            ):
                with contextlib.suppress(ValueError):
                    soak.begin_continuous(
                        now_ms=event_time_ms,
                        alerts_verified=notification_ok,
                    )

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

        last_market_receipt_ms: int | None = None
        last_clock_drift_ms = 0

        def send_critical_alert(event_time_ms: int, message: str) -> None:
            nonlocal notification_ok
            if notifier is None:
                notification_ok = False
                return
            try:
                asyncio.run(notifier.send(message, critical=True))
            except CriticalAlertFailure as error:
                notification_ok = False
                metrics.alert_failures_total.inc()
                state.append_audit(
                    kind="critical-health-alert-failed",
                    event_time_ms=event_time_ms,
                    details={"error_type": type(error).__name__},
                )

        def apply_health(now_ms: int, event_time_ms: int | None = None) -> None:
            nonlocal last_market_receipt_ms, last_clock_drift_ms
            if event_time_ms is not None:
                last_market_receipt_ms = now_ms
                last_clock_drift_ms = now_ms - event_time_ms
            freshness = None if last_market_receipt_ms is None else now_ms - last_market_receipt_ms
            try:
                persistence_ok = state.persistence_healthy()
            except Exception:
                persistence_ok = False
            prior_mode = state.operating_mode
            health_result = health.apply(
                HealthSignals(
                    market_data_freshness_ms=freshness,
                    clock_drift_ms=last_clock_drift_ms,
                    persistence_ok=persistence_ok,
                    reconciliation_ok=reconciliation.consistent,
                    storage_ok=storage_ok,
                    notification_ok=notification_ok,
                    rate_limit_blocked=state.rate_limit_blocked,
                    exchange_streams_connected=(market_stream_connected and user_stream_connected),
                    dependency_status=dependency_status(now_ms),
                ),
                event_time_ms=now_ms,
            )
            if (
                health_result.mode is OperationalMode.DEGRADED
                and prior_mode is not OperationalMode.DEGRADED
            ):
                send_critical_alert(
                    now_ms,
                    "CRITICAL scalping-bot health degraded: " + "; ".join(health_result.reasons),
                )
            if health_result.mode is OperationalMode.DEGRADED:
                record_soak_incident(
                    now_ms,
                    "health-degraded:" + ";".join(health_result.reasons),
                )

        def perform_housekeeping(now_ms: int) -> None:
            nonlocal last_backup_ms, last_heartbeat_ms, storage_ok, notification_ok
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
                    for auxiliary_path, auxiliary_name in (
                        (operator_database_path, "operator"),
                        (soak.path, "testnet-soak"),
                    ):
                        auxiliary_artifact = backup.create(
                            auxiliary_path,
                            destination=settings.state_path.parent / "backups" / auxiliary_name,
                            created_ms=now_ms,
                        )
                        backup.publish_latest(auxiliary_artifact)
                    metrics.storage_health.set(1)
                    last_backup_ms = now_ms
                    storage_ok = True
                except (OSError, ValueError) as error:
                    metrics.storage_health.set(0)
                    storage_ok = False
                    state.set_operating_mode(OperationalMode.DEGRADED)
                    send_critical_alert(now_ms, "CRITICAL scalping-bot encrypted backup failed")
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
                allow_entries=market_stream_connected and user_stream_connected,
            )

        def market_stream_disconnected(reason: str) -> None:
            market_notifications.put(("lost", reason))

        def market_stream_connected_callback() -> None:
            market_notifications.put(("connected", ""))

        def market_stream_reconnected() -> None:
            market_notifications.put(("reconnected", ""))

        def drain_market_notifications(event_time_ms: int) -> None:
            nonlocal market_stream_connected
            while True:
                try:
                    kind, reason = market_notifications.get_nowait()
                except queue.Empty:
                    return
                if kind == "lost":
                    market_stream_connected = False
                    mark_dependencies_unavailable()
                    event_applier.stream_lost(event_time_ms=event_time_ms, reason=reason)
                    send_critical_alert(
                        event_time_ms, "CRITICAL scalping-bot market stream lost: " + reason
                    )
                    record_soak_incident(event_time_ms, f"market-stream:{reason}")
                elif kind in {"connected", "reconnected"}:
                    market_stream_connected = True
                    try:
                        reconcile_after_reconnect()
                    except Exception as error:
                        state.set_operating_mode(OperationalMode.DEGRADED)
                        send_critical_alert(
                            event_time_ms,
                            "CRITICAL scalping-bot market stream reconciliation failed",
                        )
                        state.append_audit(
                            kind="market-stream-reconnect-reconciliation-failed",
                            event_time_ms=event_time_ms,
                            details={"error_type": type(error).__name__},
                        )

        def drain_operator_commands(event_time_ms: int) -> None:
            nonlocal reconciliation
            for command in operator_commands.claim_pending():
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

        def collect_market_events() -> None:
            try:
                for market_event in stream.events(
                    stop,
                    on_disconnect=market_stream_disconnected,
                    on_connect=market_stream_connected_callback,
                    on_reconnect=market_stream_reconnected,
                ):
                    market_events.put(market_event)
            finally:
                market_events.put(None)

        market_thread = threading.Thread(
            target=collect_market_events, name="binance-market-stream", daemon=True
        )
        market_thread.start()

        try:
            while not stop.is_set():
                now_ms = time.time_ns() // 1_000_000
                drain_market_notifications(now_ms)
                drain_user_events(now_ms)
                drain_operator_commands(now_ms)
                try:
                    queued_event = market_events.get(timeout=1)
                except queue.Empty:
                    now_ms = time.time_ns() // 1_000_000
                    apply_health(now_ms)
                    perform_housekeeping(now_ms)
                    continue
                if queued_event is None:
                    if not stop.is_set():
                        drain_market_notifications(now_ms)
                        market_stream_connected = False
                        mark_dependencies_unavailable()
                        event_applier.stream_lost(
                            event_time_ms=now_ms, reason="market stream stopped"
                        )
                    break
                event = queued_event
                if not isinstance(event, (CandleClosed, MarketTick)):
                    raise TypeError("Market Stream emitted a non-market event")
                now_ms = time.time_ns() // 1_000_000
                record_soak_supervision(now_ms)
                if isinstance(event, CandleClosed):
                    mark_dependency_seen("closed-1m-klines", now_ms)
                elif isinstance(event, MarketTick):
                    mark_dependency_seen("aggTrades", now_ms)
                    mark_dependency_seen("tradeTicks", now_ms)
                prior_entry_id: str | None = None
                prior_position = state.position()
                if prior_position is not None:
                    prior_entry_id = state.position_entry_client_order_id()
                apply_health(now_ms, event.event_time_ms)
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
                perform_housekeeping(now_ms)
        finally:
            stop.set()
            market_thread.join(timeout=5)
            user_thread.join(timeout=5)
            soak.close()
            operator_commands.close()
