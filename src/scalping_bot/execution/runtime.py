from __future__ import annotations

import signal
import threading
import time

from prometheus_client import start_http_server

from scalping_bot.config import Settings
from scalping_bot.execution.binance import (
    BinanceSpotTestnetAdapter,
    OfficialSpotTransport,
)
from scalping_bot.execution.reconcile import Reconciler
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.observability import OperationalMetrics


def run_executor(settings: Settings) -> None:
    if settings.binance_api_key is None or settings.binance_api_secret is None:
        raise RuntimeError("Binance Testnet credentials are required by the executor")
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    signal.signal(signal.SIGINT, lambda *_args: stop.set())
    metrics = OperationalMetrics()
    start_http_server(8000, registry=metrics.registry)
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
        reconciliation = Reconciler(state, exchange).run(
            reason="startup", event_time_ms=time.time_ns() // 1_000_000
        )
        metrics.reconciliation_divergences.set(len(reconciliation.divergences))
        metrics.sqlite_health.set(1)
        while not stop.wait(60):
            now_ms = time.time_ns() // 1_000_000
            state.append_audit(
                kind="heartbeat",
                event_time_ms=now_ms,
                details={
                    "entries_allowed": state.entries_allowed,
                    "operating_mode": state.operating_mode,
                },
            )
            metrics.heartbeat_timestamp_seconds.set(now_ms / 1000)
