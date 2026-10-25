"""Production-market Live Shadow adapter using the shared deterministic engine."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from scalping_bot.domain.costs import ExecutionCostSnapshot
from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import CandleClosed, EngineEvent
from scalping_bot.domain.state import InMemoryState
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.execution.live import _parse_market_message
from scalping_bot.operations.durable_shadow import DurableLiveShadow
from scalping_bot.operations.promotion import LiveReplayComparison

PRODUCTION_SHADOW_STREAM = (
    "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@kline_1m"
)


@dataclass(frozen=True)
class _DecisionValues:
    signal: bool
    features: dict[str, str]
    expiry_ms: int | None


class SharedEngineLiveShadow:
    """Feed live observations through two no-order instances of the Engine."""

    def __init__(
        self,
        process: DurableLiveShadow,
        *,
        candidate_manifest: Path,
        cost_model: Path,
        initial_equity: Decimal = Decimal("10000"),
    ) -> None:
        candidate = PromotedCandidateManifest.load(candidate_manifest)
        snapshot = ExecutionCostSnapshot.load(cost_model)
        costs = snapshot.as_engine_costs()
        self.process = process
        self.live = Engine(
            InMemoryState(initial_equity=initial_equity), candidate.policy(), execution_costs=costs
        )
        self.replay = Engine(
            InMemoryState(initial_equity=initial_equity), candidate.policy(), execution_costs=costs
        )

    def process_event(self, event: EngineEvent, *, receipt_time_ms: int) -> None:
        live_result = self.live.handle(event)
        replay_result = self.replay.handle(event)
        # SubmitEntry commands are deliberately discarded. The two engines are
        # still exercised, but no exchange adapter is constructed or called.
        self.live.state.pending_entry = None
        self.replay.state.pending_entry = None
        if not isinstance(event, CandleClosed):
            return
        live = _decision_values(live_result.decisions)
        replay = _decision_values(replay_result.decisions)
        divergence = _divergence(live, replay)
        comparison = LiveReplayComparison(
            event_time_ms=event.event_time_ms,
            live_features=live.features,
            replay_features=replay.features,
            live_signal=live.signal,
            replay_signal=replay.signal,
            expiry_match=live.expiry_ms == replay.expiry_ms,
            latency_ms=max(0, receipt_time_ms - event.event_time_ms),
            data_quality="valid" if event.complete else "invalid",
            hypothetical_cost=str(self.live.execution_costs.estimated_close_cost(Decimal("1000"))),
            divergence=divergence,
            replay_hypothetical_cost=str(
                self.replay.execution_costs.estimated_close_cost(Decimal("1000"))
            ),
            replay_data_quality="valid" if event.complete else "invalid",
            replay_latency_ms=0,
            live_expiry_ms=live.expiry_ms,
            replay_expiry_ms=replay.expiry_ms,
        )
        self.process.record_comparison(comparison)


class ProductionLiveShadowStream:
    """Reconnectable public production stream; it never has order methods."""

    def __init__(self, runner: SharedEngineLiveShadow, *, endpoint: str = PRODUCTION_SHADOW_STREAM):
        if endpoint != PRODUCTION_SHADOW_STREAM:
            raise ValueError("Live Shadow accepts only the Binance public production stream")
        self.runner = runner
        self.endpoint = endpoint

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                with connect(self.endpoint, open_timeout=10, close_timeout=5) as websocket:
                    while not stop.is_set():
                        try:
                            message = websocket.recv(timeout=1)
                        except TimeoutError:
                            continue
                        if not isinstance(message, str):
                            continue
                        event = _parse_market_message(message)
                        if event is not None:
                            self.runner.process_event(
                                event, receipt_time_ms=time.time_ns() // 1_000_000
                            )
            except (ConnectionClosed, OSError, ValueError, json.JSONDecodeError) as error:
                self.runner.process.mark_continuity_broken(type(error).__name__)
                stop.wait(1)


def parse_shadow_event(value: dict[str, object]) -> EngineEvent:
    """Parse one raw Binance production event for deterministic file replay."""
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    event = _parse_market_message(raw)
    if event is None:
        raise ValueError("shadow event must be a complete trade or one-minute kline")
    return event


def _decision_values(decisions: tuple[object, ...]) -> _DecisionValues:
    signal = False
    features: dict[str, str] = {}
    expiry_ms: int | None = None
    for decision in decisions:
        kind = getattr(decision, "kind", "")
        details = getattr(decision, "details", {})
        if kind in {"entry-proposed", "entry-submitted"}:
            signal = True
        if kind == "entry-proposed" and isinstance(details, dict):
            raw_features = details.get("features", {})
            if isinstance(raw_features, dict):
                features = {str(key): str(value) for key, value in raw_features.items()}
            if details.get("expiry_ms") is not None:
                expiry_ms = int(str(details["expiry_ms"]))
    return _DecisionValues(signal=signal, features=features, expiry_ms=expiry_ms)


def _divergence(live: _DecisionValues, replay: _DecisionValues) -> str | None:
    if live.features != replay.features:
        return "feature-values"
    if live.signal != replay.signal:
        return "signal"
    if live.expiry_ms != replay.expiry_ms:
        return "expiry"
    return None
