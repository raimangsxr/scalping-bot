"""Production-market Live Shadow adapters using the shared deterministic engine."""

from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from websockets.sync.client import connect

from scalping_bot.domain.costs import ExecutionCostSnapshot
from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import (
    CandleClosed,
    EngineEvent,
    EntryFillUpdated,
    ExitFillReceived,
    FillReceived,
    MarketTick,
    OperatorConfirmedNewDay,
)
from scalping_bot.domain.state import InMemoryState
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.execution.live import _parse_market_message
from scalping_bot.operations.durable_shadow import DurableLiveShadow
from scalping_bot.operations.promotion import LiveReplayComparison

PRODUCTION_SHADOW_STREAM = (
    "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m"
)


@dataclass(frozen=True)
class _DecisionValues:
    signal: bool
    features: dict[str, str]
    expiry_ms: int | None


@dataclass(frozen=True)
class _LaneCandle:
    event_time_ms: int
    complete: bool
    values: _DecisionValues
    receipt_time_ms: int
    hypothetical_cost: str


class ReplayEventAdapter:
    """Reconstruct a fresh domain event for the independent replay lane."""

    def adapt(self, event: EngineEvent) -> EngineEvent:
        if isinstance(event, CandleClosed):
            return CandleClosed(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                sequence=event.sequence,
                open=event.open,
                high=event.high,
                low=event.low,
                close=event.close,
                complete=event.complete,
                volume=event.volume,
                buy_volume=event.buy_volume,
                sell_volume=event.sell_volume,
                trade_count=event.trade_count,
            )
        if isinstance(event, MarketTick):
            return MarketTick(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                price=event.price,
                volatility_bucket=event.volatility_bucket,
                size_bucket=event.size_bucket,
            )
        if isinstance(event, FillReceived):
            return FillReceived(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                client_order_id=event.client_order_id,
                price=event.price,
                quantity=event.quantity,
                commission=event.commission,
                commission_asset=event.commission_asset,
            )
        if isinstance(event, EntryFillUpdated):
            return EntryFillUpdated(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                client_order_id=event.client_order_id,
                price=event.price,
                cumulative_quantity=event.cumulative_quantity,
                cumulative_commission=event.cumulative_commission,
                commission_asset=event.commission_asset,
            )
        if isinstance(event, ExitFillReceived):
            return ExitFillReceived(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                client_order_id=event.client_order_id,
                price=event.price,
                quantity=event.quantity,
                commission=event.commission,
            )
        if isinstance(event, OperatorConfirmedNewDay):
            return OperatorConfirmedNewDay(
                event_id=event.event_id,
                event_time_ms=event.event_time_ms,
                utc_day=event.utc_day,
            )
        raise TypeError(f"unsupported shadow event: {type(event).__name__}")


class SharedEngineLiveShadow:
    """Compare independent live and replay lanes without an exchange adapter."""

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
        self._replay_adapter = ReplayEventAdapter()
        self._live_candles: dict[str, _LaneCandle] = {}
        self._replay_candles: dict[str, _LaneCandle] = {}

    def process_event(self, event: EngineEvent, *, receipt_time_ms: int) -> None:
        """Process one raw event through live and independently reconstructed lanes."""
        self.process_live_event(event, receipt_time_ms=receipt_time_ms)
        self.process_replay_event(
            self._replay_adapter.adapt(event), receipt_time_ms=receipt_time_ms
        )

    def process_live_event(self, event: EngineEvent, *, receipt_time_ms: int) -> None:
        self._process_lane(
            "live", self.live, self._live_candles, event, receipt_time_ms=receipt_time_ms
        )

    def process_replay_event(self, event: EngineEvent, *, receipt_time_ms: int) -> None:
        self._process_lane(
            "replay", self.replay, self._replay_candles, event, receipt_time_ms=receipt_time_ms
        )

    def _process_lane(
        self,
        lane: str,
        engine: Engine,
        candles: dict[str, _LaneCandle],
        event: EngineEvent,
        *,
        receipt_time_ms: int,
    ) -> None:
        result = engine.handle(event)
        # SubmitEntry and all other commands are hypothetical. No exchange
        # adapter is constructed or called by either lane.
        engine.state.pending_entry = None
        engine.state.pending_exit = None
        if not isinstance(event, CandleClosed):
            return
        candles[event.event_id] = _LaneCandle(
            event_time_ms=event.event_time_ms,
            complete=event.complete,
            values=_decision_values(result.decisions),
            receipt_time_ms=receipt_time_ms,
            hypothetical_cost=_hypothetical_cost(
                engine, event_time_ms=event.event_time_ms, volatility_bucket=None
            ),
        )
        self._record_if_aligned(event.event_id)
        if len(self._live_candles) + len(self._replay_candles) > 200:
            self.process.mark_continuity_broken("live/replay event alignment gap")
            self._live_candles.clear()
            self._replay_candles.clear()

    def _record_if_aligned(self, event_id: str) -> None:
        live = self._live_candles.get(event_id)
        replay = self._replay_candles.get(event_id)
        if live is None or replay is None:
            return
        del self._live_candles[event_id]
        del self._replay_candles[event_id]
        divergence = _divergence(live.values, replay.values)
        if live.event_time_ms != replay.event_time_ms:
            divergence = divergence or "event-time"
        live_quality = "valid" if live.complete else "invalid"
        replay_quality = "valid" if replay.complete else "invalid"
        if live.hypothetical_cost == "non-evaluable":
            live_quality = "missing:execution-cost-context"
        if replay.hypothetical_cost == "non-evaluable":
            replay_quality = "missing:execution-cost-context"
        self.process.record_comparison(
            LiveReplayComparison(
                event_time_ms=live.event_time_ms,
                live_features=live.values.features,
                replay_features=replay.values.features,
                live_signal=live.values.signal,
                replay_signal=replay.values.signal,
                expiry_match=live.values.expiry_ms == replay.values.expiry_ms,
                latency_ms=max(0, live.receipt_time_ms - live.event_time_ms),
                data_quality=live_quality,
                hypothetical_cost=live.hypothetical_cost,
                divergence=divergence,
                replay_hypothetical_cost=replay.hypothetical_cost,
                replay_data_quality=replay_quality,
                replay_latency_ms=max(0, replay.receipt_time_ms - replay.event_time_ms),
                live_expiry_ms=live.values.expiry_ms,
                replay_expiry_ms=replay.values.expiry_ms,
            )
        )


class ProductionLiveShadowStream:
    """Two reconnectable public streams feeding separate no-order lanes."""

    def __init__(self, runner: SharedEngineLiveShadow, *, endpoint: str = PRODUCTION_SHADOW_STREAM):
        if endpoint != PRODUCTION_SHADOW_STREAM:
            raise ValueError("Live Shadow accepts only the Binance public production stream")
        self.runner = runner
        self.endpoint = endpoint

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            connection_stop = threading.Event()
            messages: queue.Queue[tuple[str, str, object, int]] = queue.Queue()
            workers = tuple(
                threading.Thread(
                    target=self._consume,
                    args=(lane, messages, stop, connection_stop),
                    name=f"live-shadow-{lane}",
                    daemon=True,
                )
                for lane in ("live", "replay")
            )
            for worker in workers:
                worker.start()
            failure: tuple[str, object] | None = None
            while not stop.is_set() and not connection_stop.is_set():
                try:
                    kind, lane, payload, receipt_time_ms = messages.get(timeout=1)
                except queue.Empty:
                    continue
                if kind == "error":
                    failure = (lane, payload)
                    connection_stop.set()
                    break
                if not isinstance(payload, (CandleClosed, MarketTick)):
                    continue
                if lane == "live":
                    self.runner.process_live_event(payload, receipt_time_ms=receipt_time_ms)
                else:
                    self.runner.process_replay_event(payload, receipt_time_ms=receipt_time_ms)
            connection_stop.set()
            for worker in workers:
                worker.join(timeout=5)
            if failure is not None and not stop.is_set():
                lane, error = failure
                self.runner.process.mark_continuity_broken(f"{lane}-stream:{type(error).__name__}")
                stop.wait(1)

    def _consume(
        self,
        lane: str,
        messages: queue.Queue[tuple[str, str, object, int]],
        stop: threading.Event,
        connection_stop: threading.Event,
    ) -> None:
        try:
            with connect(self.endpoint, open_timeout=10, close_timeout=5) as websocket:
                while not stop.is_set() and not connection_stop.is_set():
                    try:
                        message = websocket.recv(timeout=1)
                    except TimeoutError:
                        continue
                    if not isinstance(message, str):
                        continue
                    event = _parse_market_message(message)
                    if event is not None:
                        messages.put(("event", lane, event, time.time_ns() // 1_000_000))
        except Exception as error:
            if not stop.is_set() and not connection_stop.is_set():
                messages.put(("error", lane, error, time.time_ns() // 1_000_000))


def parse_shadow_event(value: dict[str, object]) -> EngineEvent:
    """Parse one raw Binance production event for deterministic file replay."""
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    event = _parse_market_message(raw)
    if event is None:
        raise ValueError("shadow event must be a complete trade or one-minute kline")
    return event


def _hypothetical_cost(engine: Engine, *, event_time_ms: int, volatility_bucket: str | None) -> str:
    try:
        return str(
            engine.execution_costs.estimated_close_cost(
                Decimal("1000"),
                event_time_ms=event_time_ms,
                volatility_bucket=volatility_bucket,
            )
        )
    except ValueError:
        return "non-evaluable"


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
