"""Production-market Live Shadow adapters using the shared deterministic engine."""

from __future__ import annotations

import json
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
from scalping_bot.domain.market_context import MarketContext
from scalping_bot.domain.market_context import size_bucket as notional_size_bucket
from scalping_bot.domain.models import ExitMarket, SubmitEntry
from scalping_bot.domain.state import InMemoryState
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.execution.live import _parse_market_message
from scalping_bot.operations.durable_shadow import DurableLiveShadow
from scalping_bot.operations.observability import OperationalMetrics
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
        metrics: OperationalMetrics | None = None,
    ) -> None:
        candidate = PromotedCandidateManifest.load(candidate_manifest)
        snapshot = ExecutionCostSnapshot.load(cost_model)
        if candidate.execution_cost_model_identity != snapshot.snapshot_id:
            raise ValueError("Live Shadow candidate and cost model identities do not match")
        costs = snapshot.as_engine_costs()
        self.process = process
        self.metrics = metrics
        self.live = Engine(
            InMemoryState(initial_equity=initial_equity), candidate.policy(), execution_costs=costs
        )
        self.replay = Engine(
            InMemoryState(initial_equity=initial_equity), candidate.policy(), execution_costs=costs
        )
        self._replay_adapter = ReplayEventAdapter()
        self._live_candles: dict[str, _LaneCandle] = {}
        self._replay_candles: dict[str, _LaneCandle] = {}
        self._lane_context: dict[str, tuple[str | None, str | None]] = {}
        self._lane_notional: dict[str, Decimal | None] = {"live": None, "replay": None}

    def process_event(self, event: EngineEvent, *, receipt_time_ms: int) -> None:
        """Process one raw event through live and independently reconstructed lanes."""
        self.process_live_event(event, receipt_time_ms=receipt_time_ms)
        self.process_replay_event(
            self._replay_adapter.adapt(event), receipt_time_ms=receipt_time_ms
        )
        self._publish_metrics(receipt_time_ms)

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
        if isinstance(event, MarketTick):
            self._lane_context[lane] = (event.volatility_bucket, event.size_bucket)
        result = engine.handle(event)
        if lane == "live":
            for decision in result.decisions:
                self.process.record_hypothetical_decision(
                    event_time_ms=decision.event_time_ms,
                    kind=f"decision:{decision.kind}",
                )
            for command in result.commands:
                self.process.record_hypothetical_decision(
                    event_time_ms=event.event_time_ms,
                    kind=f"hypothetical:{type(command).__name__}",
                )
        for command in result.commands:
            if isinstance(command, SubmitEntry):
                self._lane_notional[lane] = command.price_reference * command.quantity
            elif isinstance(command, ExitMarket) and isinstance(event, MarketTick):
                self._lane_notional[lane] = event.price * command.quantity
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
                engine,
                event_time_ms=event.event_time_ms,
                volatility_bucket=self._lane_context.get(lane, (None, None))[0],
                notional=self._lane_notional.get(lane),
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

    def _publish_metrics(self, now_ms: int) -> None:
        if self.metrics is None:
            return
        status = self.process.health_status(now_ms=now_ms)
        freshness = status.get("freshness_ms")
        if isinstance(freshness, int):
            self.metrics.market_data_freshness_ms.set(freshness)
        self.metrics.market_data_gaps_total.set(float(bool(status.get("continuity_broken"))))
        self.metrics.entries_allowed.set(0)
        self.metrics.operating_mode.set(1 if bool(status.get("healthy")) else 2)
        self.metrics.heartbeat_timestamp_seconds.set(now_ms / 1000)


class ProductionLiveShadowStream:
    """One reconnectable public stream fanned out into two no-order lanes."""

    def __init__(self, runner: SharedEngineLiveShadow, *, endpoint: str = PRODUCTION_SHADOW_STREAM):
        if endpoint != PRODUCTION_SHADOW_STREAM:
            raise ValueError("Live Shadow accepts only the Binance public production stream")
        self.runner = runner
        self.endpoint = endpoint

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                context = MarketContext()
                with connect(self.endpoint, open_timeout=10, close_timeout=5) as websocket:
                    while not stop.is_set():
                        try:
                            message = websocket.recv(timeout=1)
                        except TimeoutError:
                            continue
                        if not isinstance(message, str):
                            continue
                        event = _parse_market_message(message, context=context)
                        if event is not None:
                            # SharedEngineLiveShadow reconstructs the replay event from
                            # this exact immutable input, so both lanes observe the same
                            # exchange event and receipt timestamp.
                            self.runner.process_event(
                                event, receipt_time_ms=time.time_ns() // 1_000_000
                            )
                if stop.is_set():
                    return
                raise RuntimeError("Live Shadow websocket closed cleanly")
            except Exception as error:
                if stop.is_set():
                    return
                self.record_missing_dependency(
                    dependency="production-stream",
                    event_time_ms=time.time_ns() // 1_000_000,
                )
                self.runner.process.mark_continuity_broken(
                    f"production-stream:{type(error).__name__}"
                )
                stop.wait(1)

    def record_missing_dependency(self, *, dependency: str, event_time_ms: int) -> None:
        """Persist a degraded observation while keeping evidence chronological."""
        self.runner.process.record_missing_dependency(
            event_time_ms=event_time_ms,
            dependency=dependency,
        )
        self.runner._publish_metrics(event_time_ms)


def parse_shadow_event(
    value: dict[str, object], *, context: MarketContext | None = None
) -> EngineEvent:
    """Parse one raw Binance production event for deterministic file replay."""
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    event = _parse_market_message(raw, context=context)
    if event is None:
        raise ValueError("shadow event must be a complete trade or one-minute kline")
    return event


def _hypothetical_cost(
    engine: Engine,
    *,
    event_time_ms: int,
    volatility_bucket: str | None,
    notional: Decimal | None,
) -> str:
    if notional is None or volatility_bucket is None:
        return "non-evaluable"
    try:
        return str(
            engine.execution_costs.estimated_close_cost(
                notional,
                event_time_ms=event_time_ms,
                volatility_bucket=volatility_bucket,
                size_bucket=notional_size_bucket(notional),
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
