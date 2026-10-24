from __future__ import annotations

import itertools
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from scalping_bot.domain.engine import Engine, StrategyPolicy
from scalping_bot.domain.events import CandleClosed, ExitFillReceived, FillReceived, MarketTick
from scalping_bot.domain.models import EntryProposal, ExecutionCosts, ExitMarket, SubmitEntry
from scalping_bot.domain.state import InMemoryState
from scalping_bot.research.candidates import CandidateBatch, first_batch
from scalping_bot.research.evidence import (
    CandidateRecord,
    EvidenceProtocol,
    EvidenceResult,
    ScalpResult,
)
from scalping_bot.research.execution import AggTrade
from scalping_bot.research.screening import AttemptLedger
from scalping_bot.strategies.breakout import BreakoutPolicy
from scalping_bot.strategies.mean_reversion import RegimeVwapPolicy
from scalping_bot.strategies.momentum import MomentumPolicy


@dataclass(frozen=True)
class BatchEvaluation:
    evidence: Mapping[str, EvidenceResult]
    ledger: AttemptLedger
    trades: Mapping[str, tuple[ScalpResult, ...]]


class ResearchBatchRunner:
    """Runs preregistered variants through the same Engine used by Testnet."""

    def __init__(
        self,
        *,
        execution_costs: ExecutionCosts,
        initial_equity: Decimal = Decimal("10000"),
    ) -> None:
        self.execution_costs = execution_costs
        self.initial_equity = initial_equity
        self.protocol = EvidenceProtocol()

    def run(
        self, trades: tuple[AggTrade, ...], batch: CandidateBatch | None = None
    ) -> BatchEvaluation:
        selected_batch = batch or first_batch()
        ledger = AttemptLedger()
        evidence: dict[str, EvidenceResult] = {}
        evidence_trades: dict[str, tuple[ScalpResult, ...]] = {}
        attempt_count = sum(
            len(_parameter_sets(candidate)) for candidate in selected_batch.candidates
        )
        for candidate in selected_batch.candidates:
            scenario_results: dict[Decimal, list[ScalpResult]] = {
                Decimal("1"): [],
                Decimal("1.5"): [],
                Decimal("2"): [],
            }
            for fold_index, fold in enumerate(self.protocol.walk_forward_folds()):
                development = tuple(
                    item
                    for item in trades
                    if fold.development[0] <= _month(item.time_ms) <= fold.development[1]
                )
                validation = tuple(
                    item
                    for item in trades
                    if fold.validation[0] <= _month(item.time_ms) <= fold.validation[1]
                )
                if not development or not validation:
                    continue
                ranked: list[tuple[Decimal, Mapping[str, object]]] = []
                for parameters in _parameter_sets(candidate):
                    development_results = self._evaluate_period(
                        candidate,
                        parameters,
                        development,
                        fold=fold_index,
                        stress=Decimal("1"),
                    )
                    expectancy = (
                        sum((trade.net_pnl for trade in development_results), Decimal(0))
                        / len(development_results)
                        if development_results
                        else Decimal(0)
                    )
                    ledger.record(
                        candidate.identity,
                        {
                            **parameters,
                            "development_period": fold.development,
                            "validation_period": fold.validation,
                        },
                        net_expectancy=expectancy,
                        baseline=Decimal(0),
                    )
                    ranked.append((expectancy, parameters))
                selected = max(ranked, key=lambda item: item[0])[1]
                for multiplier in scenario_results:
                    scenario_results[multiplier].extend(
                        self._evaluate_period(
                            candidate,
                            selected,
                            validation,
                            fold=fold_index,
                            stress=multiplier,
                        )
                    )
            scenarios = {
                multiplier: tuple(results) for multiplier, results in scenario_results.items()
            }
            worst_results = scenarios[Decimal("2")]
            evidence_trades[candidate.identity] = worst_results
            evidence[candidate.identity] = self.protocol.evaluate(
                worst_results,
                initial_equity=self.initial_equity,
                attempts=attempt_count,
                comparators=_comparators(trades, worst_results),
                cost_stress_multipliers=tuple(scenarios),
                cost_scenario_trades=scenarios,
            )
        return BatchEvaluation(evidence, ledger, evidence_trades)

    def _evaluate_period(
        self,
        candidate: CandidateRecord,
        parameters: Mapping[str, object],
        trades: tuple[AggTrade, ...],
        *,
        fold: int,
        stress: Decimal,
    ) -> tuple[ScalpResult, ...]:
        return _replay_fold(
            trades,
            policy=build_policy(candidate, parameters),
            fold=fold,
            initial_equity=self.initial_equity,
            execution_costs=self.execution_costs,
            stress=stress,
        )


def load_aggtrades(paths: tuple[Path, ...]) -> tuple[AggTrade, ...]:
    rows: list[AggTrade] = []
    for path in paths:
        table = pq.read_table(path)
        for item in table.to_pylist():
            rows.append(
                AggTrade(
                    aggregate_trade_id=int(item["aggregate_trade_id"]),
                    time_ms=int(item["time_ms"]),
                    price=Decimal(str(item["price"])),
                    quantity=Decimal(str(item["quantity"])),
                    buyer_maker=bool(item["buyer_maker"]),
                )
            )
    rows.sort(key=lambda item: (item.time_ms, item.aggregate_trade_id))
    if len({item.aggregate_trade_id for item in rows}) != len(rows):
        raise ValueError("aggregate-trade inputs overlap or contain duplicate identifiers")
    return tuple(rows)


def _replay_fold(
    trades: tuple[AggTrade, ...],
    *,
    policy: StrategyPolicy,
    fold: int,
    initial_equity: Decimal,
    execution_costs: ExecutionCosts,
    stress: Decimal,
) -> tuple[ScalpResult, ...]:
    state = InMemoryState(initial_equity=initial_equity)
    stressed_costs = ExecutionCosts(
        commission_rate=execution_costs.commission_rate * stress,
        half_spread_rate=execution_costs.half_spread_rate * stress,
        slippage_rate=execution_costs.slippage_rate * stress,
    )
    engine = Engine(state, policy, execution_costs=stressed_costs)
    completed: list[ScalpResult] = []
    open_trade: tuple[str, int, Decimal, Decimal, Decimal] | None = None
    open_marks: list[tuple[int, Decimal]] = []

    def process(event: CandleClosed | MarketTick) -> None:
        nonlocal open_trade, open_marks
        if open_trade is not None and isinstance(event, MarketTick):
            open_marks.append((event.event_time_ms, event.price))
        result = engine.handle(event)
        for command in result.commands:
            if isinstance(command, SubmitEntry):
                commission = stressed_costs.estimated_close_cost(
                    command.price_reference * command.quantity
                )
                fill = FillReceived(
                    event_id=f"sim-entry-{command.client_order_id}",
                    event_time_ms=event.event_time_ms,
                    client_order_id=command.client_order_id,
                    price=command.price_reference,
                    quantity=command.quantity,
                    commission=commission,
                    commission_asset="USDT",
                )
                engine.handle(fill)
                open_trade = (
                    command.client_order_id,
                    event.event_time_ms,
                    command.price_reference,
                    command.quantity,
                    commission,
                )
                open_marks = [(event.event_time_ms, command.price_reference)]
            elif isinstance(command, ExitMarket) and open_trade is not None:
                identity, opened_ms, entry_price, quantity, entry_cost = open_trade
                exit_price = event.price if isinstance(event, MarketTick) else event.close
                exit_cost = stressed_costs.estimated_close_cost(exit_price * quantity)
                engine.handle(
                    ExitFillReceived(
                        event_id=f"sim-exit-{command.client_order_id}",
                        event_time_ms=event.event_time_ms,
                        client_order_id=command.client_order_id,
                        price=exit_price,
                        quantity=quantity,
                        commission=exit_cost,
                    )
                )
                completed.append(
                    ScalpResult(
                        scalp_id=identity,
                        opened_ms=opened_ms,
                        closed_ms=event.event_time_ms,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        quantity=quantity,
                        gross_pnl=(exit_price - entry_price) * quantity,
                        costs=entry_cost + exit_cost,
                        fold=fold,
                        trace_event_ids=(identity, command.client_order_id),
                        equity_marks=tuple(open_marks),
                    )
                )
                open_trade = None
                open_marks = []

    for candle, candle_trades in _minute_groups(trades):
        for trade in candle_trades:
            process(
                MarketTick(
                    event_id=f"aggTrade-{trade.aggregate_trade_id}",
                    event_time_ms=trade.time_ms,
                    price=trade.price,
                )
            )
        process(candle)
    if open_trade is not None:
        last = trades[-1]
        process(
            MarketTick(
                event_id=f"force-{last.aggregate_trade_id}",
                event_time_ms=max(last.time_ms + 30 * 60_000, open_trade[1] + 30 * 60_000),
                price=last.price,
            )
        )
    return tuple(completed)


def _minute_groups(
    trades: tuple[AggTrade, ...],
) -> tuple[tuple[CandleClosed, tuple[AggTrade, ...]], ...]:
    grouped: dict[int, list[AggTrade]] = defaultdict(list)
    for trade in trades:
        grouped[trade.time_ms // 60_000].append(trade)
    output: list[tuple[CandleClosed, tuple[AggTrade, ...]]] = []
    for minute, values in sorted(grouped.items()):
        buy_volume = sum((item.quantity for item in values if not item.buyer_maker), Decimal(0))
        sell_volume = sum((item.quantity for item in values if item.buyer_maker), Decimal(0))
        prices = [item.price for item in values]
        output.append(
            (
                CandleClosed(
                    event_id=f"minute-{minute}",
                    event_time_ms=(minute + 1) * 60_000 - 1,
                    sequence=minute,
                    open=prices[0],
                    high=max(prices),
                    low=min(prices),
                    close=prices[-1],
                    complete=True,
                    volume=buy_volume + sell_volume,
                    buy_volume=buy_volume,
                    sell_volume=sell_volume,
                    trade_count=len(values),
                ),
                tuple(values),
            )
        )
    return tuple(output)


class _TradeFlowPolicy:
    def __init__(
        self,
        candidate_version: str,
        window: int,
        threshold: Decimal,
        minimum_intensity: Decimal = Decimal(0),
    ) -> None:
        self.candidate_version = candidate_version
        self.window = window
        self.threshold = threshold
        self.minimum_intensity = minimum_intensity
        self.history: list[CandleClosed] = []

    def observe(self, event: CandleClosed) -> EntryProposal | None:
        self.history.append(event)
        self.history = self.history[-self.window :]
        if len(self.history) < self.window:
            return None
        buy = sum((item.buy_volume for item in self.history), Decimal(0))
        sell = sum((item.sell_volume for item in self.history), Decimal(0))
        total = buy + sell
        imbalance = (buy - sell) / total if total else Decimal(0)
        intensity = Decimal(sum(item.trade_count for item in self.history)) / Decimal(
            self.window * 60
        )
        if (
            imbalance < self.threshold
            or intensity < self.minimum_intensity
            or event.close <= self.history[0].close
        ):
            return None
        return EntryProposal(
            candidate_version=self.candidate_version,
            source_event_id=event.event_id,
            event_time_ms=event.event_time_ms,
            direction="long",
            rationale={"condition": "price continuation confirmed by executed buyer pressure"},
            feature_snapshot={
                "executed_volume_imbalance": str(imbalance),
                "trade_intensity": str(intensity),
            },
            invalidation_price=min(item.low for item in self.history),
            expiry_ms=event.event_time_ms + 60_000,
            market_data_dependencies=("aggTrades",),
        )


class _MovingAveragePolicy:
    def __init__(self, candidate_version: str, short: int, long: int) -> None:
        self.candidate_version = candidate_version
        self.short = short
        self.long = long
        self.history: deque[CandleClosed] = deque(maxlen=long)

    def observe(self, event: CandleClosed) -> EntryProposal | None:
        self.history.append(event)
        if len(self.history) < self.long:
            return None
        candles = tuple(self.history)
        short_average = (
            sum((item.close for item in candles[-self.short :]), Decimal(0)) / self.short
        )
        long_average = sum((item.close for item in candles), Decimal(0)) / self.long
        if short_average <= long_average:
            return None
        return EntryProposal(
            candidate_version=self.candidate_version,
            source_event_id=event.event_id,
            event_time_ms=event.event_time_ms,
            direction="long",
            rationale={"condition": "short completed-bar average above long average"},
            feature_snapshot={
                "short_average": str(short_average),
                "long_average": str(long_average),
            },
            invalidation_price=min(item.low for item in candles),
            expiry_ms=event.event_time_ms + 60_000,
            market_data_dependencies=("closed-1m-klines",),
        )


class _VolatilityBreakoutPolicy:
    def __init__(self, candidate_version: str, lookback: int, multiplier: Decimal) -> None:
        self.candidate_version = candidate_version
        self.lookback = lookback
        self.multiplier = multiplier
        self.history: deque[CandleClosed] = deque(maxlen=lookback)

    def observe(self, event: CandleClosed) -> EntryProposal | None:
        if len(self.history) < self.lookback:
            self.history.append(event)
            return None
        history = tuple(self.history)
        ranges = sorted(item.high - item.low for item in history)
        median_range = ranges[len(ranges) // 2]
        prior_high = max(item.high for item in history)
        prior_low = min(item.low for item in history)
        self.history.append(event)
        if event.close <= prior_high or event.high - event.low < median_range * self.multiplier:
            return None
        return EntryProposal(
            candidate_version=self.candidate_version,
            source_event_id=event.event_id,
            event_time_ms=event.event_time_ms,
            direction="long",
            rationale={"condition": "range break confirmed by true-range expansion"},
            feature_snapshot={"median_range": str(median_range), "prior_high": str(prior_high)},
            invalidation_price=prior_low,
            expiry_ms=event.event_time_ms + 60_000,
            market_data_dependencies=("closed-1m-klines",),
        )


def build_policy(candidate: CandidateRecord, parameters: Mapping[str, object]) -> StrategyPolicy:
    if candidate.family == "momentum":
        if "long_window" in parameters:
            return _MovingAveragePolicy(
                candidate.identity,
                short=int(str(parameters["short_window"])),
                long=int(str(parameters["long_window"])),
            )
        window = int(str(parameters.get("window_bars", parameters.get("short_window", 3))))
        threshold = Decimal(str(parameters.get("threshold", "0.005")))
        return MomentumPolicy(
            candidate_version=candidate.identity, window=window, threshold=threshold
        )
    if candidate.family == "volatility-breakout":
        lookback = int(str(parameters["lookback"]))
        if "buffer_bps" in parameters:
            buffer = Decimal(str(parameters["buffer_bps"])) / Decimal("10000")
        else:
            return _VolatilityBreakoutPolicy(
                candidate.identity,
                lookback,
                Decimal(str(parameters["range_multiplier"])),
            )
        return BreakoutPolicy(
            candidate_version=candidate.identity, lookback=lookback, buffer=buffer
        )
    if candidate.family == "regime-conditioned-mean-reversion":
        return RegimeVwapPolicy(
            candidate_version=candidate.identity,
            window=int(str(parameters["window"])),
            max_efficiency_ratio=Decimal("0.40"),
            deviation=Decimal(str(parameters["deviation"])),
        )
    window = int(str(parameters.get("window_seconds", 10)))
    threshold = Decimal(str(parameters.get("minimum_delta", "0")))
    minimum_intensity = Decimal(str(parameters.get("minimum_intensity", "0")))
    return _TradeFlowPolicy(
        candidate.identity,
        max(2, window // 5),
        threshold,
        minimum_intensity,
    )


def _parameter_sets(candidate: CandidateRecord) -> tuple[dict[str, object], ...]:
    names = tuple(candidate.parameters)
    values = tuple(
        value if isinstance(value, list) else [value] for value in candidate.parameters.values()
    )
    return tuple(
        dict(zip(names, combination, strict=True)) for combination in itertools.product(*values)
    )


def _comparators(
    trades: tuple[AggTrade, ...], scalps: tuple[ScalpResult, ...]
) -> Mapping[str, Decimal | None]:
    del scalps
    buy_hold = (trades[-1].price - trades[0].price) / trades[0].price if len(trades) > 1 else None
    momentum_returns = [
        (trades[index + 1].price - trades[index].price) / trades[index].price
        for index in range(1, len(trades) - 1)
        if trades[index].price > trades[index - 1].price
    ]
    return {
        "no_trade": Decimal(0),
        "buy_and_hold": buy_hold,
        "momentum": (
            sum(momentum_returns, Decimal(0)) / len(momentum_returns) if momentum_returns else None
        ),
    }


def _month(time_ms: int) -> str:
    return datetime.fromtimestamp(time_ms / 1000, tz=UTC).strftime("%Y-%m")
