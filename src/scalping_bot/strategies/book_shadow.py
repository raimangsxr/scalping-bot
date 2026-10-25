import gzip
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from statistics import pstdev
from typing import Any

from scalping_bot.data.book import BookGap, LocalOrderBook
from scalping_bot.data.capture import CaptureLedger
from scalping_bot.data.storage import ObjectStore
from scalping_bot.research.features import (
    BookFlowFeatures,
    BookFlowInput,
    TradePrint,
    book_flow_features,
)


@dataclass(frozen=True)
class ShadowDecision:
    hypothetical_allow: bool
    executable: bool = False
    reason: str = "Book Flow is restricted to Shadow Evaluation"


@dataclass(frozen=True)
class ShadowBookConfirmation:
    min_imbalance: Decimal

    def evaluate(self, features: BookFlowFeatures) -> ShadowDecision:
        allow = (
            features.valid
            and features.market_data_quality == "valid"
            and features.segment_id is not None
            and features.best_quote_imbalance >= self.min_imbalance
            and features.depth_imbalance >= self.min_imbalance
            and features.liquidity_change_confirmed_by_trades
        )
        return ShadowDecision(hypothetical_allow=allow)


DEFAULT_BOOK_CONFIRMATION_IMBALANCE = Decimal("0.10")


@dataclass(frozen=True)
class BookShadowObservation:
    event_time_ms: int
    features: BookFlowFeatures
    baseline_entry: bool
    hypothetical_entry: bool


@dataclass(frozen=True)
class BookShadowReport:
    observations: int
    valid_observations: int
    coverage: Decimal
    gaps: int
    feature_stability: Decimal | None
    hypothetical_entries: int
    blocked_entries: int
    promoted: bool = False
    incremental_shadow_entries: int = 0
    incremental_shadow_effect: Decimal | None = None

    @property
    def markdown(self) -> str:
        stability = (
            "non-evaluable" if self.feature_stability is None else str(self.feature_stability)
        )
        effect = (
            "non-evaluable"
            if self.incremental_shadow_effect is None
            else str(self.incremental_shadow_effect)
        )
        return (
            "# Book Flow Shadow report\n\n"
            f"Observations: {self.observations}\n"
            f"Valid Book Segment coverage: {self.coverage}\n"
            f"Gaps: {self.gaps}\n"
            f"Feature stability: {stability}\n"
            f"Hypothetical entries: {self.hypothetical_entries}\n"
            f"Entries blocked by Book Flow: {self.blocked_entries}\n"
            f"Incremental shadow effect: {self.incremental_shadow_entries} entries; {effect}\n"
            "Promotion: prohibited; Book Flow remains Shadow Evaluation.\n"
        )


class BookShadowEvaluator:
    """Collect prospective Book Flow observations without an executable path."""

    def __init__(self, confirmation: ShadowBookConfirmation | None = None) -> None:
        self.confirmation = confirmation
        self._observations: list[BookShadowObservation] = []
        self._last_event_time_ms: int | None = None
        self._gaps = 0

    @classmethod
    def from_capture(
        cls,
        store: ObjectStore,
        *,
        depth_stream: str = "btcusdt-depth-100ms",
        trade_stream: str = "btcusdt-aggTrade",
        confirmation: ShadowBookConfirmation | None = None,
        entry_proposal: Callable[[int, BookFlowFeatures], bool] | None = None,
    ) -> "BookShadowEvaluator":
        """Derive prospective observations directly from immutable capture segments."""
        evaluator = cls(
            confirmation
            or ShadowBookConfirmation(min_imbalance=DEFAULT_BOOK_CONFIRMATION_IMBALANCE)
        )
        ledger = CaptureLedger(store)
        depth_catalog = ledger.catalog(depth_stream)
        trade_catalog = ledger.catalog(trade_stream)
        trades = _captured_trades(store, trade_catalog)
        trade_index = 0
        recent: list[TradePrint] = []
        book = LocalOrderBook("BTCUSDT")
        previous_bid_depth = Decimal(0)
        previous_ask_depth = Decimal(0)
        for entry in depth_catalog.entries:
            segment = entry.segment
            if segment.gap_before_ms is not None:
                evaluator.record_gap()
                book = LocalOrderBook("BTCUSDT")
                previous_bid_depth = Decimal(0)
                previous_ask_depth = Decimal(0)
            if not segment.valid:
                evaluator.record_gap()
                book = LocalOrderBook("BTCUSDT")
                previous_bid_depth = Decimal(0)
                previous_ask_depth = Decimal(0)
                continue
            for item in _segment_events(store, segment.object_key):
                event_type = str(item.get("event_type", ""))
                payload = item.get("payload")
                if not isinstance(payload, dict):
                    evaluator.record_gap()
                    continue
                event_time_ms = int(str(item.get("exchange_time_ms", segment.started_ms)))
                if event_type == "depth-snapshot":
                    try:
                        book.apply_snapshot(
                            last_update_id=int(payload["lastUpdateId"]),
                            bids=_levels(payload["bids"]),
                            asks=_levels(payload["asks"]),
                            exchange_time_ms=event_time_ms,
                            receipt_time_ms=int(str(item["receipt_time_ms"])),
                        )
                        previous_bid_depth = book.bid_depth
                        previous_ask_depth = book.ask_depth
                    except (BookGap, KeyError, TypeError, ValueError):
                        evaluator.record_gap()
                    continue
                if "depth" not in event_type:
                    continue
                try:
                    book.apply_depth(
                        first_update_id=int(payload["U"]),
                        final_update_id=int(payload["u"]),
                        bids=_levels(payload["b"]),
                        asks=_levels(payload["a"]),
                        exchange_time_ms=event_time_ms,
                        receipt_time_ms=int(str(item["receipt_time_ms"])),
                    )
                except (BookGap, KeyError, TypeError, ValueError):
                    evaluator.record_gap()
                    continue
                while trade_index < len(trades) and trades[trade_index].time_ms <= event_time_ms:
                    recent.append(trades[trade_index])
                    trade_index += 1
                recent = [trade for trade in recent if event_time_ms - trade.time_ms <= 60_000]
                buy = sum((trade.quantity for trade in recent if not trade.buyer_maker), Decimal(0))
                sell = sum((trade.quantity for trade in recent if trade.buyer_maker), Decimal(0))
                current_bid_depth = book.bid_depth
                current_ask_depth = book.ask_depth
                features = book_flow_features(
                    BookFlowInput(
                        valid_segment=True,
                        best_bid_quantity=book.best_bid[1],
                        best_ask_quantity=book.best_ask[1],
                        bid_depth=current_bid_depth,
                        ask_depth=current_ask_depth,
                        previous_bid_depth=previous_bid_depth,
                        previous_ask_depth=previous_ask_depth,
                        executed_buy_volume=buy,
                        executed_sell_volume=sell,
                        segment_id=book.segment_id,
                    )
                )
                proposal = (
                    entry_proposal(event_time_ms, features) if entry_proposal is not None else False
                )
                evaluator.record(
                    # Book Flow may only confirm or block a proposal supplied by
                    # the frozen candidate evaluation.  It never invents the
                    # baseline signal from trade-flow heuristics.
                    BookShadowObservation(
                        event_time_ms=event_time_ms,
                        features=features,
                        baseline_entry=proposal,
                        hypothetical_entry=proposal,
                    )
                )
                previous_bid_depth = current_bid_depth
                previous_ask_depth = current_ask_depth
        return evaluator

    def record(self, observation: BookShadowObservation) -> ShadowDecision:
        if (
            self._last_event_time_ms is not None
            and observation.event_time_ms <= self._last_event_time_ms
        ):
            raise ValueError("Book Flow shadow observations must be chronological")
        if (
            self._last_event_time_ms is not None
            and observation.event_time_ms - self._last_event_time_ms > 60_000
        ):
            self._gaps += 1
        self._last_event_time_ms = observation.event_time_ms
        confirmation = (
            self.confirmation.evaluate(observation.features)
            if self.confirmation is not None
            else ShadowDecision(
                hypothetical_allow=(
                    observation.hypothetical_entry
                    and observation.features.valid
                    and observation.features.market_data_quality == "valid"
                    and observation.features.segment_id is not None
                )
            )
        )
        self._observations.append(
            BookShadowObservation(
                event_time_ms=observation.event_time_ms,
                features=observation.features,
                baseline_entry=observation.baseline_entry,
                hypothetical_entry=(
                    observation.hypothetical_entry and confirmation.hypothetical_allow
                ),
            )
        )
        return ShadowDecision(
            hypothetical_allow=(observation.hypothetical_entry and confirmation.hypothetical_allow)
        )

    def record_gap(self) -> None:
        """Record a sequence gap that invalidates the current Book Flow segment."""
        self._gaps += 1

    def report(self) -> BookShadowReport:
        valid = [
            item
            for item in self._observations
            if item.features.valid
            and item.features.market_data_quality == "valid"
            and item.features.segment_id is not None
        ]
        imbalances = [
            float(item.features.depth_imbalance)
            for item in valid
            if item.features.market_data_quality == "valid"
        ]
        baseline_entries = sum(item.baseline_entry for item in valid)
        hypothetical_entries = sum(item.hypothetical_entry for item in valid)
        incremental_entries = hypothetical_entries - baseline_entries
        return BookShadowReport(
            observations=len(self._observations),
            valid_observations=len(valid),
            coverage=(
                Decimal(len(valid)) / Decimal(len(self._observations))
                if self._observations
                else Decimal(0)
            ),
            gaps=self._gaps,
            feature_stability=(Decimal(str(pstdev(imbalances))) if len(imbalances) > 1 else None),
            hypothetical_entries=hypothetical_entries,
            blocked_entries=max(0, baseline_entries - hypothetical_entries),
            incremental_shadow_entries=incremental_entries,
            incremental_shadow_effect=(
                Decimal(incremental_entries) / Decimal(baseline_entries)
                if baseline_entries
                else None
            ),
        )

    @property
    def observations(self) -> tuple[BookShadowObservation, ...]:
        return tuple(self._observations)

    def persist_report(self, store: ObjectStore) -> str:
        report = self.report()
        payload = {
            "schema_version": 1,
            "report": {
                "observations": report.observations,
                "valid_observations": report.valid_observations,
                "coverage": str(report.coverage),
                "gaps": report.gaps,
                "feature_stability": (
                    str(report.feature_stability) if report.feature_stability is not None else None
                ),
                "hypothetical_entries": report.hypothetical_entries,
                "blocked_entries": report.blocked_entries,
                "incremental_shadow_entries": report.incremental_shadow_entries,
                "incremental_shadow_effect": (
                    str(report.incremental_shadow_effect)
                    if report.incremental_shadow_effect is not None
                    else None
                ),
                "promoted": False,
            },
            "observations": [
                {
                    "event_time_ms": item.event_time_ms,
                    "baseline_entry": item.baseline_entry,
                    "hypothetical_entry": item.hypothetical_entry,
                    "valid": item.features.valid,
                    "market_data_quality": item.features.market_data_quality,
                    "segment_id": item.features.segment_id,
                    "depth_imbalance": str(item.features.depth_imbalance),
                    "best_quote_imbalance": str(item.features.best_quote_imbalance),
                }
                for item in self._observations
            ],
            "classification": "Shadow Evaluation",
            "markdown": report.markdown,
        }
        content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        key = f"shadow/book-flow/{hashlib.sha256(content).hexdigest()}.json"
        store.put_immutable(key, content)
        return key


def _segment_events(store: ObjectStore, key: str) -> tuple[dict[str, Any], ...]:
    payload = gzip.decompress(store.read(key))
    events: list[dict[str, Any]] = []
    for line in payload.splitlines():
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError("captured segment contains a non-object event")
        events.append(value)
    return tuple(events)


def _levels(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        raise ValueError("captured book levels are malformed")
    levels: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            raise ValueError("captured book level is malformed")
        levels.append((str(item[0]), str(item[1])))
    return tuple(levels)


def _captured_trades(store: ObjectStore, catalog: object) -> tuple[TradePrint, ...]:
    entries = getattr(catalog, "entries", ())
    trades: list[TradePrint] = []
    for entry in entries:
        if not entry.segment.valid:
            continue
        for item in _segment_events(store, entry.segment.object_key):
            payload = item.get("payload")
            if not isinstance(payload, dict) or "a" not in payload:
                continue
            trades.append(
                TradePrint(
                    aggregate_trade_id=int(payload["a"]),
                    time_ms=int(payload.get("T", item["exchange_time_ms"])),
                    price=Decimal(str(payload["p"])),
                    quantity=Decimal(str(payload["q"])),
                    buyer_maker=bool(payload["m"]),
                )
            )
    return tuple(sorted(trades, key=lambda item: (item.time_ms, item.aggregate_trade_id)))
