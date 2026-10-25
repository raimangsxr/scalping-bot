import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from statistics import pstdev

from scalping_bot.data.storage import ObjectStore
from scalping_bot.research.features import BookFlowFeatures


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
