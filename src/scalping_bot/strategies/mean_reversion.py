from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise

from scalping_bot.domain.events import CandleClosed
from scalping_bot.domain.models import EntryProposal


@dataclass(frozen=True)
class _VolumeBar:
    candle: CandleClosed
    volume: Decimal


class RegimeVwapPolicy:
    def __init__(
        self,
        *,
        candidate_version: str,
        window: int,
        max_efficiency_ratio: Decimal,
        deviation: Decimal,
    ) -> None:
        self.candidate_version = candidate_version
        self.window = window
        self.max_efficiency_ratio = max_efficiency_ratio
        self.deviation = deviation
        self._history: deque[_VolumeBar] = deque(maxlen=window)

    def observe(
        self, event: CandleClosed, *, volume: Decimal | None = None
    ) -> EntryProposal | None:
        volume = event.volume if volume is None else volume
        if not event.complete or volume <= 0:
            return None
        proposal: EntryProposal | None = None
        if len(self._history) == self.window:
            closes = [item.candle.close for item in self._history]
            path = sum(
                (abs(current - previous) for previous, current in pairwise(closes)),
                Decimal(0),
            )
            efficiency = abs(closes[-1] - closes[0]) / path if path else Decimal(0)
            volume_sum = sum((item.volume for item in self._history), Decimal(0))
            vwap = (
                sum((item.candle.close * item.volume for item in self._history), Decimal(0))
                / volume_sum
            )
            price_to_vwap = (event.close - vwap) / vwap
            if efficiency <= self.max_efficiency_ratio and price_to_vwap <= -self.deviation:
                invalidation = min(event.low, min(item.candle.low for item in self._history))
                proposal = EntryProposal(
                    candidate_version=self.candidate_version,
                    source_event_id=event.event_id,
                    event_time_ms=event.event_time_ms,
                    direction="long",
                    rationale={
                        "condition": "downside VWAP deviation in a lateral regime",
                        "invalidation": str(invalidation),
                    },
                    feature_snapshot={
                        "regime": "lateral",
                        "efficiency_ratio": str(efficiency),
                        "causal_vwap": str(vwap),
                        "price_to_vwap": str(price_to_vwap),
                    },
                    invalidation_price=invalidation,
                    expiry_ms=event.event_time_ms + 60_000,
                    market_data_dependencies=("closed-1m-klines", "completed-volume"),
                )
        self._history.append(_VolumeBar(event, volume))
        return proposal
