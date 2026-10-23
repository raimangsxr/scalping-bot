from __future__ import annotations

from collections import deque
from decimal import Decimal

from scalping_bot.domain.events import CandleClosed
from scalping_bot.domain.models import EntryProposal


class MomentumPolicy:
    def __init__(
        self,
        *,
        candidate_version: str,
        window: int = 3,
        threshold: Decimal = Decimal("0.01"),
    ) -> None:
        if window < 2:
            raise ValueError("momentum window must contain at least two complete bars")
        self.candidate_version = candidate_version
        self.window = window
        self.threshold = threshold
        self._candles: deque[CandleClosed] = deque(maxlen=window)

    def observe(self, event: CandleClosed) -> EntryProposal | None:
        if not event.complete:
            return None
        self._candles.append(event)
        if len(self._candles) < self.window:
            return None
        first, last = self._candles[0], self._candles[-1]
        observed_return = (last.close - first.close) / first.close
        if observed_return < self.threshold:
            return None
        invalidation = min(candle.close for candle in self._candles)
        window_name = {2: "two", 3: "three", 4: "four", 5: "five"}.get(
            self.window, str(self.window)
        )
        return EntryProposal(
            candidate_version=self.candidate_version,
            source_event_id=event.event_id,
            event_time_ms=event.event_time_ms,
            direction="long",
            rationale={
                "condition": f"{window_name}-bar return >= {self.threshold:.0%}",
                "observed_return": str(observed_return),
                "invalidation": str(invalidation),
            },
            feature_snapshot={
                "first_close": str(first.close),
                "last_close": str(last.close),
                "return": str(observed_return),
            },
            invalidation_price=invalidation,
            expiry_ms=event.event_time_ms + 60_000,
            market_data_dependencies=("closed-1m-klines",),
        )
