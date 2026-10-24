from __future__ import annotations

from collections import deque
from decimal import Decimal

from scalping_bot.domain.events import CandleClosed
from scalping_bot.domain.models import EntryProposal


class BreakoutPolicy:
    def __init__(self, *, candidate_version: str, lookback: int, buffer: Decimal) -> None:
        if lookback < 2 or buffer < 0:
            raise ValueError("invalid preregistered breakout parameters")
        self.candidate_version = candidate_version
        self.lookback = lookback
        self.buffer = buffer
        self._history: deque[CandleClosed] = deque(maxlen=lookback)

    def observe(self, event: CandleClosed) -> EntryProposal | None:
        if not event.complete:
            return None
        proposal: EntryProposal | None = None
        if len(self._history) == self.lookback:
            range_high = max(item.high for item in self._history)
            threshold = range_high * (Decimal(1) + self.buffer)
            if event.close > threshold:
                range_low = min(item.low for item in self._history)
                proposal = EntryProposal(
                    candidate_version=self.candidate_version,
                    source_event_id=event.event_id,
                    event_time_ms=event.event_time_ms,
                    direction="long",
                    rationale={
                        "condition": "completed close above prior trading range",
                        "invalidation": str(range_low),
                    },
                    feature_snapshot={
                        "prior_range_high": str(range_high),
                        "breakout_threshold": str(threshold),
                        "signal_close": str(event.close),
                    },
                    invalidation_price=range_low,
                    expiry_ms=event.event_time_ms + 60_000,
                    market_data_dependencies=("closed-1m-klines",),
                )
        self._history.append(event)
        return proposal
