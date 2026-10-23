from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from statistics import median


@dataclass(frozen=True)
class AccountCommissionSnapshot:
    symbol: str
    maker: Decimal
    taker: Decimal
    captured_ms: int


@dataclass(frozen=True)
class CalibrationObservation:
    time_ms: int
    hour_utc: int
    volatility_bucket: str
    size_bucket: str
    bid: Decimal
    ask: Decimal
    simulated_buy_price: Decimal


@dataclass(frozen=True)
class CostBucket:
    spread_bps: Decimal
    slippage_bps: Decimal
    observations: int


@dataclass(frozen=True)
class ExecutionCostModel:
    commission: AccountCommissionSnapshot
    buckets: dict[tuple[int, str, str], CostBucket]
    scenarios: tuple[Decimal, ...] = (Decimal("1"), Decimal("1.5"), Decimal("2"))

    def estimate(
        self,
        *,
        notional: Decimal,
        hour_utc: int,
        volatility_bucket: str,
        size_bucket: str,
        stress_multiplier: Decimal = Decimal("1"),
    ) -> Decimal:
        if stress_multiplier not in self.scenarios:
            raise ValueError("cost stress multiplier must be preregistered")
        bucket = self.buckets[(hour_utc, volatility_bucket, size_bucket)]
        friction = (bucket.spread_bps / 2 + bucket.slippage_bps) / Decimal("10000")
        return notional * (self.commission.taker + friction) * stress_multiplier


class ExecutionCostCalibrator:
    def fit(
        self,
        observations: tuple[CalibrationObservation, ...],
        *,
        commission: AccountCommissionSnapshot,
    ) -> ExecutionCostModel:
        if not observations:
            raise ValueError("cost calibration requires production observations")
        grouped: dict[tuple[int, str, str], list[tuple[Decimal, Decimal]]] = defaultdict(list)
        for item in observations:
            mid = (item.bid + item.ask) / 2
            if not (item.bid <= item.ask <= item.simulated_buy_price):
                raise ValueError("calibration observation has inconsistent prices")
            spread_bps = (item.ask - item.bid) / mid * Decimal("10000")
            slippage_bps = (item.simulated_buy_price - item.ask) / mid * Decimal("10000")
            grouped[(item.hour_utc, item.volatility_bucket, item.size_bucket)].append(
                (spread_bps, slippage_bps)
            )
        buckets = {
            key: CostBucket(
                spread_bps=median(value[0] for value in values),
                slippage_bps=median(value[1] for value in values),
                observations=len(values),
            )
            for key, values in grouped.items()
        }
        return ExecutionCostModel(commission=commission, buckets=buckets)
