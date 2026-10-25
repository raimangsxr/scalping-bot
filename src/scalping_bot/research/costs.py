from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from statistics import median

from scalping_bot.data.storage import ObjectStore
from scalping_bot.domain.costs import EngineCostBucket, ExecutionCosts


@dataclass(frozen=True)
class AccountCommissionSnapshot:
    symbol: str
    maker: Decimal
    taker: Decimal
    captured_ms: int
    account_context: str = ""

    def __post_init__(self) -> None:
        if self.symbol != "BTCUSDT":
            raise ValueError("commission snapshots are scoped to BTCUSDT")
        if self.maker < 0 or self.taker < 0 or self.captured_ms < 0:
            raise ValueError("commission snapshot values cannot be negative")

    @classmethod
    def from_binance_response(
        cls, response: dict[str, object], *, captured_ms: int, account_context: str = ""
    ) -> AccountCommissionSnapshot:
        standard = response.get("standardCommission")
        if not isinstance(standard, dict):
            raise ValueError("Binance commission response is missing standardCommission")
        try:
            maker = Decimal(str(standard["maker"]))
            taker = Decimal(str(standard["taker"]))
        except (KeyError, ValueError):
            raise ValueError("Binance commission response has malformed standard rates") from None
        return cls("BTCUSDT", maker, taker, captured_ms, account_context)


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
    source_evidence: tuple[str, ...] = ()

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def canonical_bytes(self) -> bytes:
        value = {
            "schema_version": 1,
            "commission": {
                "symbol": self.commission.symbol,
                "maker": str(self.commission.maker),
                "taker": str(self.commission.taker),
                "captured_ms": self.commission.captured_ms,
                "account_context": self.commission.account_context,
            },
            "buckets": [
                {
                    "hour_utc": hour,
                    "volatility_bucket": volatility,
                    "size_bucket": size,
                    "spread_bps": str(bucket.spread_bps),
                    "slippage_bps": str(bucket.slippage_bps),
                    "observations": bucket.observations,
                }
                for (hour, volatility, size), bucket in sorted(self.buckets.items())
            ],
            "scenarios": [str(item) for item in self.scenarios],
            "source_evidence": list(self.source_evidence),
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()

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
        try:
            bucket = self.buckets[(hour_utc, volatility_bucket, size_bucket)]
        except KeyError as error:
            raise ValueError("no calibrated execution-cost bucket matches this order") from error
        friction = (bucket.spread_bps / 2 + bucket.slippage_bps) / Decimal("10000")
        return notional * (self.commission.taker + friction) * stress_multiplier

    def as_engine_costs(self) -> ExecutionCosts:
        """Resolve the evidenced model to the shared research/live seam."""
        total_observations = sum(bucket.observations for bucket in self.buckets.values())
        if total_observations < 1:
            raise ValueError("execution cost model has no observations")
        half_spread = sum(
            (
                bucket.spread_bps / Decimal("20000") * bucket.observations
                for bucket in self.buckets.values()
            ),
            Decimal(0),
        ) / Decimal(total_observations)
        slippage = sum(
            (
                bucket.slippage_bps / Decimal("10000") * bucket.observations
                for bucket in self.buckets.values()
            ),
            Decimal(0),
        ) / Decimal(total_observations)
        return ExecutionCosts(
            commission_rate=self.commission.taker,
            half_spread_rate=half_spread,
            slippage_rate=slippage,
            calibrated_buckets=tuple(
                EngineCostBucket(
                    hour_utc=hour,
                    volatility_bucket=volatility,
                    size_bucket=size,
                    half_spread_rate=bucket.spread_bps / Decimal("20000"),
                    slippage_rate=bucket.slippage_bps / Decimal("10000"),
                    observations=bucket.observations,
                )
                for (hour, volatility, size), bucket in sorted(self.buckets.items())
            ),
        )

    @classmethod
    def load(cls, store: ObjectStore, key: str) -> ExecutionCostModel:
        raw = json.loads(store.read(key))
        if not isinstance(raw, dict):
            raise ValueError("execution cost model must be a JSON object")
        expected = key.rsplit("/", 1)[-1].removesuffix(".json")
        content = {
            item_key: item
            for item_key, item in raw.items()
            if item_key not in {"sha256", "identity"}
        }
        canonical = (json.dumps(content, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if hashlib.sha256(canonical).hexdigest() != expected:
            raise ValueError("execution cost model checksum does not match its key")
        commission_value = raw.get("commission")
        if not isinstance(commission_value, dict):
            raise ValueError("execution cost model commission is malformed")
        commission = AccountCommissionSnapshot(
            symbol=str(commission_value["symbol"]),
            maker=Decimal(str(commission_value["maker"])),
            taker=Decimal(str(commission_value["taker"])),
            captured_ms=int(commission_value["captured_ms"]),
            account_context=str(commission_value.get("account_context", "")),
        )
        raw_buckets = raw.get("buckets")
        if not isinstance(raw_buckets, list):
            raise ValueError("execution cost model buckets are malformed")
        buckets: dict[tuple[int, str, str], CostBucket] = {}
        for item in raw_buckets:
            if not isinstance(item, dict):
                raise ValueError("execution cost model bucket is malformed")
            identity = (
                int(str(item["hour_utc"])),
                str(item["volatility_bucket"]),
                str(item["size_bucket"]),
            )
            buckets[identity] = CostBucket(
                spread_bps=Decimal(str(item["spread_bps"])),
                slippage_bps=Decimal(str(item["slippage_bps"])),
                observations=int(item["observations"]),
            )
        scenarios = tuple(Decimal(str(item)) for item in raw.get("scenarios", ["1", "1.5", "2"]))
        return cls(
            commission=commission,
            buckets=buckets,
            scenarios=scenarios,
            source_evidence=tuple(str(item) for item in raw.get("source_evidence", [])),
        )


class ExecutionCostCalibrator:
    def fit(
        self,
        observations: tuple[CalibrationObservation, ...],
        *,
        commission: AccountCommissionSnapshot,
        required_buckets: tuple[tuple[int, str, str], ...] = (),
        minimum_observations: int = 1,
    ) -> ExecutionCostModel:
        if not observations:
            raise ValueError("cost calibration requires production observations")
        if minimum_observations < 1:
            raise ValueError("minimum calibration observations must be positive")
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
        if required_buckets:
            missing = [
                bucket
                for bucket in required_buckets
                if len(grouped.get(bucket, ())) < minimum_observations
            ]
            if missing:
                raise ValueError(f"missing calibration bucket evidence: {missing[0]}")
        buckets = {
            key: CostBucket(
                spread_bps=median(value[0] for value in values),
                slippage_bps=median(value[1] for value in values),
                observations=len(values),
            )
            for key, values in grouped.items()
        }
        return ExecutionCostModel(commission=commission, buckets=buckets)


class ExecutionCostModelPublisher:
    """Publish the same calibrated model consumed by replay and operations."""

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def publish(self, model: ExecutionCostModel, *, source_evidence: tuple[str, ...] = ()) -> str:
        published = ExecutionCostModel(
            commission=model.commission,
            buckets=dict(model.buckets),
            scenarios=model.scenarios,
            source_evidence=tuple(source_evidence),
        )
        canonical = published.canonical_bytes()
        key = f"cost-models/{published.identity}.json"
        payload = json.loads(canonical)
        if not isinstance(payload, dict):
            raise ValueError("execution cost model canonical payload is malformed")
        payload["sha256"] = published.identity
        content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.store.put_immutable(key, content)
        return key
