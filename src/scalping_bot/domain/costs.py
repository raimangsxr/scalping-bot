from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import median


@dataclass(frozen=True)
class EngineCostBucket:
    """A calibrated friction bucket exposed at the shared-engine seam."""

    hour_utc: int
    volatility_bucket: str
    half_spread_rate: Decimal
    slippage_rate: Decimal
    observations: int
    size_bucket: str | None = None
    minimum_notional: Decimal = Decimal(0)
    maximum_notional: Decimal | None = None

    def __post_init__(self) -> None:
        if (
            not 0 <= self.hour_utc <= 23
            or not self.volatility_bucket
            or self.size_bucket == ""
            or self.half_spread_rate < 0
            or self.slippage_rate < 0
            or self.observations < 1
            or self.minimum_notional < 0
            or (
                self.maximum_notional is not None and self.maximum_notional <= self.minimum_notional
            )
        ):
            raise ValueError("engine execution cost bucket is invalid")

    def matches(
        self,
        *,
        hour_utc: int,
        volatility_bucket: str,
        size_bucket: str | None,
        notional: Decimal,
    ) -> bool:
        return (
            self.hour_utc == hour_utc
            and self.volatility_bucket == volatility_bucket
            and (self.size_bucket is None or self.size_bucket == size_bucket)
            and notional >= self.minimum_notional
            and (self.maximum_notional is None or notional < self.maximum_notional)
        )


@dataclass(frozen=True)
class ExecutionCosts:
    """The immutable friction assumptions used by replay and live evaluation.

    The rates are expressed as fractions of notional.  This deliberately small
    value object is the shared-engine seam; a calibrated snapshot can be
    resolved to these rates by an adapter without leaking calibration storage
    into the domain engine.
    """

    commission_rate: Decimal
    half_spread_rate: Decimal = Decimal(0)
    slippage_rate: Decimal = Decimal(0)
    calibrated_buckets: tuple[EngineCostBucket, ...] = ()

    def __post_init__(self) -> None:
        if any(
            rate < 0 for rate in (self.commission_rate, self.half_spread_rate, self.slippage_rate)
        ):
            raise ValueError("execution-cost rates cannot be negative")

    def estimated_close_cost(
        self,
        notional: Decimal,
        *,
        event_time_ms: int | None = None,
        volatility_bucket: str | None = None,
        size_bucket: str | None = None,
    ) -> Decimal:
        if notional < 0:
            raise ValueError("execution-cost notional cannot be negative")
        if self.calibrated_buckets:
            if event_time_ms is None or volatility_bucket is None:
                # The engine can receive a bare MarketTick from a legacy/live
                # adapter that has not yet attached a volatility classifier.
                # Keep the evidenced aggregate as a safe compatibility value;
                # callers that have context use the exact bucket below.
                return notional * (
                    self.commission_rate + self.half_spread_rate + self.slippage_rate
                )
            hour = datetime.fromtimestamp(event_time_ms / 1000, tz=UTC).hour
            matches = tuple(
                item
                for item in self.calibrated_buckets
                if item.matches(
                    hour_utc=hour,
                    volatility_bucket=volatility_bucket,
                    size_bucket=size_bucket,
                    notional=notional,
                )
            )
            if len(matches) != 1:
                return notional * (
                    self.commission_rate + self.half_spread_rate + self.slippage_rate
                )
            bucket = matches[0]
            return notional * (
                self.commission_rate + bucket.half_spread_rate + bucket.slippage_rate
            )
        return notional * (self.commission_rate + self.half_spread_rate + self.slippage_rate)

    def stressed(self, multiplier: Decimal) -> ExecutionCosts:
        """Scale the complete calibrated friction model for a preregistered scenario."""
        if multiplier not in {Decimal("1"), Decimal("1.5"), Decimal("2")}:
            raise ValueError("cost stress multiplier is not preregistered")
        return replace(
            self,
            commission_rate=self.commission_rate * multiplier,
            half_spread_rate=self.half_spread_rate * multiplier,
            slippage_rate=self.slippage_rate * multiplier,
            calibrated_buckets=tuple(
                replace(
                    bucket,
                    half_spread_rate=bucket.half_spread_rate * multiplier,
                    slippage_rate=bucket.slippage_rate * multiplier,
                )
                for bucket in self.calibrated_buckets
            ),
        )


@dataclass(frozen=True)
class ExecutionCostBucket:
    hour_utc: int | None
    volatility_bucket: str
    minimum_notional: Decimal
    maximum_notional: Decimal | None
    half_spread_rate: Decimal
    slippage_rate: Decimal
    observations: int
    size_bucket: str | None = None

    def __post_init__(self) -> None:
        if (
            (self.hour_utc is not None and not 0 <= self.hour_utc <= 23)
            or not self.volatility_bucket
            or self.size_bucket == ""
            or self.minimum_notional < 0
            or (
                self.maximum_notional is not None and self.maximum_notional <= self.minimum_notional
            )
            or self.half_spread_rate < 0
            or self.slippage_rate < 0
            or self.observations < 1
        ):
            raise ValueError("execution cost bucket is invalid")

    def matches(
        self,
        *,
        hour_utc: int,
        volatility: str,
        notional: Decimal,
        size_bucket: str | None = None,
    ) -> bool:
        return (
            (self.hour_utc is None or self.hour_utc == hour_utc)
            and self.volatility_bucket == volatility
            and (self.size_bucket is None or self.size_bucket == size_bucket)
            and notional >= self.minimum_notional
            and (self.maximum_notional is None or notional < self.maximum_notional)
        )


@dataclass(frozen=True)
class ExecutionCostSnapshot:
    snapshot_id: str
    captured_ms: int
    commission_rate: Decimal
    buckets: tuple[ExecutionCostBucket, ...]
    stress_multiplier: Decimal = Decimal(1)

    @classmethod
    def load(cls, path: Path) -> ExecutionCostSnapshot:
        raw = path.read_bytes()
        value = json.loads(raw)
        if not isinstance(value, dict) or not isinstance(value.get("buckets"), list):
            raise ValueError("execution cost snapshot is malformed")
        expected = value.get("sha256")
        if isinstance(value.get("commission"), dict):
            return cls._from_research_artifact(path, raw, value)
        if not isinstance(expected, str):
            raise ValueError("execution cost snapshot has no checksum")
        canonical_value = {key: item for key, item in value.items() if key != "sha256"}
        canonical = json.dumps(canonical_value, sort_keys=True, separators=(",", ":")).encode()
        identity = hashlib.sha256(canonical).hexdigest()
        if expected != identity:
            raise ValueError("execution cost snapshot checksum is invalid")
        if not all(isinstance(item, dict) for item in value["buckets"]):
            raise ValueError("execution cost snapshot contains a malformed bucket")
        try:
            buckets = tuple(
                ExecutionCostBucket(
                    hour_utc=(int(item["hour_utc"]) if item.get("hour_utc") is not None else None),
                    volatility_bucket=str(item["volatility_bucket"]),
                    minimum_notional=Decimal(str(item["minimum_notional"])),
                    maximum_notional=(
                        Decimal(str(item["maximum_notional"]))
                        if item.get("maximum_notional") is not None
                        else None
                    ),
                    half_spread_rate=Decimal(str(item["half_spread_rate"])),
                    slippage_rate=Decimal(str(item["slippage_rate"])),
                    observations=int(item["observations"]),
                    size_bucket=(str(item["size_bucket"]) if item.get("size_bucket") else None),
                )
                for item in value["buckets"]
            )
            commission_rate = Decimal(str(value["commission_rate"]))
            captured_ms = int(value["captured_ms"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("execution cost snapshot contains invalid values") from error
        if not buckets or any(item.observations < 1 for item in buckets):
            raise ValueError("execution cost snapshot has unevidenced buckets")
        if commission_rate < 0 or captured_ms < 0:
            raise ValueError("execution cost snapshot has invalid commission or timestamp")
        return cls(
            snapshot_id=identity,
            captured_ms=captured_ms,
            commission_rate=commission_rate,
            buckets=buckets,
        )

    @classmethod
    def _from_research_artifact(
        cls, path: Path, raw: bytes, value: dict[str, object]
    ) -> ExecutionCostSnapshot:
        """Adapt the calibrated research artifact to the live-engine seam.

        Research calibration is finer grained than the first live engine
        contract: it records size, hour, and volatility buckets. The adapter
        preserves all three dimensions at the engine seam; an adapter without
        an explicit size/volatility context can still use the aggregate rate.
        No fixed friction is invented, and the source artifact remains the
        checksum authority.
        """
        declared = value.get("sha256")
        canonical_value = {key: item for key, item in value.items() if key != "sha256"}
        canonical = (
            json.dumps(canonical_value, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        expected = str(declared) if declared is not None else path.name.removesuffix(path.suffix)
        if len(expected) != 64 or any(
            character not in "0123456789abcdef" for character in expected.lower()
        ):
            raise ValueError("research cost model must be content addressed")
        if hashlib.sha256(canonical).hexdigest() != expected.lower():
            raise ValueError("research cost model checksum is invalid")
        commission_value = value.get("commission")
        raw_buckets = value.get("buckets")
        if not isinstance(commission_value, dict) or not isinstance(raw_buckets, list):
            raise ValueError("research cost model is malformed")
        try:
            commission_rate = Decimal(str(commission_value["taker"]))
            captured_ms = int(str(commission_value["captured_ms"]))
            grouped: dict[tuple[int, str, str], list[tuple[Decimal, Decimal, int]]] = {}
            for item in raw_buckets:
                if not isinstance(item, dict):
                    raise ValueError("research cost model bucket is malformed")
                identity = (
                    int(str(item["hour_utc"])),
                    str(item["volatility_bucket"]),
                    str(item["size_bucket"]),
                )
                observations = int(str(item["observations"]))
                if observations < 1:
                    raise ValueError("research cost model bucket has no observations")
                grouped.setdefault(identity, []).append(
                    (
                        Decimal(str(item["spread_bps"])) / Decimal("20000"),
                        Decimal(str(item["slippage_bps"])) / Decimal("10000"),
                        observations,
                    )
                )
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("research cost model contains invalid values") from error
        buckets = tuple(
            ExecutionCostBucket(
                hour_utc=hour,
                volatility_bucket=volatility,
                minimum_notional=Decimal(0),
                maximum_notional=None,
                half_spread_rate=_weighted_median(
                    tuple((spread, count) for spread, _slippage, count in values)
                ),
                slippage_rate=_weighted_median(
                    tuple((slippage, count) for _spread, slippage, count in values)
                ),
                observations=sum(count for _spread, _slippage, count in values),
                size_bucket=size,
            )
            for (hour, volatility, size), values in sorted(grouped.items())
        )
        if not buckets:
            raise ValueError("research cost model has no evidenced buckets")
        return cls(
            snapshot_id=expected,
            captured_ms=captured_ms,
            commission_rate=commission_rate,
            buckets=buckets,
        )

    def stressed(self, multiplier: Decimal) -> ExecutionCostSnapshot:
        if multiplier not in {Decimal("1"), Decimal("1.5"), Decimal("2")}:
            raise ValueError("cost stress multiplier is not preregistered")
        return replace(self, stress_multiplier=multiplier)

    def as_engine_costs(self) -> ExecutionCosts:
        """Resolve the evidenced snapshot to the shared deterministic seam."""
        total_observations = sum(item.observations for item in self.buckets)
        if total_observations < 1:
            raise ValueError("execution cost snapshot has no observations")
        half_spread = sum(
            (item.half_spread_rate * item.observations for item in self.buckets), Decimal(0)
        ) / Decimal(total_observations)
        slippage = sum(
            (item.slippage_rate * item.observations for item in self.buckets), Decimal(0)
        ) / Decimal(total_observations)
        return ExecutionCosts(
            commission_rate=self.commission_rate,
            half_spread_rate=half_spread,
            slippage_rate=slippage,
            calibrated_buckets=tuple(
                EngineCostBucket(
                    hour_utc=item.hour_utc,
                    volatility_bucket=item.volatility_bucket,
                    half_spread_rate=item.half_spread_rate,
                    slippage_rate=item.slippage_rate,
                    observations=item.observations,
                    size_bucket=item.size_bucket,
                    minimum_notional=item.minimum_notional,
                    maximum_notional=item.maximum_notional,
                )
                for item in self.buckets
                if item.hour_utc is not None
            ),
        )

    def estimated_close_cost(
        self,
        notional: Decimal,
        *,
        event_time_ms: int,
        volatility_bucket: str,
        size_bucket: str | None = None,
    ) -> Decimal:
        hour = datetime.fromtimestamp(event_time_ms / 1000, tz=UTC).hour
        matches = tuple(
            item
            for item in self.buckets
            if item.matches(
                hour_utc=hour,
                volatility=volatility_bucket,
                notional=notional,
                size_bucket=size_bucket,
            )
        )
        if len(matches) != 1:
            raise ValueError("no calibrated execution-cost bucket matches this order")
        bucket = matches[0]
        return (
            notional
            * (self.commission_rate + bucket.half_spread_rate + bucket.slippage_rate)
            * self.stress_multiplier
        )


def _weighted_median(values: tuple[tuple[Decimal, int], ...]) -> Decimal:
    expanded = [value for value, count in values for _ in range(count)]
    if not expanded:
        raise ValueError("cannot calculate a cost median without observations")
    return median(expanded)
