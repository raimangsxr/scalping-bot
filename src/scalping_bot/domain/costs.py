from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path


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

    def __post_init__(self) -> None:
        if any(
            rate < 0 for rate in (self.commission_rate, self.half_spread_rate, self.slippage_rate)
        ):
            raise ValueError("execution-cost rates cannot be negative")

    def estimated_close_cost(self, notional: Decimal) -> Decimal:
        if notional < 0:
            raise ValueError("execution-cost notional cannot be negative")
        return notional * (self.commission_rate + self.half_spread_rate + self.slippage_rate)


@dataclass(frozen=True)
class ExecutionCostBucket:
    hour_utc: int | None
    volatility_bucket: str
    minimum_notional: Decimal
    maximum_notional: Decimal | None
    half_spread_rate: Decimal
    slippage_rate: Decimal
    observations: int

    def __post_init__(self) -> None:
        if (
            (self.hour_utc is not None and not 0 <= self.hour_utc <= 23)
            or not self.volatility_bucket
            or self.minimum_notional < 0
            or (
                self.maximum_notional is not None and self.maximum_notional <= self.minimum_notional
            )
            or self.half_spread_rate < 0
            or self.slippage_rate < 0
            or self.observations < 1
        ):
            raise ValueError("execution cost bucket is invalid")

    def matches(self, *, hour_utc: int, volatility: str, notional: Decimal) -> bool:
        return (
            (self.hour_utc is None or self.hour_utc == hour_utc)
            and self.volatility_bucket == volatility
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

    def stressed(self, multiplier: Decimal) -> ExecutionCostSnapshot:
        if multiplier not in {Decimal("1"), Decimal("1.5"), Decimal("2")}:
            raise ValueError("cost stress multiplier is not preregistered")
        return replace(self, stress_multiplier=multiplier)

    def estimated_close_cost(
        self,
        notional: Decimal,
        *,
        event_time_ms: int,
        volatility_bucket: str,
    ) -> Decimal:
        hour = datetime.fromtimestamp(event_time_ms / 1000, tz=UTC).hour
        bucket = next(
            (
                item
                for item in self.buckets
                if item.matches(hour_utc=hour, volatility=volatility_bucket, notional=notional)
            ),
            None,
        )
        if bucket is None:
            raise ValueError("no calibrated execution-cost bucket matches this order")
        return (
            notional
            * (self.commission_rate + bucket.half_spread_rate + bucket.slippage_rate)
            * self.stress_multiplier
        )
