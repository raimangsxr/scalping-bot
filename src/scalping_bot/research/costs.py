from __future__ import annotations

import gzip
import hashlib
import html
import json
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Protocol

from scalping_bot.data.capture import CaptureCohort, CaptureLedger, RawSegment
from scalping_bot.data.storage import ObjectStore
from scalping_bot.domain.costs import EngineCostBucket, ExecutionCosts
from scalping_bot.domain.events import MarketTick
from scalping_bot.domain.market_context import MarketContext


class CommissionSource(Protocol):
    def account_commission(self, symbol: str) -> dict[str, object]: ...


@dataclass(frozen=True)
class AccountCommissionSnapshot:
    symbol: str
    maker: Decimal
    taker: Decimal
    captured_ms: int
    account_context: str = ""
    raw_response: Mapping[str, object] = field(default_factory=dict, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.symbol != "BTCUSDT":
            raise ValueError("commission snapshots are scoped to BTCUSDT")
        if (
            not self.maker.is_finite()
            or not self.taker.is_finite()
            or self.maker < 0
            or self.taker < 0
            or self.captured_ms < 0
        ):
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
        except (InvalidOperation, KeyError, ValueError):
            raise ValueError("Binance commission response has malformed standard rates") from None
        return cls("BTCUSDT", maker, taker, captured_ms, account_context, dict(response))

    def canonical_bytes(self) -> bytes:
        value = {
            "schema_version": 1,
            "symbol": self.symbol,
            "maker": str(self.maker),
            "taker": str(self.taker),
            "captured_ms": self.captured_ms,
            "account_context": self.account_context,
            "raw_response": self.raw_response,
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def persist(self, store: ObjectStore) -> str:
        key = f"commission-snapshots/{self.identity}.json"
        store.put_immutable(key, self.canonical_bytes())
        return key

    @classmethod
    def load(cls, store: ObjectStore, key: str) -> AccountCommissionSnapshot:
        raw = store.read(key)
        expected = key.rsplit("/", 1)[-1].removesuffix(".json")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("commission snapshot checksum does not match its key")
        value = json.loads(raw)
        if not isinstance(value, dict) or not isinstance(value.get("raw_response", {}), dict):
            raise ValueError("commission snapshot is malformed")
        snapshot = cls(
            symbol=str(value["symbol"]),
            maker=Decimal(str(value["maker"])),
            taker=Decimal(str(value["taker"])),
            captured_ms=int(str(value["captured_ms"])),
            account_context=str(value.get("account_context", "")),
            raw_response=dict(value["raw_response"]),
        )
        if snapshot.identity != expected:
            raise ValueError("commission snapshot identity is invalid")
        return snapshot


def capture_account_commission_snapshot(
    source: CommissionSource,
    *,
    symbol: str,
    captured_ms: int,
    account_context: str,
) -> AccountCommissionSnapshot:
    """Capture the account-specific commission through the exchange seam."""
    if symbol != "BTCUSDT":
        raise ValueError("commission capture is scoped to BTCUSDT")
    return AccountCommissionSnapshot.from_binance_response(
        source.account_commission(symbol),
        captured_ms=captured_ms,
        account_context=account_context,
    )


@dataclass(frozen=True)
class CalibrationBucketPlan:
    """Frozen evidence requirements for one Execution Cost Model."""

    hours_utc: tuple[int, ...]
    volatility_buckets: tuple[str, ...]
    size_buckets: tuple[str, ...]
    minimum_observations: int = 1
    schema_version: int = 1

    def __post_init__(self) -> None:
        normalized_hours = tuple(sorted(set(self.hours_utc)))
        if any(hour < 0 or hour > 23 for hour in normalized_hours) or not normalized_hours:
            raise ValueError("calibration plan hours must be sorted unique UTC hours")
        object.__setattr__(self, "hours_utc", normalized_hours)
        for label, values in (
            ("volatility", self.volatility_buckets),
            ("size", self.size_buckets),
        ):
            normalized_values = tuple(sorted(set(values)))
            if not normalized_values or any(not item for item in normalized_values):
                raise ValueError(f"calibration plan {label} buckets must be sorted and non-empty")
            object.__setattr__(self, f"{label}_buckets", normalized_values)
        if self.minimum_observations < 1:
            raise ValueError("calibration plan minimum observations must be positive")

    @property
    def required_buckets(self) -> tuple[tuple[int, str, str], ...]:
        return tuple(
            (hour, volatility, size)
            for hour in self.hours_utc
            for volatility in self.volatility_buckets
            for size in self.size_buckets
        )

    def canonical_bytes(self) -> bytes:
        value = {
            "schema_version": self.schema_version,
            "hours_utc": list(self.hours_utc),
            "volatility_buckets": list(self.volatility_buckets),
            "size_buckets": list(self.size_buckets),
            "minimum_observations": self.minimum_observations,
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def persist(self, store: ObjectStore) -> str:
        key = f"calibration-plans/{self.identity}.json"
        store.put_immutable(key, self.canonical_bytes())
        return key

    @classmethod
    def load(cls, store: ObjectStore, key: str) -> CalibrationBucketPlan:
        raw = store.read(key)
        expected = key.rsplit("/", 1)[-1].removesuffix(".json")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("calibration plan checksum does not match its key")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("calibration plan is malformed")
        plan = cls(
            hours_utc=tuple(int(str(item)) for item in value["hours_utc"]),
            volatility_buckets=tuple(str(item) for item in value["volatility_buckets"]),
            size_buckets=tuple(str(item) for item in value["size_buckets"]),
            minimum_observations=int(str(value["minimum_observations"])),
            schema_version=int(str(value.get("schema_version", 1))),
        )
        if plan.identity != expected:
            raise ValueError("calibration plan identity is invalid")
        return plan


@dataclass(frozen=True)
class CalibrationObservation:
    time_ms: int
    hour_utc: int
    volatility_bucket: str
    size_bucket: str
    bid: Decimal
    ask: Decimal
    simulated_buy_price: Decimal


def calibration_observations_from_capture(
    store: ObjectStore,
    *,
    trade_stream: str = "btcusdt-aggTrade",
    quote_stream: str = "btcusdt-bookTicker",
    cohort_key: str | None = None,
) -> tuple[tuple[CalibrationObservation, ...], tuple[str, ...]]:
    """Derive causal calibration observations from valid immutable capture segments."""
    if cohort_key is None:
        trade_segments = CaptureLedger(store).catalog(trade_stream).valid_trade_segments
        quote_segments = CaptureLedger(store).catalog(quote_stream).valid_quote_segments
        evidence = tuple(
            sorted({segment.object_key for segment in (*trade_segments, *quote_segments)})
        )
    else:
        cohort = CaptureCohort.load(store, cohort_key)
        cohort.verify(store)
        if trade_stream not in cohort.streams or quote_stream not in cohort.streams:
            raise ValueError("calibration cohort must include trade and quote streams")
        trade_segments = tuple(
            segment for segment in cohort.segments if segment.stream == trade_stream
        )
        quote_segments = tuple(
            segment for segment in cohort.segments if segment.stream == quote_stream
        )
        evidence = tuple(sorted({cohort_key, *cohort.segment_keys}))
    trade_events = _capture_events(store, trade_segments)
    quote_events = _capture_events(store, quote_segments)
    quotes: list[tuple[int, Decimal, Decimal]] = []
    for event in quote_events:
        payload = event.get("payload")
        if not isinstance(payload, dict):
            continue
        try:
            bid = Decimal(str(payload["b"]))
            ask = Decimal(str(payload["a"]))
        except (KeyError, ValueError):
            continue
        if bid <= 0 or ask < bid:
            continue
        quotes.append((int(str(event["exchange_time_ms"])), bid, ask))
    quotes.sort(key=lambda item: item[0])
    context = MarketContext()
    observations: list[CalibrationObservation] = []
    quote_index = 0
    latest: tuple[int, Decimal, Decimal] | None = None
    for event in sorted(trade_events, key=lambda item: int(str(item["exchange_time_ms"]))):
        timestamp = int(str(event["exchange_time_ms"]))
        while quote_index < len(quotes) and quotes[quote_index][0] <= timestamp:
            latest = quotes[quote_index]
            quote_index += 1
        payload = event.get("payload")
        if latest is None or not isinstance(payload, dict):
            continue
        try:
            price = Decimal(str(payload["p"]))
            quantity = Decimal(str(payload["q"]))
        except (KeyError, ValueError):
            continue
        annotated = context.annotate_trade(
            MarketTick(event_id=f"calibration-{timestamp}", event_time_ms=timestamp, price=price),
            quantity=quantity,
        )
        _quote_time, bid, ask = latest
        simulated_buy_price = max(ask, price)
        observations.append(
            CalibrationObservation(
                time_ms=timestamp,
                hour_utc=datetime.fromtimestamp(timestamp / 1000, tz=UTC).hour,
                volatility_bucket=str(annotated.volatility_bucket),
                size_bucket=str(annotated.size_bucket),
                bid=bid,
                ask=ask,
                simulated_buy_price=simulated_buy_price,
            )
        )
    return tuple(observations), evidence


def _capture_events(
    store: ObjectStore, segments: tuple[RawSegment, ...]
) -> tuple[dict[str, object], ...]:
    events: list[dict[str, object]] = []
    for segment in segments:
        compressed = store.read(segment.object_key)
        if hashlib.sha256(compressed).hexdigest() != segment.sha256:
            raise ValueError(f"capture segment checksum does not match: {segment.object_key}")
        for line in gzip.decompress(compressed).splitlines():
            value = json.loads(line)
            if isinstance(value, dict):
                events.append(value)
    return tuple(events)


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
    calibration_method: str = "causal-bookticker-v1"
    bucket_plan_identity: str | None = None

    def __post_init__(self) -> None:
        if not self.calibration_method.strip():
            raise ValueError("execution cost calibration method is required")

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
            "calibration_method": self.calibration_method,
            "bucket_plan_identity": self.bucket_plan_identity,
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
            commission_snapshot={
                "symbol": self.commission.symbol,
                "maker": str(self.commission.maker),
                "taker": str(self.commission.taker),
                "captured_ms": str(self.commission.captured_ms),
                "account_context": self.commission.account_context,
            },
            execution_cost_model_identity=self.identity,
            calibration_method=self.calibration_method,
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
        calibration_method = str(raw.get("calibration_method", ""))
        if not calibration_method.strip():
            raise ValueError("execution cost model has no calibration method")
        raw_evidence = raw.get("source_evidence")
        if not isinstance(raw_evidence, list):
            raise ValueError("execution cost model has malformed source evidence")
        source_evidence = tuple(str(item) for item in raw_evidence)
        if not source_evidence or any(not item.strip() for item in source_evidence):
            raise ValueError("execution cost model has no immutable source evidence")
        for reference in source_evidence:
            if "#" not in reference:
                raise ValueError("execution cost model source evidence is unresolved")
            source_key, declared_checksum = reference.rsplit("#", 1)
            if (
                not source_key
                or len(declared_checksum) != 64
                or any(item not in "0123456789abcdef" for item in declared_checksum.lower())
            ):
                raise ValueError("execution cost model source evidence checksum is malformed")
            try:
                source_content = store.read(source_key)
            except (KeyError, OSError) as error:
                raise ValueError(
                    f"execution cost model source evidence is unavailable: {source_key}"
                ) from error
            if hashlib.sha256(source_content).hexdigest() != declared_checksum.lower():
                raise ValueError(
                    f"execution cost model source evidence checksum does not match: {source_key}"
                )
        return cls(
            commission=commission,
            buckets=buckets,
            scenarios=scenarios,
            source_evidence=source_evidence,
            calibration_method=calibration_method,
            bucket_plan_identity=(
                str(raw["bucket_plan_identity"])
                if raw.get("bucket_plan_identity") is not None
                else None
            ),
        )


def render_calibration_markdown(
    model: ExecutionCostModel, *, bucket_plan: CalibrationBucketPlan | None = None
) -> str:
    """Render the immutable human-readable calibration evidence report."""
    observed = sum(bucket.observations for bucket in model.buckets.values())
    required = bucket_plan.required_buckets if bucket_plan is not None else ()
    missing = tuple(
        bucket
        for bucket in required
        if model.buckets.get(bucket) is None
        or model.buckets[bucket].observations < bucket_plan.minimum_observations
    ) if bucket_plan is not None else ()
    plan_identity = bucket_plan.identity if bucket_plan is not None else model.bucket_plan_identity
    lines = [
        "# Execution Cost Model calibration report",
        "",
        f"Model identity: `{model.identity}`",
        f"Calibration method: `{model.calibration_method}`",
        f"Account commission: maker={model.commission.maker}; taker={model.commission.taker}",
        f"Commission captured (ms): {model.commission.captured_ms}",
        f"Account context: {model.commission.account_context or 'not supplied'}",
        f"Frozen bucket plan: `{plan_identity or 'not bound'}`",
        f"Observed samples: {observed}",
        f"Stress scenarios: {', '.join(str(item) + 'x' for item in model.scenarios)}",
        "",
        "## Bucket coverage",
        "",
        "| UTC hour | Volatility | Size | Spread (bps) | Slippage (bps) | Observations |",
        "| ---: | --- | --- | ---: | ---: | ---: |",
    ]
    for (hour, volatility, size), bucket in sorted(model.buckets.items()):
        lines.append(
            f"| {hour:02d} | {volatility} | {size} | {bucket.spread_bps} | "
            f"{bucket.slippage_bps} | {bucket.observations} |"
        )
    if bucket_plan is not None:
        lines.extend(("", "## Frozen protocol", ""))
        lines.append(f"- Minimum observations per bucket: {bucket_plan.minimum_observations}")
        lines.append(f"- Required buckets: {len(required)}")
        lines.append(f"- Under-evidenced buckets: {len(missing)}")
        if missing:
            lines.extend(
                f"- Missing: `{hour:02d}/{volatility}/{size}`"
                for hour, volatility, size in missing
            )
    lines.extend(
        (
            "",
            "## Evidence and limitations",
            "",
            *[f"- Source: `{item}`" for item in model.source_evidence],
            "- Spread is estimated from the contemporaneous best quote.",
            "- Slippage is estimated from the observed simulated buy price.",
            "- Missing or under-evidenced buckets fail closed; no undocumented "
            "constant is substituted.",
            "- The same content-addressed model is adapted by replay, Live Shadow, "
            "and Testnet consumers.",
            "",
        )
    )
    return "\n".join(lines)


def render_calibration_html(
    model: ExecutionCostModel, *, bucket_plan: CalibrationBucketPlan | None = None
) -> str:
    markdown = render_calibration_markdown(model, bucket_plan=bucket_plan)
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        "<title>Execution Cost Model calibration report</title></head><body><pre>"
        + html.escape(markdown)
        + "</pre></body></html>\n"
    )


class ExecutionCostCalibrator:
    def fit(
        self,
        observations: tuple[CalibrationObservation, ...],
        *,
        commission: AccountCommissionSnapshot,
        required_buckets: tuple[tuple[int, str, str], ...] = (),
        minimum_observations: int = 1,
        bucket_plan: CalibrationBucketPlan | None = None,
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
        if bucket_plan is not None:
            if required_buckets and required_buckets != bucket_plan.required_buckets:
                raise ValueError("explicit calibration buckets conflict with the frozen plan")
            required_buckets = bucket_plan.required_buckets
            minimum_observations = bucket_plan.minimum_observations
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
        return ExecutionCostModel(
            commission=commission,
            buckets=buckets,
            bucket_plan_identity=bucket_plan.identity if bucket_plan is not None else None,
        )


class ExecutionCostModelPublisher:
    """Publish the same calibrated model consumed by replay and operations."""

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def publish(self, model: ExecutionCostModel, *, source_evidence: tuple[str, ...] = ()) -> str:
        references = tuple(source_evidence) or model.source_evidence
        if not references or any(
            not isinstance(item, str) or not item.strip() for item in references
        ):
            raise ValueError("execution cost publication requires immutable source evidence")
        evidence: list[str] = []
        for reference in references:
            source_key, declared_checksum = (
                reference.rsplit("#", 1) if "#" in reference else (reference, None)
            )
            try:
                source_content = self.store.read(source_key)
            except (KeyError, OSError) as error:
                raise ValueError(
                    f"execution cost source evidence is unavailable: {source_key}"
                ) from error
            checksum = hashlib.sha256(source_content).hexdigest()
            if declared_checksum is not None and declared_checksum.lower() != checksum:
                raise ValueError(f"execution cost source checksum does not match: {source_key}")
            evidence.append(f"{source_key}#{checksum}")
        published = ExecutionCostModel(
            commission=model.commission,
            buckets=dict(model.buckets),
            scenarios=model.scenarios,
            source_evidence=tuple(sorted(set(evidence))),
            calibration_method=model.calibration_method,
            bucket_plan_identity=model.bucket_plan_identity,
        )
        plan: CalibrationBucketPlan | None = None
        for reference in evidence:
            source_key = reference.rsplit("#", 1)[0]
            if source_key.startswith("calibration-plans/"):
                candidate_plan = CalibrationBucketPlan.load(self.store, source_key)
                if plan is not None and plan.identity != candidate_plan.identity:
                    raise ValueError("execution cost publication references multiple bucket plans")
                plan = candidate_plan
        if published.bucket_plan_identity is not None:
            if plan is None:
                raise ValueError("bound execution cost model requires its calibration bucket plan")
            if plan.identity != published.bucket_plan_identity:
                raise ValueError("execution cost model and bucket plan identities do not match")
        canonical = published.canonical_bytes()
        key = f"cost-models/{published.identity}.json"
        payload = json.loads(canonical)
        if not isinstance(payload, dict):
            raise ValueError("execution cost model canonical payload is malformed")
        payload["sha256"] = published.identity
        content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.store.put_immutable(key, content)
        self.store.put_immutable(
            f"cost-models/{published.identity}.md",
            render_calibration_markdown(published, bucket_plan=plan).encode(),
        )
        self.store.put_immutable(
            f"cost-models/{published.identity}.html",
            render_calibration_html(published, bucket_plan=plan).encode(),
        )
        return key
