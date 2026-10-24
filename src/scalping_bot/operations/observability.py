from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from prometheus_client import CollectorRegistry, Gauge, Histogram

SENSITIVE_FRAGMENTS = ("api_key", "apikey", "secret", "signature", "token", "authorization")


def redact_event(event: Mapping[str, object]) -> dict[str, Any]:
    return {key: _redact(key, value) for key, value in event.items()}


def _redact(key: str, value: object) -> Any:
    lowered = key.lower().replace("-", "_")
    if any(fragment in lowered for fragment in SENSITIVE_FRAGMENTS):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {
            nested_key: _redact(str(nested_key), nested) for nested_key, nested in value.items()
        }
    if isinstance(value, list):
        return [_redact(key, item) for item in value]
    if isinstance(value, str) and value.startswith(("https://", "http://")):
        parts = urlsplit(value)
        query = [
            (name, "[REDACTED]" if name.lower() in {"signature", "token", "apikey"} else item)
            for name, item in parse_qsl(parts.query, keep_blank_values=True)
        ]
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
        )
    return value


class OperationalMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.market_data_freshness_ms = Gauge(
            "market_data_freshness_ms", "Age of latest valid market event", registry=self.registry
        )
        self.market_data_gaps_total = Gauge(
            "market_data_gaps_total", "Observed market-data gaps", registry=self.registry
        )
        self.order_latency_seconds = Histogram(
            "order_latency_seconds", "Order request latency", registry=self.registry
        )
        self.reconciliation_divergences = Gauge(
            "reconciliation_divergences",
            "Current exchange/local divergences",
            registry=self.registry,
        )
        self.open_exposure_usdt = Gauge(
            "open_exposure_usdt", "Current BTCUSDT exposure", registry=self.registry
        )
        self.daily_pnl_usdt = Gauge(
            "daily_pnl_usdt", "Current UTC-day mark-to-market PnL", registry=self.registry
        )
        self.circuit_breaker_state = Gauge(
            "circuit_breaker_state", "Whether a circuit breaker is active", registry=self.registry
        )
        self.sqlite_health = Gauge(
            "sqlite_health", "SQLite health, one for healthy", registry=self.registry
        )
        self.storage_health = Gauge(
            "storage_health", "Object storage health, one for healthy", registry=self.registry
        )
        self.heartbeat_timestamp_seconds = Gauge(
            "heartbeat_timestamp_seconds", "Last successful heartbeat", registry=self.registry
        )

    @property
    def metric_names(self) -> set[str]:
        return {
            "market_data_freshness_ms",
            "market_data_gaps_total",
            "order_latency_seconds",
            "reconciliation_divergences",
            "open_exposure_usdt",
            "daily_pnl_usdt",
            "circuit_breaker_state",
            "sqlite_health",
            "storage_health",
            "heartbeat_timestamp_seconds",
        }
