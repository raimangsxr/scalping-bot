"""Fail-closed operational health evaluation for the executor."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.observability import OperationalMetrics
from scalping_bot.operations.safety import OperationalMode


@dataclass(frozen=True)
class HealthSignals:
    market_data_freshness_ms: int | None = None
    clock_drift_ms: int = 0
    persistence_ok: bool = False
    storage_ok: bool = False
    notification_ok: bool = False
    reconciliation_ok: bool = False
    exchange_streams_connected: bool = True
    rate_limit_blocked: bool = False
    dependency_status: Mapping[str, bool] = field(default_factory=dict)
    stale_after_ms: int = 5_000
    max_clock_drift_ms: int = 1_000


@dataclass(frozen=True)
class HealthResult:
    mode: OperationalMode
    entries_allowed: bool
    reasons: tuple[str, ...]


class OperationalHealthController:
    def __init__(self, state: SQLiteState, metrics: OperationalMetrics | None = None) -> None:
        self.state = state
        self.metrics = metrics

    def apply(self, signals: HealthSignals, *, event_time_ms: int) -> HealthResult:
        reasons = list(_blocking_reasons(signals))
        if reasons:
            self.state.set_operating_mode(OperationalMode.DEGRADED)
            self.state.set_entries_allowed(False)
            self.state.append_audit(
                kind="operational-health-degraded",
                event_time_ms=event_time_ms,
                details={"reasons": reasons},
            )
        elif self.state.operating_mode is OperationalMode.RUNNING:
            self.state.set_entries_allowed(True)
        else:
            reasons.append("operator confirmation required after degraded operation")
            self.state.set_entries_allowed(False)
        if self.metrics is not None:
            self.metrics.entries_allowed.set(int(self.state.entries_allowed))
            self.metrics.operating_mode.set(
                float(list(OperationalMode).index(self.state.operating_mode))
            )
            if signals.market_data_freshness_ms is not None:
                self.metrics.market_data_freshness_ms.set(signals.market_data_freshness_ms)
            self.metrics.sqlite_health.set(int(signals.persistence_ok))
            self.metrics.storage_health.set(int(signals.storage_ok))
            self.metrics.reconciliation_divergences.set(int(not signals.reconciliation_ok))
            if not signals.notification_ok:
                self.metrics.alert_failures_total.inc()
            if signals.rate_limit_blocked:
                self.metrics.rate_limit_events_total.inc()
        return HealthResult(self.state.operating_mode, self.state.entries_allowed, tuple(reasons))


def _blocking_reasons(signals: HealthSignals) -> tuple[str, ...]:
    reasons: list[str] = []
    if signals.market_data_freshness_ms is None:
        reasons.append("market data has no observation")
    elif signals.market_data_freshness_ms > signals.stale_after_ms:
        reasons.append("market data is stale")
    if abs(signals.clock_drift_ms) > signals.max_clock_drift_ms:
        reasons.append("clock drift exceeds the safety window")
    if not signals.persistence_ok:
        reasons.append("durable persistence is unhealthy")
    if not signals.storage_ok:
        reasons.append("object storage is unhealthy")
    if not signals.notification_ok:
        reasons.append("critical notifications are unavailable")
    if not signals.reconciliation_ok:
        reasons.append("exchange reconciliation diverges")
    if not signals.exchange_streams_connected:
        reasons.append("exchange event streams are not connected")
    if signals.rate_limit_blocked:
        reasons.append("exchange rate-limit pressure reserves capacity for exits")
    reasons.extend(
        f"missing market data dependency: {name}"
        for name, valid in sorted(signals.dependency_status.items())
        if not valid
    )
    return tuple(reasons)
