from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class OperationalMode(StrEnum):
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    PAUSED = "PAUSED"
    FLATTEN_AND_LOCK = "FLATTEN_AND_LOCK"


class ActionKind(StrEnum):
    ENTRY = "entry"
    PROTECTIVE_EXIT = "protective-exit"
    FLATTEN = "flatten"
    RECONCILIATION = "reconciliation"
    OBSERVATION = "observation"


class OperationalController:
    def __init__(self) -> None:
        self.mode = OperationalMode.RUNNING
        self.reasons: list[str] = []

    def degrade(self, reason: str) -> None:
        if self.mode is OperationalMode.RUNNING:
            self.mode = OperationalMode.DEGRADED
        self.reasons.append(reason)

    def pause(self, reason: str) -> None:
        if self.mode is not OperationalMode.FLATTEN_AND_LOCK:
            self.mode = OperationalMode.PAUSED
        self.reasons.append(reason)

    def flatten_and_lock(self, reason: str) -> None:
        self.mode = OperationalMode.FLATTEN_AND_LOCK
        self.reasons.append(reason)

    def allowed(self, action: ActionKind) -> bool:
        if action in {ActionKind.PROTECTIVE_EXIT, ActionKind.FLATTEN, ActionKind.OBSERVATION}:
            return True
        if action is ActionKind.RECONCILIATION:
            return True
        return self.mode is OperationalMode.RUNNING


@dataclass
class ReservedRateCapacity:
    total: int
    reserved_for_exits: int
    consumed: int = 0

    def consume(self, action: ActionKind) -> bool:
        protective = action in {ActionKind.PROTECTIVE_EXIT, ActionKind.FLATTEN}
        ceiling = self.total if protective else self.total - self.reserved_for_exits
        if self.consumed >= ceiling:
            return False
        self.consumed += 1
        return True

    @staticmethod
    def retry_after(status: int, *, header_seconds: int) -> int:
        if status not in {418, 429}:
            raise ValueError("backoff applies only to Binance rate-limit responses")
        return max(1, header_seconds)


@dataclass(frozen=True)
class ClockGuard:
    max_skew_ms: int

    def observe(self, *, exchange_time_ms: int, local_time_ms: int) -> bool:
        return abs(local_time_ms - exchange_time_ms) <= self.max_skew_ms
