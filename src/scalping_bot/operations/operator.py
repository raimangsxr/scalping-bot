"""Durable, auditable Operator controls for the Testnet executor."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.safety import OperationalMode


@dataclass(frozen=True)
class OperatorState:
    mode: OperationalMode
    entries_allowed: bool
    audit_id: int


class FlattenServiceProtocol(Protocol):
    def flatten_and_lock(self, *, event_time_ms: int, reason: str) -> None: ...


class OperatorControls:
    def __init__(self, state: SQLiteState, execution: FlattenServiceProtocol | None = None) -> None:
        self.state = state
        self.execution = execution

    def pause(self, *, reason: str, event_time_ms: int) -> OperatorState:
        self._require_reason(reason)
        audit_id = self.state.append_audit(
            kind="operator-pause-request",
            event_time_ms=event_time_ms,
            details={"reason": reason},
        )
        if self.state.operating_mode is not OperationalMode.FLATTEN_AND_LOCK:
            self.state.set_operating_mode(OperationalMode.PAUSED)
        return self._result(audit_id)

    def request_reconciliation(self, *, event_time_ms: int) -> OperatorState:
        audit_id = self.state.append_audit(
            kind="operator-reconciliation-request",
            event_time_ms=event_time_ms,
            details={"mode": self.state.operating_mode.value},
        )
        self.state.set_entries_allowed(False)
        return self._result(audit_id)

    def flatten_and_lock(
        self,
        *,
        reason: str,
        event_time_ms: int,
        flatten: Callable[[], None] | None = None,
    ) -> OperatorState:
        self._require_reason(reason)
        audit_id = self.state.append_audit(
            kind="operator-flatten-and-lock-request",
            event_time_ms=event_time_ms,
            details={"reason": reason},
        )
        if flatten is not None:
            flatten()
        elif self.execution is not None:
            self.execution.flatten_and_lock(event_time_ms=event_time_ms, reason=reason)
        self.state.set_operating_mode(OperationalMode.FLATTEN_AND_LOCK)
        self.state.append_audit(
            kind="operator-flatten-and-lock-state",
            event_time_ms=event_time_ms,
            details={"reason": reason},
        )
        return self._result(audit_id)

    def unlock(self, *, event_time_ms: int, operator_confirmed: bool) -> OperatorState:
        audit_id = self.state.append_audit(
            kind="operator-unlock-request",
            event_time_ms=event_time_ms,
            details={"operator_confirmed": operator_confirmed},
        )
        if self.state.operating_mode is OperationalMode.FLATTEN_AND_LOCK:
            raise ValueError("Flatten and Lock requires an explicit recovery procedure")
        if not operator_confirmed:
            raise ValueError("manual unlock requires explicit Operator confirmation")
        try:
            reconciliation = self.state.latest_reconciliation()
        except LookupError as error:
            raise ValueError("manual unlock requires a successful reconciliation") from error
        if not reconciliation["consistent"]:
            raise ValueError("manual unlock is blocked by unresolved reconciliation divergence")
        position = self.state.position()
        if position is not None and not bool(position["protected"]):
            raise ValueError("manual unlock is blocked while exposure is unprotected")
        self.state.set_operating_mode(OperationalMode.RUNNING)
        self.state.set_entries_allowed(True)
        self.state.append_audit(
            kind="operator-unlocked",
            event_time_ms=event_time_ms,
            details={"reconciliation_event_time_ms": reconciliation["event_time_ms"]},
        )
        return self._result(audit_id)

    def _result(self, audit_id: int) -> OperatorState:
        return OperatorState(
            mode=self.state.operating_mode,
            entries_allowed=self.state.entries_allowed,
            audit_id=audit_id,
        )

    @staticmethod
    def _require_reason(reason: str) -> None:
        if not reason.strip():
            raise ValueError("Operator controls require a reason")
