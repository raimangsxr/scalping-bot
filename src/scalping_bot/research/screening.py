from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Attempt:
    candidate_identity: str
    parameters: Mapping[str, object]
    net_expectancy: Decimal
    baseline_expectancy: Decimal
    incremental_expectancy: Decimal
    rejected: bool
    is_baseline: bool = False
    fold: int | None = None


class AttemptLedger:
    def __init__(self) -> None:
        self._attempts: list[Attempt] = []
        self._baseline_attempts: list[Attempt] = []

    @property
    def attempts(self) -> tuple[Attempt, ...]:
        return tuple(self._attempts)

    @property
    def baseline_attempts(self) -> tuple[Attempt, ...]:
        return tuple(self._baseline_attempts)

    @property
    def all_attempts(self) -> tuple[Attempt, ...]:
        return (*self._baseline_attempts, *self._attempts)

    def record(
        self,
        candidate_identity: str,
        parameters: Mapping[str, object],
        *,
        net_expectancy: Decimal,
        baseline: Decimal,
        is_baseline: bool = False,
        fold: int | None = None,
    ) -> Attempt:
        attempt = Attempt(
            candidate_identity=candidate_identity,
            parameters=dict(parameters),
            net_expectancy=net_expectancy,
            baseline_expectancy=baseline,
            incremental_expectancy=net_expectancy - baseline,
            rejected=net_expectancy <= baseline,
            is_baseline=is_baseline,
            fold=fold,
        )
        (self._baseline_attempts if is_baseline else self._attempts).append(attempt)
        return attempt
