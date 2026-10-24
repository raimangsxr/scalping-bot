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


class AttemptLedger:
    def __init__(self) -> None:
        self._attempts: list[Attempt] = []

    @property
    def attempts(self) -> tuple[Attempt, ...]:
        return tuple(self._attempts)

    def record(
        self,
        candidate_identity: str,
        parameters: Mapping[str, object],
        *,
        net_expectancy: Decimal,
        baseline: Decimal,
    ) -> Attempt:
        attempt = Attempt(
            candidate_identity=candidate_identity,
            parameters=dict(parameters),
            net_expectancy=net_expectancy,
            baseline_expectancy=baseline,
            incremental_expectancy=net_expectancy - baseline,
            rejected=net_expectancy <= baseline,
        )
        self._attempts.append(attempt)
        return attempt
