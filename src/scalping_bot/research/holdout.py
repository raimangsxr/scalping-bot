"""The single audited Final Holdout reveal and Testnet Ready decision."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from scalping_bot.data.storage import ObjectStore
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.research.evidence import (
    CandidateLifecycle,
    CandidateRecord,
    EvidenceResult,
    HoldoutAccessError,
    HoldoutRegistry,
)


@dataclass(frozen=True)
class FinalHoldoutDecision:
    candidate_identity: str
    decision: str
    report_key: str
    candidate_manifest_key: str | None
    reveal_key: str
    reasons: tuple[str, ...]


class FinalHoldoutGate:
    def __init__(self, store: ObjectStore, registry: HoldoutRegistry | None = None) -> None:
        self.store = store
        self.registry = registry or HoldoutRegistry(store)

    def evaluate(
        self,
        candidate: CandidateRecord,
        *,
        evidence: EvidenceResult,
        holdout_dataset_checksum: str,
        actor: str,
        holdout_dataset_key: str | None = None,
        execution_cost_model_key: str | None = None,
    ) -> FinalHoldoutDecision:
        if (
            not candidate.frozen
            or candidate.lifecycle is not CandidateLifecycle.WALK_FORWARD_VALIDATED
        ):
            raise HoldoutAccessError("candidate must be a frozen walk-forward finalist")
        if len(holdout_dataset_checksum) != 64 or any(
            item not in "0123456789abcdefABCDEF" for item in holdout_dataset_checksum
        ):
            raise ValueError("Final Holdout requires a checksum-verified dataset")
        if holdout_dataset_key is not None:
            try:
                dataset = self.store.read(holdout_dataset_key)
            except (FileNotFoundError, KeyError) as error:
                raise ValueError("Final Holdout dataset artifact is unavailable") from error
            if hashlib.sha256(dataset).hexdigest() != holdout_dataset_checksum.lower():
                raise ValueError("Final Holdout dataset checksum does not match its artifact")
        if execution_cost_model_key is not None:
            try:
                cost_model = self.store.read(execution_cost_model_key)
            except (FileNotFoundError, KeyError) as error:
                raise ValueError("Final Holdout cost artifact is unavailable") from error
            cost_key_identity = execution_cost_model_key.rsplit("/", 1)[-1].removesuffix(".json")
            if len(cost_key_identity) == 64:
                canonical = hashlib.sha256(cost_model).hexdigest()
                try:
                    parsed = json.loads(cost_model)
                except (UnicodeDecodeError, ValueError):
                    parsed = None
                if isinstance(parsed, dict) and parsed.get("sha256"):
                    canonical = hashlib.sha256(
                        (
                            json.dumps(
                                {key: value for key, value in parsed.items() if key != "sha256"},
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                            + "\n"
                        ).encode()
                    ).hexdigest()
                if canonical != cost_key_identity:
                    raise ValueError("Final Holdout cost checksum does not match its artifact")
        try:
            reveal_key = self.registry.reveal(
                candidate, command="reveal-final-holdout", actor=actor
            )
        except HoldoutAccessError as error:
            if "already" not in str(error) or not self.registry.was_revealed(candidate):
                raise
            reveal_key = f"holdout-access/{candidate.identity}.json"

        reasons: list[str] = []
        manifest_key: str | None = None
        if not evidence.market_fidelity_verified:
            reasons.append("Final Holdout aggregate-trade fidelity was not verified")
        if not {Decimal("1"), Decimal("1.5"), Decimal("2")}.issubset(
            evidence.cost_stress_multipliers
        ):
            reasons.append("Final Holdout calibrated and stressed costs were not all evidenced")
        if not evidence.promoted:
            reasons.append("Final Holdout promotion gates failed")
        decision = "Testnet Ready" if not reasons else "Rejected"
        if decision == "Testnet Ready":
            tested = candidate.transition(
                CandidateLifecycle.HOLDOUT_TESTED,
                evidence=f"Final Holdout dataset {holdout_dataset_checksum}",
            )
            ready = tested.transition(
                CandidateLifecycle.TESTNET_READY, evidence="holdout gates passed"
            )
            parameters = _concrete_parameters(ready.parameters)
            manifest_key = PromotedCandidateManifest.from_candidate(
                ready, parameters=parameters
            ).persist_store(self.store)
        report = _render_report(
            candidate,
            evidence=evidence,
            decision=decision,
            reasons=tuple(reasons),
            holdout_dataset_checksum=holdout_dataset_checksum,
            reveal_key=reveal_key,
            candidate_manifest_key=manifest_key,
            holdout_dataset_key=holdout_dataset_key,
            execution_cost_model_key=execution_cost_model_key,
        )
        report_key = (
            f"final-holdout/{candidate.identity}/{hashlib.sha256(report.encode()).hexdigest()}.md"
        )
        self.store.put_immutable(report_key, report.encode())
        return FinalHoldoutDecision(
            candidate_identity=candidate.identity,
            decision=decision,
            report_key=report_key,
            candidate_manifest_key=manifest_key,
            reveal_key=reveal_key,
            reasons=tuple(reasons),
        )


def _concrete_parameters(parameters: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(parameters, Mapping):
        raise ValueError("candidate parameters must be an object")
    return {
        name: value[0] if isinstance(value, list) and value else value
        for name, value in parameters.items()
    }


def _render_report(
    candidate: CandidateRecord,
    *,
    evidence: EvidenceResult,
    decision: str,
    reasons: tuple[str, ...],
    holdout_dataset_checksum: str,
    reveal_key: str,
    candidate_manifest_key: str | None,
    holdout_dataset_key: str | None,
    execution_cost_model_key: str | None,
) -> str:
    reason_lines = "\n".join(f"- {reason}" for reason in reasons) or "- none"
    target_note = (
        "reported separately" if decision == "Testnet Ready" else "not used to rescue the decision"
    )
    return (
        f"# Final Holdout — {candidate.identity}\n\n"
        f"Decision: {decision}\n"
        f"Dataset SHA-256: {holdout_dataset_checksum}\n"
        f"Dataset artifact: {holdout_dataset_key or 'checksum supplied by Operator'}\n"
        f"Execution Cost Model: {execution_cost_model_key or 'bound by evidence manifest'}\n"
        f"Audited reveal: {reveal_key}\n"
        f"Candidate manifest: {candidate_manifest_key or 'none'}\n"
        f"Net return: {evidence.metrics.net_return}\n"
        f"Maximum drawdown: {evidence.metrics.max_drawdown}\n"
        f"Maximum daily loss: {evidence.metrics.maximum_daily_loss}\n"
        f"Trade count: {evidence.metrics.trade_count}\n"
        "Research Target: aspirational interpretation only; "
        f"{target_note}\n\n"
        "Reasons:\n"
        f"{reason_lines}\n"
    )
