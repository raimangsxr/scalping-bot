from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from scalping_bot.research.candidates import CandidateBatch
from scalping_bot.research.evidence import (
    CandidateLifecycle,
    EvidenceResult,
    HoldoutRegistry,
)
from scalping_bot.research.screening import AttemptLedger


@dataclass(frozen=True)
class CandidateInventory:
    identity: str
    version: int
    family: str
    parameter_attempts: int
    rejected_attempts: int
    decision: str
    attempted_parameters: tuple[Mapping[str, object], ...] = ()


@dataclass(frozen=True)
class BatchReview:
    inventory: tuple[CandidateInventory, ...]
    holdout_reveals: tuple[str, ...]
    testnet_ready_candidates: tuple[str, ...]
    markdown: str


class BatchCloser:
    def __init__(self, holdout: HoldoutRegistry) -> None:
        self.holdout = holdout

    def close(
        self,
        batch: CandidateBatch,
        *,
        ledger: AttemptLedger,
        evidence: dict[str, EvidenceResult],
        holdout_evidence: dict[str, EvidenceResult] | None = None,
    ) -> BatchReview:
        final_evidence = holdout_evidence or {}
        known_identities = {candidate.identity for candidate in batch.candidates}
        unknown_attempts = {
            attempt.candidate_identity
            for attempt in ledger.all_attempts
            if attempt.candidate_identity not in known_identities
        }
        if unknown_attempts:
            raise ValueError(
                "attempt ledger contains candidates outside the preregistered batch: "
                + ", ".join(sorted(unknown_attempts))
            )
        inventory: list[CandidateInventory] = []
        reveals: list[str] = []
        ready: list[str] = []
        detail_lines: list[str] = []
        for candidate in batch.candidates:
            attempts = [
                attempt
                for attempt in ledger.all_attempts
                if attempt.candidate_identity == candidate.identity
            ]
            prior = evidence.get(candidate.identity)
            final = final_evidence.get(candidate.identity)
            if prior is None or not prior.promoted:
                decision = "rejected before Final Holdout"
            elif final is None:
                finalist = (
                    candidate.transition(CandidateLifecycle.SPECIFIED, evidence="preregistered")
                    .transition(CandidateLifecycle.SCREENED, evidence="screening passed")
                    .transition(
                        CandidateLifecycle.WALK_FORWARD_VALIDATED,
                        evidence="aggregate-trade walk-forward gates passed",
                    )
                    .freeze()
                )
                reveals.append(
                    self.holdout.reveal(
                        finalist, command="reveal-final-holdout", actor="batch-close"
                    )
                )
                decision = "awaits Final Holdout evidence"
            elif not self.holdout.was_revealed(candidate):
                raise ValueError(
                    f"Final Holdout evidence for {candidate.identity} lacks its audited reveal"
                )
            elif final.promoted:
                decision = "Testnet Ready"
                ready.append(candidate.identity)
            else:
                decision = "rejected by Final Holdout"
            inventory.append(
                CandidateInventory(
                    identity=candidate.identity,
                    version=candidate.version,
                    family=candidate.family,
                    parameter_attempts=len(attempts),
                    rejected_attempts=sum(attempt.rejected for attempt in attempts),
                    decision=decision,
                    attempted_parameters=tuple(dict(attempt.parameters) for attempt in attempts),
                )
            )
            if prior is not None:
                required_costs = {Decimal("1"), Decimal("1.5"), Decimal("2")}
                costs_evidenced = required_costs.issubset(
                    prior.cost_stress_multipliers
                ) and required_costs.issubset(prior.cost_scenario_metrics)
                incremental = (
                    prior.incremental_net_return
                    if prior.incremental_net_return is not None
                    else "non-evaluable"
                )
                target_note = (
                    "not evaluated as a promotion gate"
                    if not prior.promoted
                    else "reported separately"
                )
                final_note = (
                    "Final Holdout decision: " + ("pass" if final.promoted else "reject")
                    if final is not None
                    else "Final Holdout: not revealed"
                )
                cost_stress = (
                    ", ".join(
                        f"{key}x={value.net_return}"
                        for key, value in sorted(prior.cost_scenario_metrics.items())
                    )
                    or "not evidenced"
                )
                detail_lines.extend(
                    (
                        f"### {candidate.identity}",
                        "",
                        f"- Aggregate-trade fidelity: "
                        f"{'verified' if prior.market_fidelity_verified else 'not evidenced'}",
                        f"- Calibrated/stressed costs represented: "
                        f"{'yes' if costs_evidenced else 'no'}",
                        f"- Cost-stress net returns: {cost_stress}",
                        f"- Risk gates: {'pass' if prior.promoted else 'fail or incomplete'}",
                        f"- Concentration: {'fail' if prior.concentration_failed else 'pass'}",
                        f"- Incremental net return versus baseline: {incremental}",
                        f"- Research Target interpretation: aspirational; {target_note}",
                        f"- {final_note}; no post-reveal adjustment is unseen evidence",
                        f"- Decision: {decision}",
                        "",
                    )
                )
        summary = f"Testnet Ready: {', '.join(ready)}" if ready else "No candidate is Testnet Ready"
        revealed_candidates = ", ".join(key.split("/")[-1][:-5] for key in reveals)
        holdout_note = (
            f"Final Holdout revealed once for: {revealed_candidates}."
            if reveals
            else "Final Holdout was not revealed because no candidate passed prior gates."
        )
        lines = [
            "# First eight-candidate batch review",
            "",
            f"## Decision: {summary}",
            "",
            "The Research Target is aspirational and was not used to rescue or reject "
            "an early gate.",
            holdout_note,
            "Any post-reveal rule or parameter change creates a new candidate version "
            "and cannot reuse this holdout as unseen evidence.",
            "",
            "## Inventory",
            "",
        ]
        lines.extend(
            f"- {item.identity} v{item.version} ({item.family}): "
            f"{item.parameter_attempts} attempts, "
            f"{item.rejected_attempts} rejected — {item.decision}"
            for item in inventory
        )
        lines.extend(("", "## Evidence detail", "", *detail_lines))
        return BatchReview(tuple(inventory), tuple(reveals), tuple(ready), "\n".join(lines) + "\n")
