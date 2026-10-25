"""The single audited Final Holdout reveal and Testnet Ready decision."""

from __future__ import annotations

import hashlib
import html
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from scalping_bot.data.acquisition import FINAL_HOLDOUT, ArchiveKind, ResearchArchiveCatalog
from scalping_bot.data.storage import ObjectStore
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.costs import ExecutionCostModel
from scalping_bot.research.evidence import (
    CandidateLifecycle,
    CandidateRecord,
    EvidenceResult,
    HoldoutAccessError,
    HoldoutRegistry,
)
from scalping_bot.research.execution import AggTrade, canonical_aggregate_trade_bytes
from scalping_bot.research.runner import ResearchBatchRunner


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
        if any(isinstance(value, list) for value in candidate.parameters.values()):
            raise HoldoutAccessError("Final Holdout requires concrete frozen parameters")
        registered = next(
            (item for item in first_batch().candidates if item.identity == candidate.identity),
            None,
        )
        if registered is None or not _parameters_are_preregistered(
            candidate.parameters, registered.parameters
        ):
            raise HoldoutAccessError("candidate is not the preregistered first-batch identity")
        if len(holdout_dataset_checksum) != 64 or any(
            item not in "0123456789abcdefABCDEF" for item in holdout_dataset_checksum
        ):
            raise ValueError("Final Holdout requires a checksum-verified dataset")
        if holdout_dataset_key is None or execution_cost_model_key is None:
            raise ValueError(
                "Final Holdout requires immutable dataset and Execution Cost Model artifacts"
            )
        try:
            dataset = self.store.read(holdout_dataset_key)
        except (FileNotFoundError, KeyError) as error:
            raise ValueError("Final Holdout dataset artifact is unavailable") from error
        if hashlib.sha256(dataset).hexdigest() != holdout_dataset_checksum.lower():
            raise ValueError("Final Holdout dataset checksum does not match its artifact")
        dataset_identity = _require_content_addressed_key(holdout_dataset_key, dataset)
        if dataset_identity != holdout_dataset_checksum.lower():
            raise ValueError("Final Holdout dataset key is not its checksum")
        try:
            cost_model = self.store.read(execution_cost_model_key)
        except (FileNotFoundError, KeyError) as error:
            raise ValueError("Final Holdout cost artifact is unavailable") from error
        cost_identity = _require_content_addressed_key(execution_cost_model_key, cost_model)
        if evidence.dataset_checksum != holdout_dataset_checksum.lower():
            raise ValueError("Final Holdout evidence is bound to another dataset")
        if evidence.execution_cost_model_identity != cost_identity:
            raise ValueError("Final Holdout evidence is bound to another cost model")
        if evidence.frozen_parameters is None or dict(evidence.frozen_parameters) != dict(
            candidate.parameters
        ):
            raise ValueError("Final Holdout evidence is not bound to the frozen parameters")
        if execution_cost_model_key.startswith("cost-models/"):
            ExecutionCostModel.load(self.store, execution_cost_model_key)
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
                ready,
                parameters=parameters,
                execution_cost_model_identity=cost_identity,
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
        digest = report_key.rsplit("/", 1)[-1].removesuffix(".md")
        machine = (
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_identity": candidate.identity,
                    "decision": decision,
                    "reasons": list(reasons),
                    "holdout_dataset_checksum": holdout_dataset_checksum,
                    "reveal_key": reveal_key,
                    "candidate_manifest_key": manifest_key,
                    "report_sha256": digest,
                    "report": report,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
        self.store.put_immutable(report_key.removesuffix(".md") + ".json", machine)
        self.store.put_immutable(
            report_key.removesuffix(".md") + ".html",
            (
                '<!doctype html><html><head><meta charset="utf-8">'
                "<title>Final Holdout decision</title></head><body><pre>"
                + html.escape(report)
                + "</pre></body></html>\n"
            ).encode(),
        )
        return FinalHoldoutDecision(
            candidate_identity=candidate.identity,
            decision=decision,
            report_key=report_key,
            candidate_manifest_key=manifest_key,
            reveal_key=reveal_key,
            reasons=tuple(reasons),
        )

    def evaluate_aggregate_trades(
        self,
        candidate: CandidateRecord,
        *,
        aggregate_trades: tuple[AggTrade, ...],
        holdout_parquet_paths: tuple[Path, ...],
        holdout_catalog_key: str,
        holdout_dataset_key: str,
        holdout_dataset_checksum: str,
        execution_cost_model_key: str,
        actor: str,
    ) -> FinalHoldoutDecision:
        """Evaluate the immutable Final Holdout through the shared replay engine."""
        if not aggregate_trades:
            raise ValueError("Final Holdout aggregate-trade evidence cannot be empty")
        _verify_official_holdout_inputs(
            self.store,
            holdout_catalog_key=holdout_catalog_key,
            parquet_paths=holdout_parquet_paths,
        )
        months = {_month(item.time_ms) for item in aggregate_trades}
        required_months = {
            f"{year:04d}-{month:02d}"
            for year in (2025, 2026)
            for month in range(1, 13)
            if (year, month) >= (2025, 10) and (year, month) <= (2026, 9)
        }
        if months != required_months:
            raise ValueError(
                "Final Holdout trades must cover every month from October 2025 "
                "through September 2026"
            )
        dataset = self.store.read(holdout_dataset_key)
        if dataset != canonical_aggregate_trade_bytes(aggregate_trades):
            raise ValueError(
                "Final Holdout dataset artifact does not match parsed aggregate trades"
            )
        registered = next(
            (item for item in first_batch().candidates if item.identity == candidate.identity),
            None,
        )
        if registered is None or not _candidate_provenance_matches(candidate, registered):
            raise HoldoutAccessError("Final Holdout candidate provenance is not preregistered")
        model = ExecutionCostModel.load(self.store, execution_cost_model_key)
        evidence, _trades = ResearchBatchRunner(
            execution_costs=model.as_engine_costs(),
            code_commit="final-holdout",
            dataset_checksums=(holdout_dataset_checksum, model.identity),
        ).evaluate_frozen_candidate(
            candidate,
            aggregate_trades,
            dataset_checksum=holdout_dataset_checksum,
            execution_cost_model_identity=model.identity,
        )
        return self.evaluate(
            candidate,
            evidence=evidence,
            holdout_dataset_checksum=holdout_dataset_checksum,
            actor=actor,
            holdout_dataset_key=holdout_dataset_key,
            execution_cost_model_key=execution_cost_model_key,
        )


def _concrete_parameters(parameters: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(parameters, Mapping):
        raise ValueError("candidate parameters must be an object")
    return {
        name: value[0] if isinstance(value, list) and value else value
        for name, value in parameters.items()
    }


def _verify_official_holdout_inputs(
    store: ObjectStore,
    *,
    holdout_catalog_key: str,
    parquet_paths: tuple[Path, ...],
) -> None:
    catalog = ResearchArchiveCatalog.load(store, holdout_catalog_key)
    catalog.require_complete()
    entries = tuple(
        sorted(
            (
                entry
                for entry in catalog.entries
                if entry.period == FINAL_HOLDOUT.name and entry.kind is ArchiveKind.AGG_TRADES
            ),
            key=lambda entry: entry.month,
        )
    )
    if tuple(entry.month for entry in entries) != FINAL_HOLDOUT.months:
        raise ValueError("Final Holdout catalog must contain every official aggregate-trade month")
    if any(not entry.source_uri.startswith("https://data.binance.vision/") for entry in entries):
        raise ValueError("Final Holdout catalog contains a non-official archive source")
    expected_keys = tuple(key for entry in entries for key in entry.derived_keys)
    if not expected_keys or len(expected_keys) != len(parquet_paths):
        raise ValueError("Final Holdout Parquet inputs do not match the official catalog")
    expected_hashes = sorted(hashlib.sha256(store.read(key)).hexdigest() for key in expected_keys)
    actual_hashes = sorted(hashlib.sha256(path.read_bytes()).hexdigest() for path in parquet_paths)
    if actual_hashes != expected_hashes:
        raise ValueError("Final Holdout Parquet inputs are not the catalog-bound Binance artifacts")


def _parameters_are_preregistered(
    parameters: Mapping[str, object], preregistered: Mapping[str, object]
) -> bool:
    if set(parameters) != set(preregistered):
        return False
    for name, value in parameters.items():
        allowed = preregistered[name]
        if not isinstance(allowed, list):
            if value != allowed:
                return False
        elif isinstance(value, list):
            if any(item not in allowed for item in value):
                return False
        elif value not in allowed:
            return False
    return True


def _candidate_provenance_matches(candidate: CandidateRecord, registered: CandidateRecord) -> bool:
    return (
        candidate.family == registered.family
        and candidate.source_uri == registered.source_uri
        and candidate.source_license == registered.source_license
        and candidate.thesis == registered.thesis
        and dict(candidate.original_rules) == dict(registered.original_rules)
        and dict(candidate.modifications) == dict(registered.modifications)
        and tuple(candidate.rejection_criteria) == tuple(registered.rejection_criteria)
        and tuple(candidate.market_data_dependencies) == tuple(registered.market_data_dependencies)
    )


def _require_content_addressed_key(key: str, content: bytes) -> str:
    identity = key.rsplit("/", 1)[-1].removesuffix(".json").removesuffix(".parquet")
    if len(identity) != 64 or any(item not in "0123456789abcdef" for item in identity.lower()):
        raise ValueError("Final Holdout artifacts must be content addressed")
    actual = hashlib.sha256(content).hexdigest()
    try:
        parsed = json.loads(content)
    except (UnicodeDecodeError, ValueError):
        parsed = None
    if isinstance(parsed, dict) and parsed.get("sha256"):
        canonical = (
            json.dumps(
                {item_key: item for item_key, item in parsed.items() if item_key != "sha256"},
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
        actual = hashlib.sha256(canonical).hexdigest()
    if actual != identity.lower():
        raise ValueError("Final Holdout artifact checksum does not match its key")
    return identity.lower()


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
    fold_net_returns = json.dumps(
        {str(key): str(value.net_return) for key, value in evidence.fold_metrics.items()},
        sort_keys=True,
    )
    cost_net_returns = json.dumps(
        {str(key): str(value.net_return) for key, value in evidence.cost_scenario_metrics.items()},
        sort_keys=True,
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
        f"Expectancy: {evidence.metrics.expectancy or 'non-evaluable'}\n"
        f"Profit factor: {evidence.metrics.profit_factor or 'non-evaluable'}\n"
        f"Daily Sharpe: {evidence.metrics.daily_sharpe or 'non-evaluable'}\n"
        f"Daily Sortino: {evidence.metrics.daily_sortino or 'non-evaluable'}\n"
        f"Win rate: {evidence.metrics.win_rate or 'non-evaluable'}\n"
        f"Maximum drawdown: {evidence.metrics.max_drawdown}\n"
        f"Maximum daily loss: {evidence.metrics.maximum_daily_loss}\n"
        f"Trade count: {evidence.metrics.trade_count}\n"
        f"Bootstrap lower 95%: {evidence.bootstrap_lower_95 or 'non-evaluable'}\n"
        f"Positive fold fraction: {evidence.positive_fold_fraction}\n"
        f"Concentration failed: {evidence.concentration_failed}\n"
        f"Fold net returns: {fold_net_returns}\n"
        f"Cost stress net returns: {cost_net_returns}\n"
        "Research Target: aspirational interpretation only; "
        f"{target_note}\n\n"
        "Reasons:\n"
        f"{reason_lines}\n"
    )


def _month(time_ms: int) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(time_ms / 1000, tz=UTC).strftime("%Y-%m")
