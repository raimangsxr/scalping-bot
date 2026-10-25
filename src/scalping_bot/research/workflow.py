"""Manifest-driven research runs and immutable evidence bundles."""

from __future__ import annotations

import hashlib
import html
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path

from scalping_bot.data.acquisition import (
    HISTORICAL_STRESS,
    PRIMARY_RESEARCH,
    ArchiveCatalogEntry,
    ArchiveKind,
    ResearchArchiveCatalog,
)
from scalping_bot.data.storage import ObjectStore
from scalping_bot.research.batch import BatchReview
from scalping_bot.research.candidates import CandidateBatch
from scalping_bot.research.costs import ExecutionCostModel
from scalping_bot.research.evidence import (
    CandidateRecord,
    EvidenceMetrics,
    EvidenceResult,
    ScalpResult,
)
from scalping_bot.research.reporting import render_evidence_html, render_evidence_markdown
from scalping_bot.research.screening import Attempt


@dataclass(frozen=True)
class ResearchRunManifest:
    code_commit: str
    dataset_catalog_key: str
    execution_cost_model_key: str
    dataset_checksums: tuple[str, ...]
    candidate_batch_sha256: str
    configuration: Mapping[str, object]
    period: tuple[str, str]
    random_seed: int
    schema_version: int = 1
    dataset_derived_keys: tuple[str, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        store: ObjectStore,
        code_commit: str,
        dataset_catalog_key: str,
        execution_cost_model_key: str,
        candidate_batch: CandidateBatch,
        configuration: Mapping[str, object],
        period: tuple[str, str],
        random_seed: int,
    ) -> ResearchRunManifest:
        if not code_commit or random_seed < 0:
            raise ValueError("research runs require an explicit commit and non-negative seed")
        if not period[0] or not period[1] or period[0] > period[1]:
            raise ValueError("research run period must be an ordered non-empty range")
        dataset = store.read(dataset_catalog_key)
        cost_model = store.read(execution_cost_model_key)
        _verify_content_addressed_key(dataset_catalog_key, dataset, "dataset catalog")
        _verify_content_addressed_key(execution_cost_model_key, cost_model, "cost model")
        dataset_checksums = [hashlib.sha256(dataset).hexdigest()]
        dataset_derived_keys: tuple[str, ...] = ()
        dataset_is_json = dataset.lstrip().startswith(b"{")
        cost_model_is_json = cost_model.lstrip().startswith(b"{")
        if dataset_catalog_key.startswith("catalogs/research-market/") and dataset_is_json:
            catalog = ResearchArchiveCatalog.load(store, dataset_catalog_key)
            catalog.require_complete()
            selected = _research_trade_entries(catalog)
            _require_research_months(selected)
            _verify_archive_provenance(store, selected)
            dataset_checksums.extend(sorted({entry.source_sha256 for entry in selected}))
            dataset_derived_keys = tuple(
                sorted(derived_key for entry in selected for derived_key in entry.derived_keys)
            )
            for derived_key in dataset_derived_keys:
                dataset_checksums.append(hashlib.sha256(store.read(derived_key)).hexdigest())
        if execution_cost_model_key.startswith("cost-models/") and cost_model_is_json:
            ExecutionCostModel.load(store, execution_cost_model_key)
        return cls(
            code_commit=code_commit,
            dataset_catalog_key=dataset_catalog_key,
            execution_cost_model_key=execution_cost_model_key,
            dataset_checksums=(*dataset_checksums, hashlib.sha256(cost_model).hexdigest()),
            candidate_batch_sha256=_candidate_batch_sha256(candidate_batch),
            configuration=dict(configuration),
            period=period,
            random_seed=random_seed,
            dataset_derived_keys=dataset_derived_keys,
        )

    def canonical_bytes(self) -> bytes:
        return (json.dumps(asdict(self), sort_keys=True, separators=(",", ":")) + "\n").encode()

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def persist(self, store: ObjectStore) -> str:
        key = f"research-runs/manifests/{self.identity}.json"
        store.put_immutable(key, self.canonical_bytes())
        return key

    @classmethod
    def load(cls, store: ObjectStore, key: str) -> ResearchRunManifest:
        raw = store.read(key)
        expected = key.rsplit("/", 1)[-1].removesuffix(".json")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("research run manifest checksum does not match its key")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("research run manifest must be a JSON object")
        checksums = value.get("dataset_checksums")
        configuration = value.get("configuration")
        period = value.get("period")
        if (
            not isinstance(checksums, list)
            or not all(isinstance(item, str) for item in checksums)
            or not isinstance(configuration, dict)
            or not isinstance(period, list)
            or len(period) != 2
            or not all(isinstance(item, str) for item in period)
        ):
            raise ValueError("research run manifest contains malformed inputs")
        manifest = cls(
            code_commit=str(value["code_commit"]),
            dataset_catalog_key=str(value["dataset_catalog_key"]),
            execution_cost_model_key=str(value["execution_cost_model_key"]),
            dataset_checksums=tuple(checksums),
            candidate_batch_sha256=str(value["candidate_batch_sha256"]),
            configuration=configuration,
            period=(period[0], period[1]),
            random_seed=int(str(value["random_seed"])),
            schema_version=int(str(value.get("schema_version", 1))),
            dataset_derived_keys=tuple(str(item) for item in value.get("dataset_derived_keys", [])),
        )
        if manifest.identity != expected:
            raise ValueError("research run manifest canonical identity is invalid")
        return manifest

    def verify(self, store: ObjectStore) -> bool:
        dataset = store.read(self.dataset_catalog_key)
        cost_model = store.read(self.execution_cost_model_key)
        _verify_content_addressed_key(self.dataset_catalog_key, dataset, "dataset catalog")
        _verify_content_addressed_key(self.execution_cost_model_key, cost_model, "cost model")
        checksums = [hashlib.sha256(dataset).hexdigest()]
        dataset_is_json = dataset.lstrip().startswith(b"{")
        cost_model_is_json = cost_model.lstrip().startswith(b"{")
        if self.dataset_catalog_key.startswith("catalogs/research-market/") and dataset_is_json:
            catalog = ResearchArchiveCatalog.load(store, self.dataset_catalog_key)
            catalog.require_complete()
            selected = _research_trade_entries(catalog)
            _require_research_months(selected)
            _verify_archive_provenance(store, selected)
            expected_derived_keys = tuple(
                sorted(derived_key for entry in selected for derived_key in entry.derived_keys)
            )
            if expected_derived_keys != tuple(sorted(self.dataset_derived_keys)):
                return False
            checksums.extend(sorted({entry.source_sha256 for entry in selected}))
            for derived_key in self.dataset_derived_keys:
                checksums.append(hashlib.sha256(store.read(derived_key)).hexdigest())
        checksums.append(hashlib.sha256(cost_model).hexdigest())
        if self.execution_cost_model_key.startswith("cost-models/") and cost_model_is_json:
            ExecutionCostModel.load(store, self.execution_cost_model_key)
        return tuple(checksums) == self.dataset_checksums and self.random_seed >= 0

    def verify_candidate_batch(self, candidate_batch: CandidateBatch) -> bool:
        return _candidate_batch_sha256(candidate_batch) == self.candidate_batch_sha256

    def verify_dataset_paths(self, paths: tuple[Path, ...], store: ObjectStore) -> bool:
        """Allow only Parquet files whose bytes are bound by the catalog."""
        if not self.dataset_derived_keys or not paths:
            return False
        expected = sorted(
            hashlib.sha256(store.read(key)).hexdigest() for key in self.dataset_derived_keys
        )
        actual = sorted(hashlib.sha256(path.read_bytes()).hexdigest() for path in paths)
        return actual == expected and len(actual) == len(set(actual))


class PersistentAttemptLedger:
    """An immutable, resumable attempt ledger keyed by input and result."""

    def __init__(self, store: ObjectStore, *, run_identity: str) -> None:
        if not run_identity:
            raise ValueError("attempt ledger requires a run identity")
        self.store = store
        self.run_identity = run_identity

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
        content = _json_bytes(asdict(attempt))
        input_identity = _json_bytes(
            {
                "candidate_identity": candidate_identity,
                "parameters": dict(parameters),
                "baseline": str(baseline),
                "is_baseline": is_baseline,
                "fold": fold,
            }
        )
        key = f"research-runs/{self.run_identity}/attempts/{_digest(input_identity)}.json"
        try:
            existing = self.store.read(key)
        except (FileNotFoundError, KeyError):
            self.store.put_immutable(key, content)
            return attempt
        if existing != content:
            raise ValueError("research attempt is immutable")
        return attempt

    @property
    def all_attempts(self) -> tuple[Attempt, ...]:
        attempts: list[Attempt] = []
        prefix = f"research-runs/{self.run_identity}/attempts/"
        for key in self.store.list_keys(prefix):
            raw = json.loads(self.store.read(key))
            if not isinstance(raw, dict):
                raise ValueError("attempt ledger entry is malformed")
            attempts.append(
                Attempt(
                    candidate_identity=str(raw["candidate_identity"]),
                    parameters=dict(raw["parameters"]),
                    net_expectancy=Decimal(str(raw["net_expectancy"])),
                    baseline_expectancy=Decimal(str(raw["baseline_expectancy"])),
                    incremental_expectancy=Decimal(str(raw["incremental_expectancy"])),
                    rejected=bool(raw["rejected"]),
                    is_baseline=bool(raw.get("is_baseline", False)),
                    fold=(int(str(raw["fold"])) if raw.get("fold") is not None else None),
                )
            )
        return tuple(sorted(attempts, key=lambda item: _attempt_sort_key(item)))

    @property
    def attempts(self) -> tuple[Attempt, ...]:
        return tuple(item for item in self.all_attempts if not item.is_baseline)

    @property
    def baseline_attempts(self) -> tuple[Attempt, ...]:
        return tuple(item for item in self.all_attempts if item.is_baseline)


class ResearchArtifactWriter:
    """Write the candidate records, ledger, traces, and reports as one bundle."""

    def __init__(self, store: ObjectStore, *, run_identity: str) -> None:
        self.store = store
        self.run_identity = run_identity

    def publish_empty_batch(self, batch: CandidateBatch) -> dict[str, str]:
        candidates = tuple(_candidate_json(item) for item in batch.candidates)
        candidate_content = _json_bytes({"candidates": candidates})
        candidate_key = self._put("candidate-records.json", candidate_content)
        attempts_key = self._put("attempts.json", _json_bytes({"attempts": []}))
        report = _batch_inventory_markdown(batch)
        markdown_key = self._put("report.md", report.encode())
        html_key = self._put(
            "report.html", render_evidence_html("first-batch", _empty_evidence(), ()).encode()
        )
        traces_key = self._put("scalp-traces.json", _json_bytes({"traces": []}))
        evidence_key = self._put("evidence.json", _json_bytes({"candidates": []}))
        return {
            "candidate-records": candidate_key,
            "attempt-ledger": attempts_key,
            "scalp-traces": traces_key,
            "evidence": evidence_key,
            "markdown": markdown_key,
            "html": html_key,
        }

    def publish_batch_evaluation(
        self,
        batch: CandidateBatch,
        *,
        evidence: Mapping[str, EvidenceResult],
        attempts: tuple[Attempt, ...],
        trades: Mapping[str, tuple[ScalpResult, ...]],
        historical_evidence: Mapping[str, EvidenceResult] | None = None,
        historical_trades: Mapping[str, tuple[ScalpResult, ...]] | None = None,
        candidate_records: Mapping[str, CandidateRecord] | None = None,
        manifest_keys: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Publish one complete machine and human-readable first-batch bundle."""
        historical_evidence = historical_evidence or {}
        historical_trades = historical_trades or {}
        candidate_records = candidate_records or {}
        manifest_keys = manifest_keys or {}
        candidates = tuple(_candidate_json(item) for item in batch.candidates)
        evidence_rows = [
            {
                "candidate_identity": identity,
                "primary": _evidence_json(result),
                "historical_stress": (
                    _evidence_json(historical_evidence[identity])
                    if identity in historical_evidence
                    else None
                ),
                "candidate_record": (
                    _candidate_json(candidate_records[identity])
                    if identity in candidate_records
                    else None
                ),
                "experiment_manifest_key": manifest_keys.get(identity),
            }
            for identity, result in sorted(evidence.items())
        ]
        trace_rows = [
            {
                "candidate_identity": identity,
                "primary": [asdict(item) for item in values],
                "historical_stress": [asdict(item) for item in historical_trades.get(identity, ())],
            }
            for identity, values in sorted(trades.items())
        ]
        markdown_sections = [
            _batch_inventory_markdown(batch),
            *(
                render_evidence_markdown(identity, result, trades.get(identity, ()))
                for identity, result in sorted(evidence.items())
            ),
            "## Historical Stress (separate from Primary Research)\n\n"
            + "\n".join(
                f"- {identity}: {_historical_line(result)}"
                for identity, result in sorted(historical_evidence.items())
            )
            + "\n",
        ]
        markdown = "\n".join(markdown_sections)
        return {
            "candidate-records": self._put(
                "candidate-records.json", _json_bytes({"candidates": candidates})
            ),
            "attempt-ledger": self._put(
                "attempts.json", _json_bytes({"attempts": [asdict(item) for item in attempts]})
            ),
            "scalp-traces": self._put("scalp-traces.json", _json_bytes({"traces": trace_rows})),
            "evidence": self._put("evidence.json", _json_bytes({"candidates": evidence_rows})),
            "markdown": self._put("report.md", markdown.encode()),
            "html": self._put(
                "report.html",
                (
                    "<!doctype html><html><body><pre>"
                    + html.escape(markdown)
                    + "</pre></body></html>\n"
                ).encode(),
            ),
        }

    def publish_review(
        self,
        batch: CandidateBatch,
        *,
        review: BatchReview,
        attempts: tuple[Attempt, ...],
        evidence: Mapping[str, EvidenceResult] | None = None,
        trades: Mapping[str, tuple[ScalpResult, ...]] | None = None,
    ) -> dict[str, str]:
        candidates = tuple(_candidate_json(item) for item in batch.candidates)
        evidence = evidence or {}
        trades = trades or {}
        keys = {
            "candidate-records": self._put(
                "candidate-records.json", _json_bytes({"candidates": candidates})
            ),
            "scalp-traces": self._put(
                "scalp-traces.json",
                _json_bytes(
                    {
                        "traces": [
                            {
                                "candidate_identity": identity,
                                "trades": [asdict(item) for item in values],
                            }
                            for identity, values in sorted(trades.items())
                        ]
                    }
                ),
            ),
            "evidence": self._put(
                "evidence.json",
                _json_bytes(
                    {
                        "candidates": [
                            {"candidate_identity": identity, "evidence": _evidence_json(value)}
                            for identity, value in sorted(evidence.items())
                        ]
                    }
                ),
            ),
        }
        attempts_content = _json_bytes({"attempts": [asdict(item) for item in attempts]})
        keys["attempt-ledger"] = self._put("attempts.json", attempts_content)
        report_sections = [review.markdown]
        for identity, result in sorted(evidence.items()):
            report_sections.append(
                render_evidence_markdown(identity, result, tuple(trades.get(identity, ())))
            )
        markdown = "\n".join(report_sections)
        keys["markdown"] = self._put("report.md", markdown.encode())
        keys["html"] = self._put(
            "report.html",
            (
                "<!doctype html><html><body><pre>"
                + html.escape(markdown)
                + "</pre></body></html>\n"
            ).encode(),
        )
        return keys

    def _put(self, name: str, content: bytes) -> str:
        key = f"research-runs/{self.run_identity}/{name}"
        self.store.put_immutable(key, content)
        return key


def _candidate_batch_sha256(batch: CandidateBatch) -> str:
    return _digest(
        _json_bytes({"candidates": [_candidate_json(item) for item in batch.candidates]})
    )


def _evidence_json(result: EvidenceResult) -> dict[str, object]:
    return {
        "metrics": asdict(result.metrics),
        "bootstrap_lower_95": result.bootstrap_lower_95,
        "positive_fold_fraction": result.positive_fold_fraction,
        "concentration_failed": result.concentration_failed,
        "promoted": result.promoted,
        "selection_adjusted_alpha": result.selection_adjusted_alpha,
        "cost_stress_multipliers": result.cost_stress_multipliers,
        "cost_scenario_metrics": _metric_map(result.cost_scenario_metrics),
        "comparators": result.comparators,
        "baseline_metrics": asdict(result.baseline_metrics)
        if result.baseline_metrics is not None
        else None,
        "incremental_net_return": result.incremental_net_return,
        "fold_metrics": {str(key): asdict(value) for key, value in result.fold_metrics.items()},
        "regime_metrics": {str(key): asdict(value) for key, value in result.regime_metrics.items()},
        "market_fidelity_verified": result.market_fidelity_verified,
        "baseline_cost_scenario_metrics": _metric_map(result.baseline_cost_scenario_metrics),
        "frozen_parameters": result.frozen_parameters,
        "dataset_checksum": result.dataset_checksum,
        "execution_cost_model_identity": result.execution_cost_model_identity,
    }


def _metric_map(values: Mapping[Decimal, EvidenceMetrics]) -> dict[str, dict[str, object]]:
    return {str(key): asdict(value) for key, value in values.items()}


def _historical_line(result: EvidenceResult) -> str:
    return (
        f"net return {result.metrics.net_return}; "
        f"drawdown {result.metrics.max_drawdown}; trades {result.metrics.trade_count}"
    )


def _candidate_json(candidate: CandidateRecord) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "version": candidate.version,
        "family": candidate.family,
        "source_uri": candidate.source_uri,
        "source_license": candidate.source_license,
        "thesis": candidate.thesis,
        "original_rules": dict(candidate.original_rules),
        "modifications": dict(candidate.modifications),
        "parameters": dict(candidate.parameters),
        "rejection_criteria": list(candidate.rejection_criteria),
        "lifecycle": candidate.lifecycle.value,
        "frozen": candidate.frozen,
        "market_data_dependencies": list(candidate.market_data_dependencies),
    }


def _batch_inventory_markdown(batch: CandidateBatch) -> str:
    lines = ["# Research run bundle", "", "## Candidate inventory", ""]
    lines.extend(
        f"- {candidate.identity} ({candidate.family}): preregistered parameters retained"
        for candidate in batch.candidates
    )
    return "\n".join(lines) + "\n"


def _empty_evidence() -> EvidenceResult:
    # The HTML report only needs a stable, human-readable shell for an empty run.
    metrics = EvidenceMetrics(
        net_return=Decimal(0),
        max_drawdown=Decimal(0),
        maximum_daily_loss=Decimal(0),
        expectancy=None,
        profit_factor=None,
        daily_sharpe=None,
        daily_sortino=None,
        win_rate=None,
        exposure=Decimal(0),
        turnover=Decimal(0),
        trade_count=0,
    )
    return EvidenceResult(
        metrics=metrics,
        bootstrap_lower_95=None,
        positive_fold_fraction=Decimal(0),
        concentration_failed=False,
        promoted=False,
        selection_adjusted_alpha=Decimal("0.05"),
    )


def _research_trade_entries(catalog: ResearchArchiveCatalog) -> tuple[ArchiveCatalogEntry, ...]:
    return tuple(
        entry
        for entry in catalog.entries
        if entry.kind is ArchiveKind.AGG_TRADES
        and entry.period in {HISTORICAL_STRESS.name, PRIMARY_RESEARCH.name}
    )


def _require_research_months(entries: tuple[ArchiveCatalogEntry, ...]) -> None:
    months = {entry.month for entry in entries}
    required = {*HISTORICAL_STRESS.months, *PRIMARY_RESEARCH.months}
    if months != required or len(entries) != len(required):
        missing = sorted(required - months)
        extra = sorted(months - required)
        detail = missing[0] if missing else f"unexpected {extra[0]}"
        raise ValueError(f"research aggregate-trade catalog has an invalid month set: {detail}")
    identities = {(entry.period, entry.month) for entry in entries}
    expected = {
        *{(HISTORICAL_STRESS.name, month) for month in HISTORICAL_STRESS.months},
        *{(PRIMARY_RESEARCH.name, month) for month in PRIMARY_RESEARCH.months},
    }
    if identities != expected:
        raise ValueError("research aggregate-trade catalog has duplicate or wrong period entries")


def _verify_archive_provenance(
    store: ObjectStore, entries: tuple[ArchiveCatalogEntry, ...]
) -> None:
    for entry in entries:
        if not entry.source_uri.startswith("https://data.binance.vision/"):
            raise ValueError("research catalog contains a non-official archive source")
        raw_manifest = json.loads(store.read(entry.manifest_key))
        if not isinstance(raw_manifest, dict):
            raise ValueError("research catalog archive manifest is malformed")
        if any(
            raw_manifest.get(field) != expected
            for field, expected in {
                "source_uri": entry.source_uri,
                "source_sha256": entry.source_sha256,
                "row_count": entry.row_count,
                "start_time_ms": entry.start_time_ms,
                "end_time_ms": entry.end_time_ms,
            }.items()
        ):
            raise ValueError("research catalog archive manifest is not bound to its entry")
        raw_key = raw_manifest.get("raw_key")
        if not isinstance(raw_key, str) or not raw_key:
            raise ValueError("research catalog archive manifest has no immutable raw input")
        try:
            if hashlib.sha256(store.read(raw_key)).hexdigest() != entry.source_sha256:
                raise ValueError("research catalog raw archive checksum does not match its entry")
        except (KeyError, OSError) as error:
            raise ValueError("research catalog raw archive is unavailable") from error
        if tuple(entry.derived_keys) != tuple(
            str(raw_manifest.get("parquet_key"))
            for _ in entry.derived_keys
        ):
            raise ValueError("research catalog derived artifact is not bound to its manifest")
        try:
            for derived_key in entry.derived_keys:
                store.read(derived_key)
        except (KeyError, OSError) as error:
            raise ValueError("research catalog derived artifact is unavailable") from error


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode()


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _verify_content_addressed_key(key: str, content: bytes, label: str) -> None:
    filename = key.rsplit("/", 1)[-1]
    expected = filename.removesuffix(".json")
    if len(expected) != 64 or not all(item in "0123456789abcdefABCDEF" for item in expected):
        raise ValueError(f"{label} key is not content addressed")
    actual = _digest(content)
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("sha256"), str):
        canonical = {item_key: item for item_key, item in payload.items() if item_key != "sha256"}
        actual = _digest(
            (json.dumps(canonical, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )
    if actual != expected.lower():
        raise ValueError(f"{label} checksum does not match its immutable key")


def _attempt_sort_key(item: Attempt) -> tuple[str, str, str, str]:
    return (
        item.candidate_identity,
        json.dumps(dict(item.parameters), sort_keys=True, default=str),
        str(item.fold),
        str(item.net_expectancy),
    )
