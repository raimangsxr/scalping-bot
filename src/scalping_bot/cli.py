import asyncio
import hashlib
import json
import time
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer

from scalping_bot import __version__
from scalping_bot.config import Settings
from scalping_bot.data.acquisition import (
    ResearchArchiveMaterializer,
    research_archive_requests,
)
from scalping_bot.data.live import BinancePublicCollector
from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.domain.models import ExecutionCosts
from scalping_bot.execution.binance import BinanceSpotTestnetAdapter, OfficialSpotTransport
from scalping_bot.execution.executor import ExecutionLimits, ExecutionService
from scalping_bot.execution.reconcile import Reconciler
from scalping_bot.execution.runtime import run_executor
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.deployment import DeploymentGate
from scalping_bot.operations.durable_shadow import DurableLiveShadow
from scalping_bot.operations.durable_soak import DurableTestnetSoak
from scalping_bot.operations.operator import OperatorControls
from scalping_bot.operations.promotion import PromotedRiskGate, RiskEvaluation
from scalping_bot.operations.release import ReleaseVerifier
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.costs import (
    AccountCommissionSnapshot,
    CalibrationObservation,
    ExecutionCostCalibrator,
    ExecutionCostModel,
    ExecutionCostModelPublisher,
)
from scalping_bot.research.reporting import render_evidence_markdown
from scalping_bot.research.runner import ResearchBatchRunner, load_aggtrades
from scalping_bot.research.workflow import ResearchArtifactWriter, ResearchRunManifest

app = typer.Typer(no_args_is_help=True)


@app.callback()
def main() -> None:
    """Research and Binance Spot Testnet operations."""


@app.command()
def check_config() -> None:
    """Validate configuration without opening a network connection."""
    settings = Settings()
    typer.echo(f"scalping-bot {__version__}: {settings.symbol} Testnet configuration is safe")


@app.command()
def collect_live(
    storage: Annotated[Path, typer.Option("--storage")] = Path("data"),
) -> None:
    """Continuously capture public production Trade Flow and Book Flow."""
    collector = BinancePublicCollector(LocalObjectStore(storage))
    asyncio.run(collector.run_forever())


@app.command()
def materialize_research_archive(
    output: Path = Path("data/research-market"),
    allow_incomplete: bool = typer.Option(
        False, help="Persist a resumable partial catalog instead of failing on gaps"
    ),
) -> None:
    """Acquire official klines and aggregate trades into an immutable catalog."""
    requests = research_archive_requests()
    catalog = ResearchArchiveMaterializer(LocalObjectStore(output)).materialize(
        requests, require_complete=not allow_incomplete
    )
    typer.echo(
        f"catalog={catalog.catalog_key} missing={len(catalog.missing)} gaps={len(catalog.gaps)}"
    )


@app.command()
def publish_cost_model(
    observations: Annotated[Path, typer.Argument(exists=True, readable=True)],
    commission: Annotated[Path, typer.Argument(exists=True, readable=True)],
    output: Path = Path("data/cost-model"),
    captured_ms: int = typer.Option(..., help="Capture time for the Account Commission Snapshot"),
) -> None:
    """Calibrate and publish one immutable Execution Cost Model."""
    raw_observations = json.loads(observations.read_text(encoding="utf-8"))
    raw_commission = json.loads(commission.read_text(encoding="utf-8"))
    if not isinstance(raw_observations, list) or not isinstance(raw_commission, dict):
        raise typer.BadParameter("observations must be a list and commission a JSON object")
    parsed = tuple(
        CalibrationObservation(
            time_ms=int(item["time_ms"]),
            hour_utc=int(item["hour_utc"]),
            volatility_bucket=str(item["volatility_bucket"]),
            size_bucket=str(item["size_bucket"]),
            bid=Decimal(str(item["bid"])),
            ask=Decimal(str(item["ask"])),
            simulated_buy_price=Decimal(str(item["simulated_buy_price"])),
        )
        for item in raw_observations
        if isinstance(item, dict)
    )
    snapshot = AccountCommissionSnapshot.from_binance_response(
        raw_commission, captured_ms=captured_ms, account_context="configured-account"
    )
    model = ExecutionCostCalibrator().fit(parsed, commission=snapshot)
    source_evidence = tuple(
        str(item["source"])
        for item in raw_observations
        if isinstance(item, dict) and item.get("source")
    )
    key = ExecutionCostModelPublisher(LocalObjectStore(output)).publish(
        model, source_evidence=source_evidence
    )
    typer.echo(f"cost-model={key}")


@app.command()
def create_research_manifest(
    artifacts: Path = Path("data/research-artifacts"),
    code_commit: str = typer.Option(...),
    dataset_catalog_key: str = typer.Option(...),
    execution_cost_model_key: str = typer.Option(...),
    random_seed: int = typer.Option(7),
) -> None:
    """Bind verified datasets, costs, candidates, configuration, and code identity."""
    store = LocalObjectStore(artifacts)
    manifest = ResearchRunManifest.create(
        store=store,
        code_commit=code_commit,
        dataset_catalog_key=dataset_catalog_key,
        execution_cost_model_key=execution_cost_model_key,
        candidate_batch=first_batch(),
        configuration={"initial_equity": "10000", "market": "BTCUSDT"},
        period=("2020-01", "2025-09"),
        random_seed=random_seed,
    )
    typer.echo(f"research-manifest={manifest.persist(store)}")


@app.command()
def run_research(
    manifest_key: Annotated[str, typer.Argument()],
    parquet: Annotated[list[Path], typer.Argument(exists=True, readable=True)],
    artifacts: Path = Path("data/research-artifacts"),
    historical_parquet: Annotated[
        list[Path] | None, typer.Option(exists=True, readable=True, help="Historical Stress inputs")
    ] = None,
) -> None:
    """Run the first batch from a verified immutable research manifest."""
    store = LocalObjectStore(artifacts)
    manifest = ResearchRunManifest.load(store, manifest_key)
    if not manifest.verify(store):
        raise RuntimeError("research manifest inputs are no longer verifiable")
    model = ExecutionCostModel.load(store, manifest.execution_cost_model_key)
    batch = first_batch()
    evaluation = ResearchBatchRunner(
        execution_costs=model.as_engine_costs(),
        manifest_store=store,
        code_commit=manifest.code_commit,
        dataset_checksums=manifest.dataset_checksums,
    ).run(
        load_aggtrades(tuple(parquet)),
        batch=batch,
        historical_trades=load_aggtrades(tuple(historical_parquet or ())),
    )
    keys = ResearchArtifactWriter(store, run_identity=manifest.identity).publish_batch_evaluation(
        batch,
        evidence=evaluation.evidence,
        attempts=evaluation.ledger.all_attempts,
        trades=evaluation.trades,
        historical_evidence=evaluation.historical_evidence,
        historical_trades=evaluation.historical_trades,
        candidate_records=evaluation.candidate_records,
        manifest_keys=evaluation.manifest_keys,
    )
    typer.echo(f"research-run={manifest.identity} evidence={keys['evidence']}")


@app.command()
def deployment_check(
    database: Path = Path("state/scalping-bot.db"),
    backup: Path = Path("state/backups/latest.sqlite.fernet"),
    target_schema: int = 4,
) -> None:
    """Block a planned rollout unless the executor is flat and recoverable."""
    DeploymentGate.assert_path_safe(database, backup=backup, target_schema=target_schema)
    typer.echo("deployment gate passed: flat, reconciled, backed up, schema compatible")


@app.command()
def verify_release(
    manifests: Path = Path("deploy/k8s"),
    application: Path = Path("deploy/argocd/application.yaml"),
) -> None:
    """Verify one immutable image, GitOps revision, and isolated process identities."""
    result = ReleaseVerifier.from_paths(manifests, application).verify()
    typer.echo(f"release verified: {result.image_revision}")


@app.command()
def pause_executor(
    database: Path = Path("state/scalping-bot.db"),
    reason: str = typer.Option(...),
) -> None:
    """Pause new entries while retaining reconciliation and protection."""
    with SQLiteState(database) as state:
        result = OperatorControls(state).pause(reason=reason, event_time_ms=_now_ms())
        typer.echo(f"{result.mode.value} entries_allowed={result.entries_allowed}")


@app.command()
def flatten_and_lock(
    database: Path = Path("state/scalping-bot.db"),
    reason: str = typer.Option(...),
) -> None:
    """Cancel pending work, flatten exposure through the executor, and lock entries."""
    settings = Settings()
    with SQLiteState(database) as state:
        exchange, limits = _configured_exchange(settings)
        result = OperatorControls(
            state, ExecutionService(state, exchange, limits)
        ).flatten_and_lock(reason=reason, event_time_ms=_now_ms())
        typer.echo(f"{result.mode.value} entries_allowed={result.entries_allowed}")


@app.command()
def request_reconciliation(database: Path = Path("state/scalping-bot.db")) -> None:
    """Request an explicit exchange reconciliation and block new entries."""
    settings = Settings()
    with SQLiteState(database) as state:
        OperatorControls(state).request_reconciliation(event_time_ms=_now_ms())
        exchange, _limits = _configured_exchange(settings)
        reconciliation = Reconciler(state, exchange).run(
            reason="operator-request", event_time_ms=_now_ms()
        )
        typer.echo(
            f"reconciled={reconciliation.consistent} divergences={len(reconciliation.divergences)}"
        )


@app.command()
def unlock_executor(
    database: Path = Path("state/scalping-bot.db"),
    operator_confirmed: bool = typer.Option(...),
) -> None:
    """Unlock entries only after a consistent reconciliation and Operator confirmation."""
    with SQLiteState(database) as state:
        result = OperatorControls(state).unlock(
            event_time_ms=_now_ms(), operator_confirmed=operator_confirmed
        )
        typer.echo(f"{result.mode.value} entries_allowed={result.entries_allowed}")


@app.command()
def run_live_shadow(
    state: Path = Path("state/live-shadow.db"),
    event_file: Annotated[Path | None, typer.Option(exists=True, readable=True)] = None,
    finalize_at_ms: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Run the restartable no-order Live Shadow process from comparison observations."""
    settings = Settings()
    if settings.candidate_manifest is None or settings.cost_model_snapshot is None:
        raise RuntimeError("Live Shadow requires a Testnet Ready candidate and cost snapshot")
    store = LocalObjectStore(state.parent / "live-shadow-artifacts")
    process = DurableLiveShadow(
        state,
        candidate_manifest=settings.candidate_manifest,
        cost_model=Path(settings.cost_model_snapshot),
        store=store,
        started_ms=_now_ms() if not state.exists() else None,
    )
    try:
        if event_file is not None:
            from scalping_bot.operations.promotion import LiveReplayComparison

            for line in event_file.read_text(encoding="utf-8").splitlines():
                value = json.loads(line)
                process.record_comparison(LiveReplayComparison(**value))
            if finalize_at_ms is not None:
                typer.echo(f"live-shadow-report={process.finalize(now_ms=finalize_at_ms)}")
            else:
                typer.echo("live-shadow observations persisted")
            return
        typer.echo("live-shadow process running; press Ctrl-C to stop")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        return
    finally:
        process.close()


@app.command()
def evaluate_promoted_risk(
    candidate_identity: Annotated[str, typer.Argument()],
    baseline: Annotated[Path, typer.Argument(exists=True, readable=True)],
    promoted: Annotated[Path, typer.Argument(exists=True, readable=True)],
    artifacts: Path = Path("data/promoted-risk"),
    promotion_artifact_key: str = typer.Option(...),
    soak_report_key: str = typer.Option(...),
) -> None:
    """Run the isolated 0.5% trial while preserving the 0.25% baseline."""
    store = LocalObjectStore(artifacts)
    baseline_value = json.loads(baseline.read_text(encoding="utf-8"))
    promoted_value = json.loads(promoted.read_text(encoding="utf-8"))
    if not isinstance(baseline_value, dict) or not isinstance(promoted_value, dict):
        raise typer.BadParameter("risk evaluations must be JSON objects")
    result = PromotedRiskGate(store).evaluate(
        candidate_identity,
        baseline=RiskEvaluation.from_mapping(baseline_value),
        promoted=RiskEvaluation.from_mapping(promoted_value),
        baseline_promoted=True,
        soak_complete=True,
        promotion_artifact_key=promotion_artifact_key,
        soak_report_key=soak_report_key,
    )
    report_key = f"{result.manifest_key.removesuffix('.json')}.md"
    store.put_immutable(report_key, result.report.encode())
    typer.echo(f"promoted-risk={report_key} accepted={result.accepted}")


@app.command()
def record_soak_window(
    candidate_identity: Annotated[str, typer.Argument()],
    start_ms: Annotated[int, typer.Option()],
    end_ms: Annotated[int, typer.Option()],
    database: Path = Path("state/testnet-soak.db"),
    alerts_verified: bool = typer.Option(...),
) -> None:
    """Record one supervised Testnet window in durable soak evidence."""
    process = DurableTestnetSoak(
        database,
        candidate_identity=candidate_identity,
        store=LocalObjectStore(database.parent / "testnet-soak-artifacts"),
        started_ms=start_ms,
    )
    try:
        process.record_supervised_window(
            start_ms=start_ms, end_ms=end_ms, alerts_verified=alerts_verified
        )
        typer.echo("supervised soak window recorded")
    finally:
        process.close()


@app.command()
def begin_soak_continuous(
    candidate_identity: Annotated[str, typer.Argument()],
    now_ms: Annotated[int, typer.Option()],
    database: Path = Path("state/testnet-soak.db"),
    alerts_verified: bool = typer.Option(...),
) -> None:
    """Admit continuous Testnet operation after supervised stability."""
    process = DurableTestnetSoak(
        database,
        candidate_identity=candidate_identity,
        store=LocalObjectStore(database.parent / "testnet-soak-artifacts"),
        started_ms=now_ms,
    )
    try:
        process.begin_continuous(now_ms=now_ms, alerts_verified=alerts_verified)
        typer.echo("continuous soak operation admitted")
    finally:
        process.close()


@app.command()
def finalize_testnet_soak(
    candidate_identity: Annotated[str, typer.Argument()],
    now_ms: Annotated[int, typer.Option()],
    database: Path = Path("state/testnet-soak.db"),
) -> None:
    """Emit the immutable Testnet Soak report when all gates are complete."""
    store = LocalObjectStore(database.parent / "testnet-soak-artifacts")
    process = DurableTestnetSoak(
        database,
        candidate_identity=candidate_identity,
        store=store,
        started_ms=now_ms,
    )
    try:
        typer.echo(f"testnet-soak-report={process.finalize(now_ms=now_ms)}")
    finally:
        process.close()


@app.command()
def initialize_state(database: Path = Path("state/scalping-bot.db")) -> None:
    """Apply SQLite migrations and exit."""
    with SQLiteState(database) as state:
        typer.echo(f"initialized schema {state.schema_version}")


@app.command()
def evaluate_first_batch(
    parquet: Annotated[list[Path], typer.Argument(exists=True, readable=True)],
    commission_rate: Annotated[str, typer.Option(help="Captured account taker commission rate")],
    half_spread_rate: Annotated[str, typer.Option(help="Calibrated half-spread rate")],
    slippage_rate: Annotated[str, typer.Option(help="Calibrated slippage rate")],
    output: Path = Path("reports/first-batch"),
    code_commit: str = typer.Option("working-tree"),
    historical_parquet: Annotated[
        list[Path] | None,
        typer.Option(exists=True, readable=True, help="Historical Stress parquet inputs"),
    ] = None,
) -> None:
    """Evaluate all preregistered variants with aggregate trades and shared engine."""
    trades = load_aggtrades(tuple(parquet))
    output.mkdir(parents=True, exist_ok=True)
    evaluation = ResearchBatchRunner(
        execution_costs=ExecutionCosts(
            commission_rate=Decimal(commission_rate),
            half_spread_rate=Decimal(half_spread_rate),
            slippage_rate=Decimal(slippage_rate),
        ),
        manifest_store=LocalObjectStore(output / "artifacts"),
        code_commit=code_commit,
        dataset_checksums=tuple(hashlib.sha256(path.read_bytes()).hexdigest() for path in parquet),
    ).run(trades, historical_trades=load_aggtrades(tuple(historical_parquet or ())))
    for identity, evidence in evaluation.evidence.items():
        (output / f"{identity}.md").write_text(
            render_evidence_markdown(identity, evidence, evaluation.trades[identity]),
            encoding="utf-8",
        )
    attempts = [
        {
            **asdict(attempt),
            "parameters": dict(attempt.parameters),
            "net_expectancy": str(attempt.net_expectancy),
            "baseline_expectancy": str(attempt.baseline_expectancy),
            "incremental_expectancy": str(attempt.incremental_expectancy),
        }
        for attempt in evaluation.ledger.all_attempts
    ]
    (output / "attempts.json").write_text(
        json.dumps(attempts, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    (output / "manifests.json").write_text(
        json.dumps(dict(evaluation.manifest_keys), sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    bundle_keys = ResearchArtifactWriter(
        LocalObjectStore(output / "artifacts"), run_identity=code_commit
    ).publish_batch_evaluation(
        first_batch(),
        evidence=evaluation.evidence,
        attempts=evaluation.ledger.all_attempts,
        trades=evaluation.trades,
        historical_evidence=evaluation.historical_evidence,
        historical_trades=evaluation.historical_trades,
        candidate_records=evaluation.candidate_records,
        manifest_keys=evaluation.manifest_keys,
    )
    (output / "bundle.json").write_text(
        json.dumps(bundle_keys, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    typer.echo(f"evaluated {len(evaluation.evidence)} candidates into {output}")


@app.command()
def run_testnet_executor() -> None:
    """Run the single-writer reconciled Binance Testnet executor."""
    run_executor(Settings())


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _configured_exchange(
    settings: Settings,
) -> tuple[BinanceSpotTestnetAdapter, ExecutionLimits]:
    if not settings.binance_api_key or not settings.binance_api_secret:
        raise RuntimeError("Binance Testnet credentials are required for this Operator control")
    transport = OfficialSpotTransport(
        api_key=settings.binance_api_key, api_secret=settings.binance_api_secret
    )
    exchange = BinanceSpotTestnetAdapter(endpoint=settings.binance_rest_url, transport=transport)
    capabilities = exchange.load_capabilities()
    return exchange, ExecutionLimits(
        step_size=Decimal(capabilities.step_size),
        min_notional=Decimal(capabilities.min_notional),
        tick_size=Decimal(capabilities.tick_size),
        max_quantity=Decimal(capabilities.max_quantity),
        max_notional=Decimal(capabilities.max_notional),
    )


if __name__ == "__main__":
    app()
