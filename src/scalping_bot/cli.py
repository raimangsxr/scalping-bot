import asyncio
import hashlib
import json
import signal
import threading
import time
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer

from scalping_bot import __version__
from scalping_bot.config import Settings
from scalping_bot.data.acquisition import (
    ResearchArchiveCatalog,
    ResearchArchiveMaterializer,
    latest_research_catalog,
    research_archive_requests,
    select_archive_kinds,
    select_research_periods,
)
from scalping_bot.data.capture import freeze_capture_cohort as build_capture_cohort
from scalping_bot.data.live import BinancePublicCollector
from scalping_bot.data.storage import LocalObjectStore, object_store
from scalping_bot.domain.market_context import MarketContext
from scalping_bot.execution.binance import BinanceSpotTestnetAdapter, OfficialSpotTransport
from scalping_bot.execution.executor import ExecutionLimits
from scalping_bot.execution.runtime import run_executor
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.execution.testnet_contract import (
    probe_testnet_user_data_reconnect,
    run_testnet_contract_scenarios,
)
from scalping_bot.operations.backup import EncryptedSQLiteBackup
from scalping_bot.operations.deployment import DeploymentGate
from scalping_bot.operations.durable_shadow import DurableLiveShadow
from scalping_bot.operations.durable_soak import DurableTestnetSoak
from scalping_bot.operations.live_shadow_runtime import (
    ProductionLiveShadowStream,
    SharedEngineLiveShadow,
    parse_shadow_event,
)
from scalping_bot.operations.operator_channel import OperatorCommandStore
from scalping_bot.operations.promoted_risk_trial import run_promoted_risk_trial
from scalping_bot.operations.promotion import PromotedRiskGate, RiskEvaluation
from scalping_bot.operations.release import ReleaseVerifier
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.costs import (
    AccountCommissionSnapshot,
    CalibrationBucketPlan,
    CalibrationObservation,
    ExecutionCostCalibrator,
    ExecutionCostModel,
    ExecutionCostModelPublisher,
    calibration_observations_from_capture,
    capture_account_commission_snapshot,
)
from scalping_bot.research.evidence import CandidateLifecycle, CandidateRecord
from scalping_bot.research.execution import canonical_aggregate_trade_bytes
from scalping_bot.research.holdout import FinalHoldoutGate
from scalping_bot.research.runner import ResearchBatchRunner, load_aggtrades
from scalping_bot.research.workflow import ResearchArtifactWriter, ResearchRunManifest
from scalping_bot.strategies.book_shadow import (
    DEFAULT_BOOK_CONFIRMATION_IMBALANCE,
    BookShadowEvaluator,
    BookShadowProtocol,
    ShadowBookConfirmation,
)

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
    s3_bucket: Annotated[str | None, typer.Option("--s3-bucket")] = None,
    s3_prefix: Annotated[str, typer.Option("--s3-prefix")] = "",
    s3_endpoint: Annotated[str | None, typer.Option("--s3-endpoint")] = None,
    s3_region: Annotated[str | None, typer.Option("--s3-region")] = None,
) -> None:
    """Continuously capture public production Trade Flow and Book Flow."""
    collector = BinancePublicCollector(
        object_store(
            local_root=storage,
            bucket=s3_bucket,
            prefix=s3_prefix,
            endpoint_url=s3_endpoint,
            region_name=s3_region,
        )
    )
    asyncio.run(collector.run_forever())


@app.command()
def materialize_research_archive(
    output: Path = Path("data/research-market"),
    allow_incomplete: bool = typer.Option(
        False, help="Persist a resumable partial catalog instead of failing on gaps"
    ),
    periods: Annotated[
        list[str] | None,
        typer.Option(
            "--period",
            help="Period name; repeat for historical-stress, primary-research, or final-holdout",
        ),
    ] = None,
    kinds: Annotated[
        list[str] | None,
        typer.Option("--kind", help="Archive kind: klines or aggTrades"),
    ] = None,
    catalog_key: Annotated[
        str | None,
        typer.Option("--catalog", help="Existing immutable catalog key to resume"),
    ] = None,
    s3_bucket: Annotated[str | None, typer.Option("--s3-bucket")] = None,
    s3_prefix: Annotated[str, typer.Option("--s3-prefix")] = "",
    s3_endpoint: Annotated[str | None, typer.Option("--s3-endpoint")] = None,
    s3_region: Annotated[str | None, typer.Option("--s3-region")] = None,
) -> None:
    """Acquire official klines and aggregate trades into an immutable catalog."""
    try:
        selected_periods = select_research_periods(periods)
        selected_kinds = select_archive_kinds(kinds)
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error
    store = object_store(
        local_root=output,
        bucket=s3_bucket,
        prefix=s3_prefix,
        endpoint_url=s3_endpoint,
        region_name=s3_region,
    )
    existing: ResearchArchiveCatalog | None = None
    if catalog_key is not None:
        existing = ResearchArchiveCatalog.load(store, catalog_key)
    else:
        existing = latest_research_catalog(store)
    requests = research_archive_requests(selected_periods, kinds=selected_kinds)
    catalog = ResearchArchiveMaterializer(store).materialize(
        requests, existing=existing, require_complete=not allow_incomplete
    )
    typer.echo(
        f"catalog={catalog.catalog_key} missing={len(catalog.missing)} gaps={len(catalog.gaps)}"
    )


@app.command()
def freeze_capture_cohort(
    purpose: Annotated[str, typer.Argument()],
    streams: Annotated[list[str], typer.Option("--stream")],
    start_ms: Annotated[int, typer.Option("--start-ms")],
    end_ms: Annotated[int, typer.Option("--end-ms")],
    storage: Annotated[Path, typer.Option("--storage")] = Path("data"),
    s3_bucket: Annotated[str | None, typer.Option("--s3-bucket")] = None,
    s3_prefix: Annotated[str, typer.Option("--s3-prefix")] = "",
    s3_endpoint: Annotated[str | None, typer.Option("--s3-endpoint")] = None,
    s3_region: Annotated[str | None, typer.Option("--s3-region")] = None,
) -> None:
    """Freeze valid capture segments for calibration or Book Flow analysis."""
    if not streams:
        raise typer.BadParameter("at least one --stream is required")
    try:
        store = object_store(
            local_root=storage,
            bucket=s3_bucket,
            prefix=s3_prefix,
            endpoint_url=s3_endpoint,
            region_name=s3_region,
        )
        cohort = build_capture_cohort(
            store,
            purpose=purpose,
            streams=tuple(streams),
            start_ms=start_ms,
            end_ms=end_ms,
        )
        key = cohort.persist(store)
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(
        f"capture-cohort={key} segments={len(cohort.segments)} excluded={len(cohort.excluded)}"
    )


@app.command()
def publish_cost_model(
    observations: Annotated[Path, typer.Argument(exists=True, readable=True)],
    commission: Annotated[Path, typer.Argument(exists=True, readable=True)],
    output: Path = Path("data/cost-model"),
    captured_ms: int = typer.Option(..., help="Capture time for the Account Commission Snapshot"),
    bucket_plan_key: str | None = typer.Option(None, "--bucket-plan"),
    source_storage: Annotated[
        Path,
        typer.Option("--source-storage", help="Immutable object-store root containing sources"),
    ] = Path("data"),
) -> None:
    """Calibrate explicit observations only when every source resolves immutably."""
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
    if not parsed or any(
        not item.get("source") for item in raw_observations if isinstance(item, dict)
    ):
        raise typer.BadParameter(
            "every calibration observation must cite immutable source evidence"
        )
    source_store = LocalObjectStore(source_storage)
    if bucket_plan_key is None:
        raise typer.BadParameter("--bucket-plan is required to publish production cost evidence")
    bucket_plan = CalibrationBucketPlan.load(source_store, bucket_plan_key)
    source_evidence = tuple(
        str(item["source"])
        for item in raw_observations
        if isinstance(item, dict) and item.get("source")
    )
    try:
        for source in source_evidence:
            source_store.read(source)
    except (FileNotFoundError, KeyError, ValueError) as error:
        raise typer.BadParameter(
            "every calibration source must resolve in the immutable source storage"
        ) from error
    snapshot = AccountCommissionSnapshot.from_binance_response(
        raw_commission, captured_ms=captured_ms, account_context="configured-account"
    )
    model = ExecutionCostCalibrator().fit(
        parsed,
        commission=snapshot,
        bucket_plan=bucket_plan,
    )
    key = ExecutionCostModelPublisher(LocalObjectStore(output)).publish(
        model, source_evidence=(*source_evidence, bucket_plan_key)
    )
    typer.echo(f"cost-model={key}")


@app.command()
def capture_commission_snapshot(
    output: Path = Path("data/commission"),
    account_context: str = typer.Option("configured-account", "--account-context"),
    captured_ms: int | None = typer.Option(None, "--captured-ms"),
) -> None:
    """Capture the configured account commission from Binance Spot Testnet."""
    settings = Settings()
    if not settings.binance_api_key or not settings.binance_api_secret:
        raise RuntimeError("Binance Testnet credentials are required for commission capture")
    transport = OfficialSpotTransport(
        api_key=settings.binance_api_key, api_secret=settings.binance_api_secret
    )
    snapshot = capture_account_commission_snapshot(
        transport,
        symbol=settings.symbol,
        captured_ms=captured_ms if captured_ms is not None else _now_ms(),
        account_context=account_context,
    )
    key = snapshot.persist(LocalObjectStore(output))
    typer.echo(f"commission-snapshot={key}")


@app.command()
def freeze_calibration_plan(
    output: Annotated[Path, typer.Option("--output")] = Path("data/cost-model"),
    hours_utc: Annotated[list[int] | None, typer.Option("--hour")] = None,
    volatility_buckets: Annotated[list[str] | None, typer.Option("--volatility")] = None,
    size_buckets: Annotated[list[str] | None, typer.Option("--size")] = None,
    minimum_observations: int = typer.Option(1, "--minimum-observations"),
) -> None:
    """Freeze the required production evidence buckets before calibration capture."""
    plan = CalibrationBucketPlan(
        hours_utc=tuple(hours_utc or range(24)),
        volatility_buckets=tuple(volatility_buckets or ("low", "medium", "high")),
        size_buckets=tuple(size_buckets or ("small", "medium", "large")),
        minimum_observations=minimum_observations,
    )
    typer.echo(f"calibration-plan={plan.persist(LocalObjectStore(output))}")


@app.command()
def publish_cost_model_from_capture(
    commission: Annotated[Path, typer.Argument(exists=True, readable=True)],
    storage: Path = Path("data"),
    output: Path = Path("data/cost-model"),
    captured_ms: int = typer.Option(..., help="Capture time for the Account Commission Snapshot"),
    bucket_plan_key: str | None = typer.Option(None, "--bucket-plan"),
    cohort_key: str | None = typer.Option(None, "--cohort"),
) -> None:
    """Calibrate costs from valid immutable production capture and account commission."""
    raw_commission = json.loads(commission.read_text(encoding="utf-8"))
    if not isinstance(raw_commission, dict):
        raise typer.BadParameter("commission must be a Binance commission JSON object")
    source_store = LocalObjectStore(storage)
    if bucket_plan_key is None:
        raise typer.BadParameter("--bucket-plan is required to publish production cost evidence")
    bucket_plan = CalibrationBucketPlan.load(source_store, bucket_plan_key)
    observations, evidence = calibration_observations_from_capture(
        source_store, cohort_key=cohort_key
    )
    if not observations:
        raise RuntimeError("capture contains no quote-matched aggregate-trade observations")
    snapshot = AccountCommissionSnapshot.from_binance_response(
        raw_commission, captured_ms=captured_ms, account_context="configured-account"
    )
    model = ExecutionCostCalibrator().fit(
        observations,
        commission=snapshot,
        bucket_plan=bucket_plan,
    )
    key = ExecutionCostModelPublisher(LocalObjectStore(output)).publish(
        model, source_evidence=(*evidence, bucket_plan_key)
    )
    typer.echo(f"cost-model={key} observations={len(observations)}")


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
    if not historical_parquet:
        raise RuntimeError("research requires the complete Historical Stress parquet input set")
    if not manifest.verify_dataset_paths(
        tuple(parquet),
        store,
        historical_paths=tuple(historical_parquet or ()),
    ):
        raise RuntimeError(
            "research Parquet inputs are not the immutable files bound by the manifest"
        )
    model = ExecutionCostModel.load(store, manifest.execution_cost_model_key)
    batch = first_batch()
    if not manifest.verify_candidate_batch(batch):
        raise RuntimeError("the preregistered candidate batch does not match the manifest")
    evaluation = ResearchBatchRunner(
        execution_costs=model.as_engine_costs(),
        manifest_store=store,
        code_commit=manifest.code_commit,
        dataset_checksums=manifest.dataset_checksums,
        random_seed=manifest.random_seed,
        run_identity=manifest.identity,
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
    target_schema: int = 7,
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
    channel = OperatorCommandStore(_operator_channel_path(database))
    try:
        command_id = channel.enqueue(kind="pause", reason=reason, event_time_ms=_now_ms())
    finally:
        channel.close()
    typer.echo(f"operator command queued id={command_id}")


@app.command()
def flatten_and_lock(
    database: Path = Path("state/scalping-bot.db"),
    reason: str = typer.Option(...),
) -> None:
    """Cancel pending work, flatten exposure through the executor, and lock entries."""
    channel = OperatorCommandStore(_operator_channel_path(database))
    try:
        command_id = channel.enqueue(
            kind="flatten-and-lock", reason=reason, event_time_ms=_now_ms()
        )
    finally:
        channel.close()
    typer.echo(f"operator command queued id={command_id}")


@app.command()
def request_reconciliation(database: Path = Path("state/scalping-bot.db")) -> None:
    """Request an explicit exchange reconciliation and block new entries."""
    channel = OperatorCommandStore(_operator_channel_path(database))
    try:
        command_id = channel.enqueue(kind="reconcile", event_time_ms=_now_ms())
    finally:
        channel.close()
    typer.echo(f"operator command queued id={command_id}")


@app.command()
def unlock_executor(
    database: Path = Path("state/scalping-bot.db"),
    operator_confirmed: bool = typer.Option(...),
) -> None:
    """Unlock entries only after a consistent reconciliation and Operator confirmation."""
    channel = OperatorCommandStore(_operator_channel_path(database))
    try:
        command_id = channel.enqueue(
            kind="unlock", event_time_ms=_now_ms(), operator_confirmed=operator_confirmed
        )
    finally:
        channel.close()
    typer.echo(f"operator command queued id={command_id}")


@app.command()
def run_live_shadow(
    state: Path = Path("state/live-shadow.db"),
    event_file: Annotated[Path | None, typer.Option(exists=True, readable=True)] = None,
    finalize_at_ms: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Run restartable no-order Live Shadow from production or raw event observations."""
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
    runner = SharedEngineLiveShadow(
        process,
        candidate_manifest=settings.candidate_manifest,
        cost_model=Path(settings.cost_model_snapshot),
    )
    try:
        if event_file is not None:
            context = MarketContext()
            for line in event_file.read_text(encoding="utf-8").splitlines():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError("Live Shadow event files must contain JSON objects")
                runner.process_event(
                    parse_shadow_event(value, context=context),
                    receipt_time_ms=int(str(value.get("receipt_time_ms", _now_ms()))),
                )
            if finalize_at_ms is not None:
                typer.echo(f"live-shadow-report={process.finalize(now_ms=finalize_at_ms)}")
            else:
                typer.echo("live-shadow observations persisted")
            return
        typer.echo("live-shadow process running; press Ctrl-C to stop")
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_args: stop.set())
        signal.signal(signal.SIGINT, lambda *_args: stop.set())
        ProductionLiveShadowStream(runner).run_forever(stop)
    except KeyboardInterrupt:
        return
    finally:
        process.close()
        if settings.backup_encryption_key:
            backup = EncryptedSQLiteBackup(settings.backup_encryption_key.encode())
            artifact = backup.create(
                state,
                destination=state.parent / "backups" / "live-shadow",
                created_ms=_now_ms(),
            )
            backup.publish_latest(artifact)


@app.command()
def freeze_book_shadow_protocol(
    candidates: Annotated[list[str] | None, typer.Option("--candidate")] = None,
    start_ms: Annotated[int, typer.Option(..., "--start-ms")] = 0,
    end_ms: Annotated[int, typer.Option(..., "--end-ms")] = 0,
    min_imbalance: Annotated[str, typer.Option("--min-imbalance")] = str(
        DEFAULT_BOOK_CONFIRMATION_IMBALANCE
    ),
    max_gap_ms: Annotated[int, typer.Option("--max-gap-ms")] = 60_000,
    minimum_valid_coverage: Annotated[str, typer.Option("--minimum-valid-coverage")] = "0",
    storage: Annotated[Path, typer.Option("--storage")] = Path("data"),
) -> None:
    """Freeze and persist the audited Book Flow Shadow protocol."""
    if not candidates:
        raise typer.BadParameter("at least one --candidate is required")
    if start_ms <= 0 or end_ms <= 0:
        raise typer.BadParameter("--start-ms and --end-ms must be provided as positive timestamps")
    protocol = BookShadowProtocol(
        candidate_identities=tuple(sorted(candidates)),
        start_ms=start_ms,
        end_ms=end_ms,
        min_imbalance=Decimal(min_imbalance),
        max_gap_ms=max_gap_ms,
        minimum_valid_coverage=Decimal(minimum_valid_coverage),
    )
    key = protocol.persist(LocalObjectStore(storage))
    typer.echo(f"book-shadow-protocol={key}")


@app.command()
def evaluate_book_shadow(
    storage: Annotated[Path, typer.Option("--storage")] = Path("data"),
    protocol_key: Annotated[str, typer.Option("--protocol")] = "",
    entry_proposals: Annotated[
        Path | None,
        typer.Option(
            "--entry-proposals",
            help="JSON object mapping captured event times to frozen candidate proposals",
        ),
    ] = None,
) -> None:
    """Build non-promotable Book Flow Shadow evidence from captured segments."""
    if entry_proposals is None:
        raise typer.BadParameter("Book Shadow requires frozen candidate Entry Proposals")
    if not protocol_key:
        raise typer.BadParameter("Book Shadow requires a frozen --protocol")
    raw_proposals = json.loads(entry_proposals.read_text(encoding="utf-8"))
    if not isinstance(raw_proposals, dict) or any(
        not isinstance(value, bool) for value in raw_proposals.values()
    ):
        raise typer.BadParameter("entry proposals must be a JSON object of boolean decisions")
    proposals = {int(str(key)): value for key, value in raw_proposals.items()}
    store = LocalObjectStore(storage)
    protocol = BookShadowProtocol.load(store, protocol_key)
    evaluator = BookShadowEvaluator.from_capture(
        store,
        protocol=protocol,
        confirmation=ShadowBookConfirmation(min_imbalance=protocol.min_imbalance),
        entry_proposal=lambda event_time_ms, _features: proposals.get(event_time_ms, False),
    )
    typer.echo(f"book-shadow-report={evaluator.persist_report(store)}")


@app.command()
def evaluate_final_holdout(
    candidate: Annotated[Path, typer.Argument(exists=True, readable=True)],
    parquet: Annotated[list[Path], typer.Argument(exists=True, readable=True)],
    dataset_catalog_key: Annotated[str, typer.Option("--dataset-catalog-key")],
    execution_cost_model_key: Annotated[str, typer.Option("--execution-cost-model-key")],
    artifacts: Path = Path("data/research-artifacts"),
    actor: str = typer.Option(...),
) -> None:
    """Reveal and evaluate the immutable Final Holdout through the shared engine."""
    value = json.loads(candidate.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise typer.BadParameter("candidate must be a JSON object")
    record = _candidate_record_from_json(value)
    aggregate_trades = load_aggtrades(tuple(parquet))
    dataset_bytes = canonical_aggregate_trade_bytes(aggregate_trades)
    dataset_checksum = hashlib.sha256(dataset_bytes).hexdigest()
    store = LocalObjectStore(artifacts)
    dataset_key = f"holdout-datasets/{dataset_checksum}.json"
    store.put_immutable(dataset_key, dataset_bytes)
    decision = FinalHoldoutGate(store).evaluate_aggregate_trades(
        record,
        aggregate_trades=aggregate_trades,
        holdout_parquet_paths=tuple(parquet),
        holdout_catalog_key=dataset_catalog_key,
        holdout_dataset_key=dataset_key,
        holdout_dataset_checksum=dataset_checksum,
        execution_cost_model_key=execution_cost_model_key,
        actor=actor,
    )
    typer.echo(f"final-holdout={decision.report_key} decision={decision.decision}")


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
    baseline_evaluation = RiskEvaluation.from_mapping(baseline_value)
    promoted_evaluation = RiskEvaluation.from_mapping(promoted_value)
    if (
        not baseline_evaluation.experiment_manifest_key
        or not promoted_evaluation.experiment_manifest_key
    ):
        raise typer.BadParameter(
            "both risk evaluations must reference content-addressed experiment manifests"
        )
    gate = PromotedRiskGate(store)
    result = gate.evaluate(
        candidate_identity,
        baseline=baseline_evaluation,
        promoted=promoted_evaluation,
        baseline_promoted=gate.promotion_evidence_valid(
            promotion_artifact_key,
            candidate_identity,
            baseline_evaluation.candidate_rules_sha256,
        ),
        soak_complete=gate.soak_evidence_valid(soak_report_key, candidate_identity),
        promotion_artifact_key=promotion_artifact_key,
        soak_report_key=soak_report_key,
    )
    report_key = f"{result.manifest_key.removesuffix('.json')}.md"
    store.put_immutable(report_key, result.report.encode())
    typer.echo(f"promoted-risk={report_key} accepted={result.accepted}")


@app.command("run-promoted-risk-trial")
def run_promoted_risk_trial_command(
    candidate_manifest: Annotated[Path, typer.Argument(exists=True, readable=True)],
    historical_parquet: Annotated[
        list[Path], typer.Option("--historical-parquet", exists=True, readable=True)
    ],
    execution_cost_model_key: Annotated[str, typer.Option("--execution-cost-model-key")],
    promotion_artifact_key: Annotated[str, typer.Option("--promotion-artifact-key")],
    soak_report_key: Annotated[str, typer.Option("--soak-report-key")],
    testnet_baseline: Annotated[Path, typer.Option(exists=True, readable=True)],
    testnet_promoted: Annotated[Path, typer.Option(exists=True, readable=True)],
    code_commit: Annotated[str, typer.Option("--code-commit")],
    artifacts: Path = Path("data/promoted-risk"),
) -> None:
    """Run the engine-backed 0.25%/0.5% comparison and persist its decision."""
    if not historical_parquet:
        raise typer.BadParameter("at least one Historical Stress parquet file is required")
    trial = run_promoted_risk_trial(
        store=LocalObjectStore(artifacts),
        candidate_manifest_path=candidate_manifest,
        historical_parquet=tuple(historical_parquet),
        execution_cost_model_key=execution_cost_model_key,
        promotion_artifact_key=promotion_artifact_key,
        soak_report_key=soak_report_key,
        testnet_baseline_path=testnet_baseline,
        testnet_promoted_path=testnet_promoted,
        code_commit=code_commit,
    )
    report_key = f"{trial.result.manifest_key.removesuffix('.json')}.md"
    LocalObjectStore(artifacts).put_immutable(report_key, trial.result.report.encode())
    typer.echo(
        f"promoted-risk={report_key} accepted={trial.result.accepted} "
        f"baseline-risk={trial.result.proven_baseline_risk}"
    )


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
    manifest_key: Annotated[str, typer.Argument()],
    parquet: Annotated[list[Path], typer.Argument(exists=True, readable=True)],
    artifacts: Path = Path("data/research-artifacts"),
    historical_parquet: Annotated[
        list[Path] | None,
        typer.Option(exists=True, readable=True, help="Historical Stress parquet inputs"),
    ] = None,
) -> None:
    """Strict alias for research evaluation from a verified immutable manifest."""
    run_research(manifest_key, parquet, artifacts, historical_parquet)


@app.command()
def run_testnet_executor() -> None:
    """Run the single-writer reconciled Binance Testnet executor."""
    run_executor(Settings())


@app.command("testnet-contract-scenarios")
def testnet_contract_scenarios(
    confirm_testnet: bool = typer.Option(
        False,
        "--confirm-testnet",
        help="Confirm that this command may contact Binance Spot Testnet.",
    ),
    execute_order_round_trip: bool = typer.Option(
        False,
        "--execute-order-round-trip",
        help="Submit, protect, cancel protection, and flatten a small Testnet order.",
    ),
    quantity: str = typer.Option("0.0001", "--quantity"),
) -> None:
    """Run explicit Binance Spot Testnet REST and User Data contract checks."""
    if not confirm_testnet:
        raise typer.BadParameter("--confirm-testnet is required; no network call was made")
    settings = Settings()
    if not settings.binance_api_key or not settings.binance_api_secret:
        raise RuntimeError("Binance Testnet credentials are required for contract scenarios")
    transport = OfficialSpotTransport(
        api_key=settings.binance_api_key, api_secret=settings.binance_api_secret
    )
    adapter = BinanceSpotTestnetAdapter(endpoint=settings.binance_rest_url, transport=transport)

    def reconnect_probe(listen_key: str) -> None:
        probe_testnet_user_data_reconnect(settings.binance_ws_url, listen_key)

    report = run_testnet_contract_scenarios(
        adapter,
        confirm_testnet=True,
        reconnect_probe=reconnect_probe,
        execute_order_round_trip=execute_order_round_trip,
        quantity=quantity,
    )
    typer.echo(json.dumps(asdict(report), sort_keys=True))


def _candidate_record_from_json(value: dict[str, object]) -> CandidateRecord:
    try:
        original_rules = value["original_rules"]
        modifications = value["modifications"]
        parameters = value["parameters"]
        rejections = value["rejection_criteria"]
        dependencies = value.get("market_data_dependencies", [])
        if (
            not isinstance(original_rules, dict)
            or not isinstance(modifications, dict)
            or not isinstance(parameters, dict)
            or not isinstance(rejections, list)
            or not isinstance(dependencies, list)
        ):
            raise ValueError("candidate JSON contains malformed rule fields")
        return CandidateRecord(
            candidate_id=str(value["candidate_id"]),
            version=int(str(value["version"])),
            family=str(value["family"]),
            source_uri=str(value["source_uri"]),
            source_license=str(value["source_license"]),
            thesis=str(value["thesis"]),
            original_rules=original_rules,
            modifications=modifications,
            parameters=parameters,
            rejection_criteria=tuple(str(item) for item in rejections),
            lifecycle=CandidateLifecycle(str(value["lifecycle"])),
            frozen=bool(value["frozen"]),
            market_data_dependencies=tuple(str(item) for item in dependencies),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise typer.BadParameter(
            "candidate JSON does not describe a valid frozen finalist"
        ) from error


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _operator_channel_path(database: Path) -> Path:
    return database.with_suffix(database.suffix + ".operator.db")


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
