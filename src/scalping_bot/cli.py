import asyncio
import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer

from scalping_bot import __version__
from scalping_bot.config import Settings
from scalping_bot.data.live import BinancePublicCollector
from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.domain.models import ExecutionCosts
from scalping_bot.execution.runtime import run_executor
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.deployment import DeploymentGate
from scalping_bot.research.reporting import render_evidence_markdown
from scalping_bot.research.runner import ResearchBatchRunner, load_aggtrades

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
def collect_live(storage: Path = Path("data")) -> None:
    """Continuously capture public production Trade Flow and Book Flow."""
    collector = BinancePublicCollector(LocalObjectStore(storage))
    asyncio.run(collector.run_forever())


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
) -> None:
    """Evaluate all preregistered variants with aggregate trades and shared engine."""
    trades = load_aggtrades(tuple(parquet))
    evaluation = ResearchBatchRunner(
        execution_costs=ExecutionCosts(
            commission_rate=Decimal(commission_rate),
            half_spread_rate=Decimal(half_spread_rate),
            slippage_rate=Decimal(slippage_rate),
        )
    ).run(trades)
    output.mkdir(parents=True, exist_ok=True)
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
        for attempt in evaluation.ledger.attempts
    ]
    (output / "attempts.json").write_text(
        json.dumps(attempts, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    typer.echo(f"evaluated {len(evaluation.evidence)} candidates into {output}")


@app.command()
def run_testnet_executor() -> None:
    """Run the single-writer reconciled Binance Testnet executor."""
    run_executor(Settings())


if __name__ == "__main__":
    app()
