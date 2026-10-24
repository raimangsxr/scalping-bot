import asyncio
from pathlib import Path

import typer

from scalping_bot import __version__
from scalping_bot.config import Settings
from scalping_bot.data.live import BinancePublicCollector
from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.execution.runtime import run_executor
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.deployment import DeploymentGate

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
    target_schema: int = 2,
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
def run_testnet_executor() -> None:
    """Run the single-writer reconciled Binance Testnet executor."""
    run_executor(Settings())


if __name__ == "__main__":
    app()
