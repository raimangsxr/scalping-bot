import asyncio
from pathlib import Path

import typer

from scalping_bot import __version__
from scalping_bot.config import Settings
from scalping_bot.data.live import BinancePublicCollector
from scalping_bot.data.storage import LocalObjectStore

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


if __name__ == "__main__":
    app()
