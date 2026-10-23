import typer

from scalping_bot import __version__
from scalping_bot.config import Settings

app = typer.Typer(no_args_is_help=True)


@app.callback()
def main() -> None:
    """Research and Binance Spot Testnet operations."""


@app.command()
def check_config() -> None:
    """Validate configuration without opening a network connection."""
    settings = Settings()
    typer.echo(f"scalping-bot {__version__}: {settings.symbol} Testnet configuration is safe")


if __name__ == "__main__":
    app()
