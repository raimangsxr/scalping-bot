# scalping-bot

A deterministic Python 3.13 platform for researching `BTCUSDT` Spot scalps and
exercising their complete lifecycle on **Binance Testnet only**. Production
Binance endpoints are rejected during configuration validation.

## Local setup

```bash
uv sync --all-groups
uv run scalping-bot check-config
uv run pytest
```

Configuration uses `SCALPING_`-prefixed environment variables. Credentials are
optional for research and must be injected through local or deployment secrets;
they are never stored in repository files or rendered by the CLI.
