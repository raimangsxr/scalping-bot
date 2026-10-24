from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Self
from urllib.parse import urlparse

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SCALPING_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
    )

    symbol: str = "BTCUSDT"
    binance_rest_url: str = "https://testnet.binance.vision"
    binance_ws_url: str = "wss://stream.testnet.binance.vision/ws"
    binance_api_key: str | None = None
    binance_api_secret: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    backup_encryption_key: str | None = None
    state_path: Path = Path("state/scalping-bot.db")
    position_risk: str = "0.0025"
    candidate_manifest: Path | None = None
    initial_equity: str = "10000"
    calibrated_half_spread_rate: str | None = None
    calibrated_slippage_rate: str | None = None
    cost_model_snapshot: str | None = None

    @model_validator(mode="after")
    def validate_safe_scope(self) -> Self:
        if self.symbol != "BTCUSDT":
            raise ValueError("the first version accepts BTCUSDT only")
        try:
            configured_risk = Decimal(self.position_risk)
        except (InvalidOperation, ValueError):
            raise ValueError("position risk must be 0.25% in the first Testnet phase") from None
        if configured_risk != Decimal("0.0025"):
            raise ValueError("position risk must be 0.25% in the first Testnet phase")
        allowed_hosts = {"testnet.binance.vision", "stream.testnet.binance.vision"}
        for endpoint in (self.binance_rest_url, self.binance_ws_url):
            if urlparse(endpoint).hostname not in allowed_hosts:
                raise ValueError("endpoint must be an official Binance Spot Testnet endpoint")
        return self
