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
    state_path: Path = Path("state/scalping-bot.db")
    position_risk: str = "0.0025"

    @model_validator(mode="after")
    def validate_safe_scope(self) -> Self:
        if self.symbol != "BTCUSDT":
            raise ValueError("the first version accepts BTCUSDT only")
        allowed_hosts = {"testnet.binance.vision", "stream.testnet.binance.vision"}
        for endpoint in (self.binance_rest_url, self.binance_ws_url):
            if urlparse(endpoint).hostname not in allowed_hosts:
                raise ValueError("endpoint must be an official Binance Spot Testnet endpoint")
        return self
