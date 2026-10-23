from pathlib import Path

import pytest
from pydantic import ValidationError

from scalping_bot.config import Settings


def test_testnet_configuration_is_accepted(tmp_path: Path) -> None:
    settings = Settings(
        binance_rest_url="https://testnet.binance.vision",
        binance_ws_url="wss://stream.testnet.binance.vision/ws",
        state_path=tmp_path / "state.db",
    )

    assert settings.symbol == "BTCUSDT"
    assert settings.position_risk == "0.0025"


@pytest.mark.parametrize(
    ("field", "url"),
    [
        ("binance_rest_url", "https://api.binance.com"),
        ("binance_ws_url", "wss://stream.binance.com:9443/ws"),
        ("binance_rest_url", "https://example.testnet.invalid"),
    ],
)
def test_non_testnet_endpoints_are_rejected_before_network_use(
    tmp_path: Path, field: str, url: str
) -> None:
    values: dict[str, object] = {
        "binance_rest_url": "https://testnet.binance.vision",
        "binance_ws_url": "wss://stream.testnet.binance.vision/ws",
        "state_path": tmp_path / "state.db",
    }
    values[field] = url

    with pytest.raises(ValidationError, match="Binance Spot Testnet"):
        Settings(**values)  # type: ignore[arg-type]


def test_only_the_research_market_is_accepted(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="BTCUSDT"):
        Settings(symbol="ETHUSDT", state_path=tmp_path / "state.db")
