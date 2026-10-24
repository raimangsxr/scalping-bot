from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse

from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import SPOT_REST_API_TESTNET_URL
from binance_sdk_spot.spot import Spot

TESTNET_HOST = "testnet.binance.vision"


class UnsafeEndpointError(ValueError):
    pass


class SpotTransport(Protocol):
    def exchange_info(self, symbol: str) -> dict[str, object]: ...

    def account_commission(self, symbol: str) -> dict[str, object]: ...

    def account(self) -> dict[str, object]: ...

    def open_orders(self, symbol: str) -> list[dict[str, object]]: ...

    def trades(self, symbol: str) -> list[dict[str, object]]: ...


class OfficialSpotTransport:
    """Contains every official Binance SDK type behind plain dictionaries."""

    def __init__(self, *, api_key: str, api_secret: str) -> None:
        configuration = ConfigurationRestAPI(
            api_key=api_key,
            api_secret=api_secret,
            base_path=SPOT_REST_API_TESTNET_URL,
            timeout=10_000,
            retries=0,
        )
        self._client = Spot(config_rest_api=configuration).rest_api

    def exchange_info(self, symbol: str) -> dict[str, object]:
        return _plain(self._client.exchange_info(symbol=symbol).data())

    def account_commission(self, symbol: str) -> dict[str, object]:
        return _plain(self._client.account_commission(symbol=symbol).data())

    def account(self) -> dict[str, object]:
        return _plain(self._client.get_account(omit_zero_balances=False, recv_window=5000).data())

    def open_orders(self, symbol: str) -> list[dict[str, object]]:
        value = self._client.get_open_orders(symbol=symbol, recv_window=5000).data()
        return _plain_list(value)

    def trades(self, symbol: str) -> list[dict[str, object]]:
        value = self._client.my_trades(symbol=symbol, limit=1000, recv_window=5000).data()
        return _plain_list(value)


@dataclass(frozen=True)
class BinanceCapabilities:
    symbol_status: str
    order_types: tuple[str, ...]
    step_size: str
    min_notional: str
    maker_commission: str
    taker_commission: str
    request_weight_limit: int


class BinanceSpotTestnetAdapter:
    def __init__(self, *, endpoint: str, transport: SpotTransport) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme != "https" or parsed.hostname != TESTNET_HOST:
            raise UnsafeEndpointError("only the official Binance Spot Testnet endpoint is allowed")
        self.endpoint = endpoint
        self.transport = transport
        self.symbol = "BTCUSDT"

    def load_capabilities(self) -> BinanceCapabilities:
        info = self.transport.exchange_info(self.symbol)
        symbols = info.get("symbols")
        if not isinstance(symbols, list) or len(symbols) != 1:
            raise ValueError("BTCUSDT exchange information is missing")
        symbol = symbols[0]
        if not isinstance(symbol, dict) or symbol.get("symbol") != self.symbol:
            raise ValueError("unexpected exchange symbol")
        filters = {
            item["filterType"]: item
            for item in symbol.get("filters", [])
            if isinstance(item, dict) and "filterType" in item
        }
        limits = info.get("rateLimits")
        weight = (
            next(
                (item["limit"] for item in limits if item.get("rateLimitType") == "REQUEST_WEIGHT"),
                None,
            )
            if isinstance(limits, list)
            else None
        )
        commission = self.transport.account_commission(self.symbol)
        standard = commission.get("standardCommission")
        if not isinstance(standard, dict):
            raise ValueError("account commission response is missing standard rates")
        if not isinstance(weight, int):
            raise ValueError("request-weight limit is missing")
        return BinanceCapabilities(
            symbol_status=str(symbol["status"]),
            order_types=tuple(str(item) for item in symbol["orderTypes"]),
            step_size=str(filters["LOT_SIZE"]["stepSize"]),
            min_notional=str(filters["NOTIONAL"]["minNotional"]),
            maker_commission=str(standard["maker"]),
            taker_commission=str(standard["taker"]),
            request_weight_limit=weight,
        )

    def reconciliation_snapshot(self) -> dict[str, object]:
        return {
            "account": self.transport.account(),
            "open_orders": self.transport.open_orders(self.symbol),
            "trades": self.transport.trades(self.symbol),
        }


def _plain(model: object) -> dict[str, object]:
    if hasattr(model, "to_dict"):
        value = model.to_dict()
        if isinstance(value, dict):
            return value
    if isinstance(model, dict):
        return model
    raise TypeError(f"SDK returned unsupported model {type(model).__name__}")


def _plain_list(model: object) -> list[dict[str, object]]:
    if hasattr(model, "to_dict"):
        model = model.to_dict()
    if not isinstance(model, list):
        raise TypeError(f"SDK returned unsupported list model {type(model).__name__}")
    return [_plain(item) for item in model]
