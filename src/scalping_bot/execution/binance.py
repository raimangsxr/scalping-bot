from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from urllib.parse import urlparse

from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import SPOT_REST_API_TESTNET_URL
from binance_sdk_spot.rest_api.models.enums import (
    NewOrderNewOrderRespTypeEnum,
    NewOrderSideEnum,
    NewOrderTypeEnum,
    OrderOcoNewOrderRespTypeEnum,
    OrderOcoSideEnum,
    OrderOcoStopLimitTimeInForceEnum,
)
from binance_sdk_spot.spot import Spot

from scalping_bot.execution.executor import ExecutionFill, OrderResult

TESTNET_HOST = "testnet.binance.vision"


class UnsafeEndpointError(ValueError):
    pass


class SpotTransport(Protocol):
    def exchange_info(self, symbol: str) -> dict[str, object]: ...

    def account_commission(self, symbol: str) -> dict[str, object]: ...

    def account(self) -> dict[str, object]: ...

    def open_orders(self, symbol: str) -> list[dict[str, object]]: ...

    def trades(self, symbol: str) -> list[dict[str, object]]: ...

    def submit_market_buy(
        self, symbol: str, client_order_id: str, quantity: str
    ) -> OrderResult: ...

    def query_order(self, symbol: str, client_order_id: str) -> OrderResult: ...

    def place_oco_protection(
        self,
        symbol: str,
        client_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
    ) -> str: ...

    def flatten_market_sell(
        self, symbol: str, client_order_id: str, quantity: str
    ) -> OrderResult: ...

    def cancel_protection(self, symbol: str, client_order_id: str) -> None: ...


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

    def submit_market_buy(self, symbol: str, client_order_id: str, quantity: str) -> OrderResult:
        response = _plain(
            self._client.new_order(
                symbol=symbol,
                side=NewOrderSideEnum.BUY,
                type=NewOrderTypeEnum.MARKET,
                quantity=float(quantity),
                new_client_order_id=client_order_id,
                new_order_resp_type=NewOrderNewOrderRespTypeEnum.FULL,
                recv_window=5000,
            ).data()
        )
        return _order_result(response)

    def query_order(self, symbol: str, client_order_id: str) -> OrderResult:
        response = _plain(
            self._client.get_order(
                symbol=symbol, orig_client_order_id=client_order_id, recv_window=5000
            ).data()
        )
        order_id = _field(response, "orderId", "order_id")
        if not isinstance(order_id, (str, int)):
            raise ValueError("Binance order identifier is malformed")
        trades = _plain_list(
            self._client.my_trades(
                symbol=symbol, order_id=int(order_id), limit=1000, recv_window=5000
            ).data()
        )
        response["fills"] = trades
        return _order_result(response)

    def place_oco_protection(
        self,
        symbol: str,
        client_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
    ) -> str:
        stop = Decimal(stop_price)
        response = _plain(
            self._client.order_oco(
                symbol=symbol,
                side=OrderOcoSideEnum.SELL,
                quantity=float(quantity),
                price=float(take_profit_price),
                stop_price=float(stop),
                stop_limit_price=float(stop * Decimal("0.999")),
                stop_limit_time_in_force=OrderOcoStopLimitTimeInForceEnum.GTC,
                list_client_order_id=client_order_id,
                new_order_resp_type=OrderOcoNewOrderRespTypeEnum.FULL,
                recv_window=5000,
            ).data()
        )
        return str(_field(response, "orderListId", "order_list_id"))

    def flatten_market_sell(self, symbol: str, client_order_id: str, quantity: str) -> OrderResult:
        response = _plain(
            self._client.new_order(
                symbol=symbol,
                side=NewOrderSideEnum.SELL,
                type=NewOrderTypeEnum.MARKET,
                quantity=float(quantity),
                new_client_order_id=client_order_id,
                new_order_resp_type=NewOrderNewOrderRespTypeEnum.FULL,
                recv_window=5000,
            ).data()
        )
        return _order_result(response)

    def cancel_protection(self, symbol: str, client_order_id: str) -> None:
        self._client.delete_order_list(
            symbol=symbol, list_client_order_id=client_order_id, recv_window=5000
        ).data()


class InMemorySpotTransport:
    """Deterministic SpotTransport for adapter and reconciliation contract tests."""

    def __init__(
        self,
        *,
        initial_usdt: Decimal = Decimal("10000"),
        reference_price: Decimal = Decimal("90000"),
    ) -> None:
        if initial_usdt <= 0 or reference_price <= 0:
            raise ValueError("in-memory account values must be positive")
        self._usdt = initial_usdt
        self._btc = Decimal(0)
        self._reference_price = reference_price
        self._next_order_id = 1
        self._next_trade_id = 1
        self._next_list_id = 1
        self._results: dict[str, OrderResult] = {}
        self._open_orders: dict[str, dict[str, object]] = {}
        self._trades: list[dict[str, object]] = []

    def exchange_info(self, symbol: str) -> dict[str, object]:
        self._check_symbol(symbol)
        return {
            "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "limit": 6000}],
            "symbols": [
                {
                    "symbol": symbol,
                    "status": "TRADING",
                    "orderTypes": ["MARKET", "LIMIT", "STOP_LOSS_LIMIT"],
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.00001000", "maxQty": "100"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "NOTIONAL", "minNotional": "5", "maxNotional": "1000000"},
                    ],
                }
            ],
        }

    def account_commission(self, symbol: str) -> dict[str, object]:
        self._check_symbol(symbol)
        return {"standardCommission": {"maker": "0.001", "taker": "0.001"}}

    def account(self) -> dict[str, object]:
        return {
            "balances": [
                {"asset": "USDT", "free": str(self._usdt), "locked": "0"},
                {"asset": "BTC", "free": str(self._btc), "locked": "0"},
            ]
        }

    def open_orders(self, symbol: str) -> list[dict[str, object]]:
        self._check_symbol(symbol)
        return list(self._open_orders.values())

    def trades(self, symbol: str) -> list[dict[str, object]]:
        self._check_symbol(symbol)
        return list(self._trades)

    def submit_market_buy(self, symbol: str, client_order_id: str, quantity: str) -> OrderResult:
        self._check_symbol(symbol)
        return self._market_order(symbol, client_order_id, quantity, side="BUY")

    def query_order(self, symbol: str, client_order_id: str) -> OrderResult:
        self._check_symbol(symbol)
        try:
            return self._results[client_order_id]
        except KeyError as error:
            raise LookupError(f"unknown in-memory order: {client_order_id}") from error

    def place_oco_protection(
        self,
        symbol: str,
        client_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
    ) -> str:
        self._check_symbol(symbol)
        existing = self._open_orders.get(client_order_id)
        if existing is not None:
            return str(existing["orderListId"])
        list_id = str(self._next_list_id)
        self._next_list_id += 1
        self._open_orders[client_order_id] = {
            "symbol": symbol,
            "orderListId": list_id,
            "listClientOrderId": client_order_id,
            "origQty": quantity,
            "stopPrice": stop_price,
            "price": take_profit_price,
            "status": "EXEC_STARTED",
        }
        return list_id

    def flatten_market_sell(self, symbol: str, client_order_id: str, quantity: str) -> OrderResult:
        self._check_symbol(symbol)
        return self._market_order(symbol, client_order_id, quantity, side="SELL")

    def cancel_protection(self, symbol: str, client_order_id: str) -> None:
        self._check_symbol(symbol)
        self._open_orders.pop(client_order_id, None)

    def _market_order(
        self, symbol: str, client_order_id: str, quantity: str, *, side: str
    ) -> OrderResult:
        existing = self._results.get(client_order_id)
        if existing is not None:
            return existing
        amount = Decimal(quantity)
        if amount <= 0:
            raise ValueError("in-memory order quantity must be positive")
        notional = amount * self._reference_price
        commission = notional * Decimal("0.001")
        if side == "BUY":
            if self._usdt < notional + commission:
                raise ValueError("in-memory account has insufficient USDT")
            self._usdt -= notional + commission
            self._btc += amount
        else:
            if self._btc < amount:
                raise ValueError("in-memory account has insufficient BTC")
            self._btc -= amount
            self._usdt += notional - commission
        order_id = str(self._next_order_id)
        self._next_order_id += 1
        trade_id = str(self._next_trade_id)
        self._next_trade_id += 1
        fill = ExecutionFill(
            fill_id=trade_id,
            price=self._reference_price,
            quantity=amount,
            commission=commission,
            commission_asset="USDT",
        )
        result = OrderResult(status="FILLED", exchange_order_id=order_id, fills=(fill,))
        self._results[client_order_id] = result
        self._trades.append(
            {
                "id": trade_id,
                "orderId": order_id,
                "clientOrderId": client_order_id,
                "price": str(self._reference_price),
                "qty": str(amount),
                "commission": str(commission),
                "commissionAsset": "USDT",
                "time": 0,
            }
        )
        return result

    @staticmethod
    def _check_symbol(symbol: str) -> None:
        if symbol != "BTCUSDT":
            raise ValueError("in-memory transport only supports BTCUSDT")


@dataclass(frozen=True)
class BinanceCapabilities:
    symbol_status: str
    order_types: tuple[str, ...]
    step_size: str
    min_notional: str
    tick_size: str
    max_quantity: str
    max_notional: str
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
        raw_filters = symbol.get("filters")
        if not isinstance(raw_filters, list):
            raise ValueError("BTCUSDT exchange filters are missing")
        filters = {
            str(item["filterType"]): item
            for item in raw_filters
            if isinstance(item, dict) and item.get("filterType") is not None
        }
        notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL")
        if notional_filter is None:
            raise ValueError("BTCUSDT notional filter is missing")
        order_types = symbol.get("orderTypes")
        if not isinstance(order_types, list):
            raise ValueError("BTCUSDT order types are missing")
        limits = info.get("rateLimits")
        weight = (
            next(
                (
                    item["limit"]
                    for item in limits
                    if isinstance(item, dict) and item.get("rateLimitType") == "REQUEST_WEIGHT"
                ),
                None,
            )
            if isinstance(limits, list)
            else None
        )
        commission = self.transport.account_commission(self.symbol)
        standard = commission.get("standardCommission")
        if not isinstance(standard, dict):
            raise ValueError("account commission response is missing standard rates")
        try:
            request_weight_limit = int(str(weight))
        except (TypeError, ValueError):
            raise ValueError("request-weight limit is missing") from None
        required_filters = {"LOT_SIZE", "PRICE_FILTER"}
        if not required_filters.issubset(filters):
            raise ValueError("BTCUSDT quantity or price filters are missing")
        return BinanceCapabilities(
            symbol_status=str(symbol["status"]),
            order_types=tuple(str(item) for item in order_types),
            step_size=str(filters["LOT_SIZE"]["stepSize"]),
            min_notional=str(notional_filter["minNotional"]),
            tick_size=str(filters["PRICE_FILTER"]["tickSize"]),
            max_quantity=str(filters.get("MARKET_LOT_SIZE", filters["LOT_SIZE"])["maxQty"]),
            max_notional=str(notional_filter.get("maxNotional", "0")),
            maker_commission=str(standard["maker"]),
            taker_commission=str(standard["taker"]),
            request_weight_limit=request_weight_limit,
        )

    def cancel_protection(self, *, client_order_id: str) -> None:
        self.transport.cancel_protection(self.symbol, client_order_id)

    def reconciliation_snapshot(self) -> dict[str, object]:
        account = self.transport.account()
        return {
            "account": account,
            "balances": account.get("balances", []),
            "open_orders": self.transport.open_orders(self.symbol),
            "trades": self.transport.trades(self.symbol),
        }

    def submit_market_buy(self, *, client_order_id: str, quantity: str) -> OrderResult:
        return self.transport.submit_market_buy(self.symbol, client_order_id, quantity)

    def query_order(self, *, client_order_id: str) -> OrderResult:
        return self.transport.query_order(self.symbol, client_order_id)

    def place_oco_protection(
        self,
        *,
        client_order_id: str,
        quantity: str,
        stop_price: str,
        take_profit_price: str,
    ) -> str:
        return self.transport.place_oco_protection(
            self.symbol, client_order_id, quantity, stop_price, take_profit_price
        )

    def flatten_market_sell(self, *, client_order_id: str, quantity: str) -> OrderResult:
        return self.transport.flatten_market_sell(self.symbol, client_order_id, quantity)


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


def _field(item: dict[str, object], *names: str) -> object:
    for name in names:
        if name in item:
            return item[name]
    raise ValueError(f"Binance response is missing one of {names}")


def _order_result(response: dict[str, object]) -> OrderResult:
    raw_fills = response.get("fills", [])
    if not isinstance(raw_fills, list):
        raise ValueError("Binance order fills are malformed")
    fills: list[ExecutionFill] = []
    for index, raw in enumerate(raw_fills):
        if not isinstance(raw, dict):
            raise ValueError("Binance order fill is malformed")
        fill_id = raw.get("id", raw.get("tradeId", raw.get("trade_id", index)))
        fills.append(
            ExecutionFill(
                fill_id=str(fill_id),
                price=Decimal(str(_field(raw, "price"))),
                quantity=Decimal(str(_field(raw, "qty", "quantity"))),
                commission=Decimal(str(_field(raw, "commission"))),
                commission_asset=str(_field(raw, "commissionAsset", "commission_asset")),
            )
        )
    return OrderResult(
        status=str(_field(response, "status")),
        exchange_order_id=str(_field(response, "orderId", "order_id")),
        fills=tuple(fills),
    )
