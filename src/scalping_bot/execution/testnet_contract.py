"""Explicit, safety-gated Binance Spot Testnet contract scenarios."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse
from uuid import uuid4

from websockets.sync.client import connect

from scalping_bot.execution.binance import BinanceCapabilities, BinanceSpotTestnetAdapter
from scalping_bot.execution.executor import OrderResult

TESTNET_STREAM_HOST = "stream.testnet.binance.vision"


@dataclass(frozen=True)
class TestnetContractReport:
    """Redacted result of a Testnet contract run."""

    capabilities: BinanceCapabilities
    checks: tuple[str, ...]
    order_round_trip_executed: bool
    reconnect_verified: bool
    order_client_ids: tuple[str, ...] = ()


def run_testnet_contract_scenarios(
    adapter: BinanceSpotTestnetAdapter,
    *,
    confirm_testnet: bool,
    reconnect_probe: Callable[[str], None] | None,
    execute_order_round_trip: bool = False,
    quantity: str = "0.0001",
) -> TestnetContractReport:
    """Run the opt-in REST/User Data contract against Binance Spot Testnet.

    The read-only portion checks the current exchange contract.  The optional
    order portion is deliberately separate because it creates and closes real
    Testnet exposure.  A caller must provide an explicit reconnect probe so a
    successful report cannot accidentally omit the User Data reconnect check.
    """

    if not confirm_testnet:
        raise PermissionError("explicit Testnet confirmation is required")
    if urlparse(adapter.endpoint).hostname != "testnet.binance.vision":
        raise ValueError("contract scenarios only allow Binance Spot Testnet")
    if reconnect_probe is None:
        raise ValueError("a User Data reconnect probe is required")

    checks: list[str] = []
    capabilities = adapter.load_capabilities()
    if capabilities.symbol_status != "TRADING":
        raise RuntimeError("BTCUSDT is not currently trading on Binance Testnet")
    if not {"MARKET", "STOP_LOSS_LIMIT"}.issubset(capabilities.order_types):
        raise RuntimeError("Testnet does not expose the required order types")
    checks.extend(("exchange-info", "account-commission"))
    _validate_filters(capabilities, quantity)
    checks.extend(("filters", "rate-limits"))

    snapshot = adapter.reconciliation_snapshot()
    for name in ("account", "open_orders", "trades"):
        value = snapshot.get(name)
        if not isinstance(value, (dict, list)):
            raise ValueError(f"Binance {name} response is malformed")
        checks.append(name.replace("_", "-"))

    listen_key = adapter.transport.create_listen_key()
    if not listen_key or any(character.isspace() for character in listen_key):
        raise ValueError("Binance Testnet listen key is malformed")
    try:
        adapter.transport.keepalive_listen_key(listen_key)
        checks.append("user-data-listen-key")
        reconnect_probe(listen_key)
        checks.append("user-data-reconnect")
        order_client_ids: tuple[str, ...] = ()
        if execute_order_round_trip:
            order_client_ids = _execute_order_round_trip(
                adapter,
                quantity=quantity,
                tick_size=Decimal(capabilities.tick_size),
                order_client_ids=(
                    f"contract-entry-{uuid4().hex[:12]}",
                    f"contract-protect-{uuid4().hex[:12]}",
                    f"contract-exit-{uuid4().hex[:12]}",
                ),
                checks=checks,
            )
        return TestnetContractReport(
            capabilities=capabilities,
            checks=tuple(checks),
            order_round_trip_executed=execute_order_round_trip,
            reconnect_verified=True,
            order_client_ids=order_client_ids,
        )
    finally:
        adapter.transport.close_listen_key(listen_key)


def probe_testnet_user_data_reconnect(
    endpoint: str, listen_key: str, *, timeout_seconds: float = 10
) -> None:
    """Open, close, and reopen a real Testnet User Data connection."""

    parsed = urlparse(endpoint)
    if parsed.scheme != "wss" or parsed.hostname != TESTNET_STREAM_HOST:
        raise ValueError("User Data probes only allow Binance Spot Testnet")
    if timeout_seconds <= 0:
        raise ValueError("User Data probe timeout must be positive")
    url = f"{endpoint.rstrip('/')}/{listen_key}"
    for _ in range(2):
        with connect(url, open_timeout=timeout_seconds, close_timeout=timeout_seconds):
            pass


def _validate_filters(capabilities: BinanceCapabilities, quantity: str) -> None:
    try:
        value = Decimal(quantity)
        step = Decimal(capabilities.step_size)
        maximum = Decimal(capabilities.max_quantity)
    except (InvalidOperation, ValueError) as error:
        raise ValueError("contract order quantity or exchange filters are malformed") from error
    if value <= 0 or step <= 0 or value % step != 0:
        raise ValueError("contract order quantity does not satisfy LOT_SIZE")
    if maximum > 0 and value > maximum:
        raise ValueError("contract order quantity exceeds MARKET_LOT_SIZE")


def _execute_order_round_trip(
    adapter: BinanceSpotTestnetAdapter,
    *,
    quantity: str,
    tick_size: Decimal,
    order_client_ids: tuple[str, str, str],
    checks: list[str],
) -> tuple[str, ...]:
    entry_id, protection_id, exit_id = order_client_ids
    bought = False
    flattened = False
    filled_quantity = Decimal(0)
    try:
        entry = _submit_and_query(
            lambda: adapter.submit_market_buy(client_order_id=entry_id, quantity=quantity),
            lambda: adapter.query_order(client_order_id=entry_id),
        )
        checks.append("order-submit")
        checks.append("order-query")
        filled_quantity = _filled_quantity(entry)
        if filled_quantity <= 0:
            raise RuntimeError("Testnet contract entry returned no fill")
        bought = True
        entry_price = entry.fills[0].price
        stop_price = _floor_price(entry_price * Decimal("0.5"), tick_size)
        take_profit_price = _floor_price(entry_price * Decimal("1.5"), tick_size)
        adapter.place_oco_protection(
            client_order_id=protection_id,
            quantity=str(filled_quantity),
            stop_price=str(stop_price),
            take_profit_price=str(take_profit_price),
        )
        checks.append("linked-protection")
        adapter.cancel_protection(client_order_id=protection_id)
        checks.append("protection-cancel")
        exit_result = _submit_and_query(
            lambda: adapter.flatten_market_sell(
                client_order_id=exit_id, quantity=str(filled_quantity)
            ),
            lambda: adapter.query_order(client_order_id=exit_id),
        )
        _require_filled(exit_result, "Testnet contract flatten")
        checks.append("order-flatten")
        checks.append("flatten-query")
        flattened = True
    finally:
        if bought and not flattened:
            with suppress(Exception):
                adapter.cancel_protection(client_order_id=protection_id)
            # This is the one emergency cleanup attempt.  The scenario never
            # retries a timed-out order without first querying its client id.
            with suppress(Exception):
                _submit_and_query(
                    lambda: adapter.flatten_market_sell(
                        client_order_id=exit_id, quantity=str(filled_quantity)
                    ),
                    lambda: adapter.query_order(client_order_id=exit_id),
                )
    return (entry_id, protection_id, exit_id)


def _submit_and_query(
    submit: Callable[[], OrderResult], query: Callable[[], OrderResult]
) -> OrderResult:
    try:
        submitted = submit()
    except Exception:
        # An ambiguous response is looked up by the persisted client id before
        # the contract scenario is allowed to submit anything else.
        return query()
    if submitted.status in {"UNKNOWN", "SUBMITTING"}:
        return query()
    queried = query()
    return queried


def _filled_quantity(result: OrderResult) -> Decimal:
    return sum((fill.quantity for fill in result.fills), Decimal(0))


def _floor_price(value: Decimal, tick_size: Decimal) -> Decimal:
    if value <= 0 or tick_size <= 0:
        raise ValueError("contract protection prices must be positive")
    return value // tick_size * tick_size


def _require_filled(result: OrderResult, label: str) -> None:
    if result.status not in {"FILLED", "PARTIALLY_FILLED"} or _filled_quantity(result) <= 0:
        raise RuntimeError(f"{label} did not fill")
