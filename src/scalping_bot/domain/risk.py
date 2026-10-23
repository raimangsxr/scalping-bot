from decimal import ROUND_DOWN, Decimal


def position_quantity(
    *,
    equity: Decimal,
    initial_capital: Decimal,
    entry_price: Decimal,
    invalidation_price: Decimal,
    risk_fraction: Decimal = Decimal("0.0025"),
    notional_fraction: Decimal = Decimal("0.50"),
    step_size: Decimal = Decimal("0.00001"),
) -> Decimal:
    sizing_equity = min(equity, initial_capital)
    distance = entry_price - invalidation_price
    if distance <= 0:
        return Decimal(0)
    risk_quantity = sizing_equity * risk_fraction / distance
    notional_quantity = sizing_equity * notional_fraction / entry_price
    raw = min(risk_quantity, notional_quantity)
    return (raw / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size
