from __future__ import annotations

import html

from scalping_bot.research.evidence import EvidenceResult, ScalpResult


def render_evidence_markdown(
    candidate_identity: str, evidence: EvidenceResult, trades: tuple[ScalpResult, ...]
) -> str:
    decision = "PROMOTED" if evidence.promoted else "REJECTED"
    cost_stress = ", ".join(map(str, evidence.cost_stress_multipliers)) or "not evidenced"
    lines = [
        f"# Evidence report — {candidate_identity}",
        "",
        f"## Decision: {decision}",
        "",
        f"- Net return: {evidence.metrics.net_return}",
        f"- Maximum drawdown: {evidence.metrics.max_drawdown}",
        f"- Maximum daily loss: {evidence.metrics.maximum_daily_loss}",
        f"- Net expectancy: {_display(evidence.metrics.expectancy)}",
        f"- Profit factor: {_display(evidence.metrics.profit_factor)}",
        f"- Daily Sharpe: {_display(evidence.metrics.daily_sharpe)}",
        f"- Daily Sortino: {_display(evidence.metrics.daily_sortino)}",
        f"- Win rate: {_display(evidence.metrics.win_rate)}",
        f"- Exposure: {evidence.metrics.exposure}",
        f"- Turnover: {evidence.metrics.turnover}",
        f"- Trade count: {evidence.metrics.trade_count}",
        f"- Aggregate-trade fidelity: {evidence.market_fidelity_verified}",
        f"- Selection-adjusted lower bound: {_display(evidence.bootstrap_lower_95)}",
        f"- Positive folds: {evidence.positive_fold_fraction}",
        f"- Concentration gate failed: {evidence.concentration_failed}",
        f"- Cost stress multipliers: {cost_stress}",
        f"- Baseline net return: "
        f"{_display(evidence.baseline_metrics.net_return if evidence.baseline_metrics else None)}",
        f"- Incremental net return versus baseline: {_display(evidence.incremental_net_return)}",
        *(
            f"- Baseline cost {multiplier}x net/expectancy: "
            f"{metrics.net_return}/{_display(metrics.expectancy)}"
            for multiplier, metrics in sorted(evidence.baseline_cost_scenario_metrics.items())
        ),
        "",
        "## Walk-forward folds",
        "",
        *(
            f"- Fold {fold}: net return {metrics.net_return}; expectancy "
            f"{_display(metrics.expectancy)}; trades {metrics.trade_count}"
            for fold, metrics in sorted(evidence.fold_metrics.items())
        ),
        "",
        "## Regime breakdown",
        "",
        *(
            f"- {regime}: net return {metrics.net_return}; expectancy "
            f"{_display(metrics.expectancy)}; trades {metrics.trade_count}"
            for regime, metrics in sorted(evidence.regime_metrics.items())
        ),
        *(
            f"- Cost {multiplier}x net/expectancy: "
            f"{metrics.net_return}/{_display(metrics.expectancy)}"
            for multiplier, metrics in sorted(evidence.cost_scenario_metrics.items())
        ),
        "",
        "## Comparators",
        "",
        *(
            f"- {name}: {value if value is not None else 'non-evaluable'}"
            for name, value in evidence.comparators.items()
        ),
        "",
        "## Scalp traces",
        "",
    ]
    for trade in trades:
        trace = " → ".join(trade.trace_event_ids) or "not supplied"
        lines.append(
            f"- `{trade.scalp_id}`: {trade.entry_price} → {trade.exit_price}; "
            f"net {trade.net_pnl}; {trace}"
        )
    return "\n".join(lines) + "\n"


def _display(value: object) -> object:
    return "non-evaluable" if value is None else value


def render_evidence_html(
    candidate_identity: str, evidence: EvidenceResult, trades: tuple[ScalpResult, ...]
) -> str:
    markdown = render_evidence_markdown(candidate_identity, evidence, trades)
    return (
        '<!doctype html><html><head><meta charset="utf-8"><title>Evidence report</title>'
        "</head><body><pre>" + html.escape(markdown) + "</pre></body></html>\n"
    )
