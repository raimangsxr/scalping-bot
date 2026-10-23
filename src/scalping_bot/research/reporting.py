from __future__ import annotations

import html

from scalping_bot.research.evidence import EvidenceResult, ScalpResult


def render_evidence_markdown(
    candidate_identity: str, evidence: EvidenceResult, trades: tuple[ScalpResult, ...]
) -> str:
    decision = "PROMOTED" if evidence.promoted else "REJECTED"
    lines = [
        f"# Evidence report — {candidate_identity}",
        "",
        f"## Decision: {decision}",
        "",
        f"- Net return: {evidence.metrics.net_return}",
        f"- Maximum drawdown: {evidence.metrics.max_drawdown}",
        f"- Maximum daily loss: {evidence.metrics.maximum_daily_loss}",
        f"- Net expectancy: {evidence.metrics.expectancy or 'non-evaluable'}",
        f"- Profit factor: {evidence.metrics.profit_factor or 'non-evaluable'}",
        f"- Daily Sharpe: {evidence.metrics.daily_sharpe or 'non-evaluable'}",
        f"- Daily Sortino: {evidence.metrics.daily_sortino or 'non-evaluable'}",
        f"- Win rate: {evidence.metrics.win_rate or 'non-evaluable'}",
        f"- Exposure: {evidence.metrics.exposure}",
        f"- Turnover: {evidence.metrics.turnover}",
        f"- Trade count: {evidence.metrics.trade_count}",
        f"- Selection-adjusted lower bound: {evidence.bootstrap_lower_95 or 'non-evaluable'}",
        f"- Positive folds: {evidence.positive_fold_fraction}",
        f"- Concentration gate failed: {evidence.concentration_failed}",
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


def render_evidence_html(
    candidate_identity: str, evidence: EvidenceResult, trades: tuple[ScalpResult, ...]
) -> str:
    markdown = render_evidence_markdown(candidate_identity, evidence, trades)
    return (
        '<!doctype html><html><head><meta charset="utf-8"><title>Evidence report</title>'
        "</head><body><pre>" + html.escape(markdown) + "</pre></body></html>\n"
    )
