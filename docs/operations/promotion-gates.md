# Promotion gates

The time-based gates for Live Shadow, Testnet Soak, and Promoted Position Risk are implemented in
`scalping_bot.operations.promotion`.

## Live Shadow (#19)

`LiveShadowRunner` sends the same immutable market events through the shared `Engine` and records
the resulting proposals and commands as hypothetical decisions. It has no exchange adapter, so a
`SubmitEntry`, exit, or protection command cannot be submitted by the runner.

The operator records one `LiveReplayComparison` per live observation. The comparison includes
features, signals, expiry, latency, data quality, and hypothetical cost. Missing mandatory data
must be recorded with `LiveShadowGate.missing_dependency`, which returns `DEGRADED` and never
authorises a hypothetical entry. A divergence is accepted only after
`resolve_divergence(..., regression=...)` records both its explanation and a replayable regression.

`evaluate(now_ms=...)` requires fourteen continuous calendar days, rejects calendar-day gaps,
rejects any submitted order, and returns a final approval or rejection report. Persist the report
with `persist_report` in the immutable object store.

## Testnet Soak (#20)

`SoakGate` accepts only `0.0025` position risk and no more than 50% account notional. The initial
period is supervised. `begin_continuous` is allowed only after seven incident-free days and
verified alerts; explicit supervised windows can be recorded with
`record_supervised_window`.

Every executed scalp is recorded through `record_scalp`. Its protection, duplicate-order status,
exchange reconciliation, risk, notional, and unresolved divergences are retained in the gate.
`record_incident` requires a regression for every critical incident and resets continuous
admission, forcing another seven-day stability period. Completion requires thirty calendar days
and 300 safe executed scalps. The report explicitly excludes Testnet P&L from production evidence.

The configuration validator also rejects any Testnet `position_risk` other than `0.0025`, so the
operational setting cannot silently drift from the soak contract.

## Promoted Position Risk (#21)

`PromotedRiskGate` requires a promoted 0.25% baseline and a completed soak before it evaluates
0.5%. `RiskEvaluation` carries separate historical-stress and Testnet-stress evidence. The gate
requires the candidate rule hash, notional cap, daily-loss cap, drawdown cap, and risk-control
policy to remain unchanged; it also rechecks risk, cost stress, operational controls, and profit
concentration at both risk levels.

Each decision is written as a separate immutable JSON manifest. The returned result and report
always preserve `0.0025` as the proven baseline, whether the 0.5% trial is accepted or rejected.

These classes validate and persist evidence; they do not manufacture elapsed time or Testnet
fills. Issues #19 and #20 remain operationally open until the real fourteen- and thirty-day
windows, with their reports and audit evidence, have completed.
