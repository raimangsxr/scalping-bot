# Scalping Research and Execution

This context evaluates short-horizon trading strategies and exercises their order lifecycle without risking real capital.

## Language

**Research Market**:
The Binance Spot `BTCUSDT` market, the sole instrument and venue used during the first research phase.
_Avoid_: Portfolio, multi-market universe

**Scalp**:
A single `BTCUSDT` Spot position held for no more than 30 minutes, with at most one scalp open at a time.
_Avoid_: Swing trade, investment

**Conservative Market Execution**:
An execution assumption in which an order is submitted only after its signal is confirmed and pays modeled taker fees and slippage.
_Avoid_: Signal-price fill, same-close execution

**Account Commission Snapshot**:
The `BTCUSDT` commission configuration returned by Binance for the configured account and retained with an experiment's results.
_Avoid_: Default fee, assumed commission

**Execution Cost Model**:
The combination of account commission, observed spread, and size-, time-, and volatility-dependent slippage used to evaluate hypothetical fills.
_Avoid_: Fee estimate, fixed friction

**Market Calibration Sample**:
Prospectively collected public production-market bid, ask, and depth observations used to calibrate the execution cost model without placing orders.
_Avoid_: Testnet fills, historical order book

**Trade Flow**:
The chronological flow of executed trades, including aggressor side, price, quantity, and intensity; historical research derives it from Binance aggregate trades.
_Avoid_: Order flow, book pressure

**Book Flow**:
The chronological additions, removals, and executions affecting the visible limit order book; it describes displayed liquidity, not trader intent.
_Avoid_: Trade flow, committed liquidity

**Order Flow Confirmation**:
A trade-flow or book-flow condition evaluated first as incremental evidence that permits or blocks an otherwise valid strategy entry.
_Avoid_: Standalone prediction, discretionary tape reading

**Valid Book Segment**:
A continuous interval whose local order book is reconstructible from an exchange snapshot and gap-free sequenced depth events.
_Avoid_: Filled gap, interpolated book

**Shadow Evaluation**:
Prospective measurement of an experimental feature or candidate that may record hypothetical decisions but cannot authorize executable entries.
_Avoid_: Testnet execution, validated strategy

**Live Shadow**:
A minimum fourteen-day period in which a historically promoted candidate produces real-time decisions without submitting orders, used to verify live/replay fidelity and data quality.
_Avoid_: Testnet soak, paper trading

**Testnet Ready**:
A candidate that has passed walk-forward, aggregate-trade validation, final holdout, and all risk limits and may therefore begin Live Shadow before Testnet orders.
_Avoid_: Profitable strategy, production ready

**Testnet Soak**:
Testnet operation lasting at least thirty calendar days and 300 executed scalps at 0.25% position risk, used to validate operational behaviour rather than economic edge.
_Avoid_: Profit validation, live trading

**Market Data Dependency**:
A candidate's explicit requirement for particular streams and quality conditions, whose violation blocks new dependent entries without weakening protective exits.
_Avoid_: Optional input, silent fallback

**Degraded Mode**:
A safe operating state that continues observation and protective exits while prohibiting new entries affected by missing, late, or invalid market data.
_Avoid_: Best-effort trading, automatic fallback

**Unknown Order Outcome**:
The state after a submitted request loses its response, requiring lookup by its persisted client order identifier before any retry.
_Avoid_: Failed order, rejected order

**Protective Order**:
An exchange-managed exit placed for an open position so its loss remains bounded if the local executor becomes unavailable.
_Avoid_: Local stop, exit intention

**Pause**:
An operator state that blocks new entries while preserving supervision and protection of existing exposure.
_Avoid_: Shutdown, emergency close

**Flatten and Lock**:
An operator action that cancels pending orders, closes exposure, and prevents new entries until explicit reconciliation and manual unlock.
_Avoid_: Pause, restart

**Confirmed Signal**:
A signal derived exclusively from observations already complete at decision time; its order may execute only on a subsequent market event.
_Avoid_: Prediction, same-bar signal

**Entry Proposal**:
A structured, non-authoritative strategy result containing candidate version, time, direction, rationale, feature snapshot, invalidation price, expiry, and market-data dependencies.
_Avoid_: Order, trade, confidence bet

**Signal Expiry**:
The point after which an unexecuted entry proposal is no longer actionable, no later than the close of the next one-minute bar.
_Avoid_: Entry timeout, delayed signal

**Strategy Rationale**:
The structured conditions that produced an entry proposal and would invalidate it, emitted by the same evaluation that created the proposal.
_Avoid_: Post-hoc explanation, free-form commentary

**Timed Exit**:
The mandatory exit policy for a scalp: normal risk exits retain priority, break-even-or-better closes are accepted from minute 25, and market execution is forced at minute 30.
_Avoid_: Hold until recovery, discretionary exit

**Session Filter**:
A restriction on eligible trading hours accepted only when it improves net out-of-sample results and retains at least 100 validation trades.
_Avoid_: Best historical hours

**Strategy Candidate**:
A specific, falsifiable combination of market conditions, entry rules, and exit rules proposed for independent evaluation; it is not yet a validated strategy.
_Avoid_: Winning strategy, profitable strategy

**Candidate Lifecycle**:
The evidence-gated progression `proposed → specified → screened → walk-forward-validated → holdout-tested → testnet-ready`, with `rejected` terminal from any stage.
_Avoid_: Work status, confidence label

**Experiment Manifest**:
The immutable identity of a research run: code commit, complete configuration, data checksums, commission snapshot, execution cost model, evaluation period, and random seed.
_Avoid_: Run notes, latest configuration

**Candidate Record**:
The versioned thesis, source provenance, original rules, local modifications, machine-readable configuration, lifecycle evidence, and rejection history for one strategy candidate.
_Avoid_: Backtest result, strategy file

**Strategy Family**:
A shared market hypothesis grouping candidates as momentum, volatility breakout, regime-conditioned mean reversion, or executed-trade pressure.
_Avoid_: Indicator collection

**Ensemble**:
A coordinated combination of independently validated strategies whose out-of-sample behaviour provides useful diversification.
_Avoid_: Indicator stack, strategy bundle

**Robust Parameter Region**:
A neighbourhood of reasonable parameter values that preserves a candidate's useful out-of-sample behaviour.
_Avoid_: Best parameter, optimal point

**Net Out-of-Sample Return**:
The change in account value measured on market data excluded from strategy development, after modeled trading costs and slippage.
_Avoid_: Profit, gross return, in-sample return

**Research Target**:
An aspirational average net monthly return of at least 10% on a fixed 10,000 USDT model account, with a positive monthly median and at least eight positive months in twelve; it is neither a monthly floor nor a promised outcome.
_Avoid_: Guaranteed income, monthly salary

**Mark-to-Market Equity**:
Account value including realized and unrealized P&L, commissions, and the modeled cost of immediately closing open exposure.
_Avoid_: Balance, realized equity

**Risk Limits**:
The research constraints of 10% maximum drawdown, 2% maximum daily loss, and 0.25% maximum estimated account risk per trade.
_Avoid_: Risk tolerance, loss target

**Position Risk**:
The loss at the precommitted invalidation price, sized from the lesser of current equity and the initial 10,000 USDT capital and capped by a 50% account-notional limit.
_Avoid_: Position value, order size

**Promoted Position Risk**:
An optional 0.5% per-scalp risk level evaluated only after a candidate passes promotion at 0.25%, and accepted only while all daily-loss and drawdown limits remain satisfied.
_Avoid_: Optimization parameter, recovery sizing

**Daily Circuit Breaker**:
A control that closes exposure and blocks new entries after a 2% UTC-day loss; Testnet resumes no earlier than the next UTC day and only with operator confirmation.
_Avoid_: Daily stop-loss order

**Drawdown Circuit Breaker**:
A control that closes exposure and locks the system indefinitely at 10% drawdown pending operator review; reaching it fails a strategy candidate.
_Avoid_: Temporary pause, recovery mode

**Validation Sample**:
Market observations excluded from strategy fitting and parameter selection, used only to estimate how a candidate behaves on unseen conditions.
_Avoid_: Training data, optimization data

**Walk-Forward Fold**:
A chronological experiment using twelve months for development followed by three unseen months for evaluation, advanced three months for the next fold.
_Avoid_: Random split, shuffled validation

**Primary Research Period**:
The January 2020 through September 2025 `BTCUSDT` history used for development and walk-forward evaluation.
_Avoid_: Full history, final holdout

**Historical Stress Period**:
The August 2017 through December 2019 `BTCUSDT` history reported separately to expose structural fragility without rescuing failure in the primary protocol.
_Avoid_: Training extension, promotion evidence

**Final Holdout**:
The October 2025 through September 2026 history, kept unseen until a candidate's hypothesis, rules, and parameter ranges are frozen for its final evaluation.
_Avoid_: Validation sample, tuning data

**Promotion Gate**:
The minimum evidence required for a candidate to advance: risk limits satisfied, at least 300 out-of-sample scalps, positive net results in at least 70% of walk-forward folds, and a non-negative lower 95% bootstrap bound for net expectancy.
_Avoid_: Best backtest, leaderboard rank

**Profit Concentration**:
The share of net out-of-sample return attributable to one scalp or one calendar month; it fails when a scalp exceeds 10% or a month exceeds 25% of total profit.
_Avoid_: Event-dependent result, concentrated profit

**Operator**:
The single person who configures, supervises, and evaluates the system.
_Avoid_: User, customer

**Testnet Execution**:
Order execution against Binance's test environment using simulated funds, intended to validate integration and order behaviour without risking real capital.
_Avoid_: Live trading, paper trading

**Live Trading**:
Order execution using real funds in a production market environment.
_Avoid_: Testnet execution
