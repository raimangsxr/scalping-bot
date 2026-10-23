# Scalping Research and Testnet Platform v1

## Problem Statement

The Operator needs a reproducible way to discover whether short-horizon `BTCUSDT` strategies have a durable economic edge after commissions, spread, slippage, and strict risk limits. Published rankings and attractive backtests are not sufficient: they commonly omit execution costs, reuse the same history for selection and evaluation, hide rejected variants, or rely on future information.

The Operator also needs to exercise the complete order lifecycle without risking real capital. Research logic and Testnet execution must not drift apart, transient exchange failures must not duplicate orders or leave exposure unprotected, and live market-data degradation must fail safely. The first version must be capable of concluding that none of the researched candidates is good enough; finding no robust edge is a valid outcome.

## Solution

Build a Python modular monolith for a single Operator that researches Binance Spot `BTCUSDT` long-only strategies and, only after strict evidence gates, runs them on Binance Testnet. The system will share one deterministic event-driven engine across historical replay and Testnet. Live and replay market adapters, simulated and Binance exchange adapters, and in-memory and SQLite state adapters will cross the same small seams.

The research workflow will reproduce openly described strategy ideas, preregister each Strategy Candidate, retain every attempted variant, and evaluate candidates through chronological walk-forward folds. It will model the Account Commission Snapshot exactly and spread/slippage empirically, validate finalists with aggregate trades, and reveal a locked Final Holdout only after rules and parameters are frozen. The first batch is capped at eight candidates across momentum, volatility breakout, regime-conditioned mean reversion, and executed-trade-pressure families.

The live-data workflow will collect Trade Flow and Book Flow from public production-market streams in read-only mode. Historical Trade Flow may support backtests immediately; Book Flow remains in Shadow Evaluation until enough prospective evidence exists. A candidate that passes historical gates enters Live Shadow, then a Testnet Soak. Mainnet is technically rejected and is outside this specification.

## User Stories

1. As the Operator, I want to ingest official Binance `BTCUSDT` one-minute data, so that research begins from a reproducible primary source.
2. As the Operator, I want downloaded datasets verified by checksum, so that corrupt or silently revised inputs cannot alter results.
3. As the Operator, I want raw inputs retained immutably, so that derived features can be regenerated after definitions change.
4. As the Operator, I want timestamp units normalized explicitly, so that millisecond and microsecond source changes cannot distort chronology.
5. As the Operator, I want gaps, duplicates, disorder, and schema drift detected before feature calculation, so that invalid data cannot generate signals.
6. As the Operator, I want public production Trade Flow collected continuously, so that aggressor imbalance and trade intensity can be studied.
7. As the Operator, I want public production Book Flow collected continuously, so that displayed-liquidity changes can be studied prospectively.
8. As the Operator, I want local order books reconstructed only from valid snapshots and sequenced updates, so that corrupted books cannot influence decisions.
9. As the Operator, I want raw live events to include exchange and receipt times, so that market movement can be distinguished from network latency.
10. As the Operator, I want any invalid Book Flow interval excluded rather than interpolated, so that missing liquidity events are never invented.
11. As the Operator, I want each Strategy Candidate to record its source, thesis, original rules, modifications, and licence status, so that provenance remains auditable.
12. As the Operator, I want candidate hypotheses preregistered before backtesting, so that rules are not rewritten after seeing results.
13. As the Operator, I want published profit claims treated only as ideas, so that external marketing does not count as evidence.
14. As the Operator, I want a maximum initial batch of eight candidates, so that repeated trials cannot continue until a lucky winner appears.
15. As the Operator, I want simple unoptimised baselines for each Strategy Family, so that added complexity must demonstrate incremental value.
16. As the Operator, I want momentum, breakout, regime-conditioned mean reversion, and Trade Flow hypotheses compared under one protocol, so that the preferred mechanism is determined empirically.
17. As the Operator, I want Book Flow used first as shadow context or entry confirmation, so that immature data cannot authorize orders.
18. As the Operator, I want small predefined parameter ranges, so that the system searches Robust Parameter Regions instead of isolated peaks.
19. As the Operator, I want every attempted configuration retained, so that multiple testing and rejected paths remain visible.
20. As the Operator, I want vectorised screening available for speed, so that obviously weak candidates can be rejected cheaply.
21. As the Operator, I want promotion evidence produced only by the shared event engine, so that approximations cannot certify a candidate.
22. As the Operator, I want signals based only on complete observations, so that no candidate uses future or same-close information.
23. As the Operator, I want an Entry Proposal to carry rationale, invalidation, expiry, and data dependencies, so that every proposed scalp is explainable and complete.
24. As the Operator, I want strategy policies unable to choose position size or submit orders, so that all exposure passes through central risk controls.
25. As the Operator, I want stale signals to expire, so that momentum entries are never executed after their context has passed.
26. As the Operator, I want a mandatory feature warm-up, so that incomplete indicators cannot generate entries.
27. As the Operator, I want exits and risk obligations processed before entries, so that exposure reduction always wins.
28. As the Operator, I want at most one open scalp and a one-bar cooldown after exit, so that risk and attribution remain unambiguous.
29. As the Operator, I want every scalp capped at thirty minutes, so that a failed scalp never becomes an unintended investment.
30. As the Operator, I want normal stops to retain priority, break-even-or-better exits accepted from minute 25, and a forced market exit at minute 30, so that timed exits are deterministic.
31. As the Operator, I want market entries evaluated after signal confirmation with taker commission and calibrated execution costs, so that optimistic same-price fills are impossible.
32. As the Operator, I want the effective Binance commission captured for every experiment, so that fees are account- and symbol-specific.
33. As the Operator, I want spread and slippage calibrated from observed production-market data, so that execution costs vary with size, time, and volatility.
34. As the Operator, I want cost stress scenarios at 1.5 and 2 times calibrated cost, so that candidates fragile to worse execution are exposed.
35. As the Operator, I want walk-forward evaluation with twelve development months and three unseen months, so that performance is tested chronologically.
36. As the Operator, I want January 2020 through September 2025 used for primary research, so that all candidates share the same contemporary period.
37. As the Operator, I want August 2017 through December 2019 reported separately as historical stress, so that early-market fragility is visible without rescuing protocol failure.
38. As the Operator, I want October 2025 through September 2026 locked as the Final Holdout, so that recent evidence remains genuinely unseen.
39. As the Operator, I want Final Holdout access gated and audited, so that repeated peeking cannot be disguised as final testing.
40. As the Operator, I want finalists reevaluated with aggregate trades, so that one-minute bar ambiguity cannot provide promotion evidence.
41. As the Operator, I want complete metrics per fold and in aggregate, so that one exceptional period cannot hide weak windows.
42. As the Operator, I want mark-to-market equity to include costs and unrealised exposure, so that drawdown is not understated.
43. As the Operator, I want bootstrap confidence based on grouped trading days, so that correlated intraday trades are not treated as independent.
44. As the Operator, I want selection-adjusted evidence, so that testing more candidates raises rather than lowers the burden of proof.
45. As the Operator, I want no single trade to exceed 10% or single month to exceed 25% of total profit, so that results are not event-dependent.
46. As the Operator, I want candidates compared with no trade, BTC buy-and-hold, and simple momentum, so that complexity must earn its place.
47. As the Operator, I want promotion to require 300 out-of-sample scalps, positive net results in at least 70% of folds, and a non-negative lower 95% bootstrap bound for expectancy, so that noise is rejected.
48. As the Operator, I want the 10% monthly Research Target treated as a final aspiration rather than an early screening gate, so that risk is not increased to manufacture returns.
49. As the Operator, I want risk initially capped at 0.25% per scalp and 50% account notional, so that a single entry cannot dominate the account.
50. As the Operator, I want position size calculated from a precommitted invalidation price and current constrained equity, so that risk is known before entry.
51. As the Operator, I want profits excluded from the sizing base during research, so that compounding does not mask the underlying strategy.
52. As the Operator, I want a 2% Daily Circuit Breaker and a 10% Drawdown Circuit Breaker, so that failed behaviour stops automatically.
53. As the Operator, I want any later 0.5% risk trial isolated behind prior promotion at 0.25%, so that larger sizing cannot rescue weak evidence.
54. As the Operator, I want Binance treated as the authority for balances, orders, and fills, so that local state never overwrites exchange reality.
55. As the Operator, I want order identities persisted before submission, so that retries cannot create duplicate exposure.
56. As the Operator, I want timeouts treated as Unknown Order Outcomes and queried before retry, so that ambiguous responses are reconciled safely.
57. As the Operator, I want every filled quantity protected at the exchange as soon as possible, so that a process failure cannot leave unlimited exposure.
58. As the Operator, I want failure to confirm protection to trigger Flatten and Lock, so that safety failures stop new trading.
59. As the Operator, I want partial fills, commissions, and dust accounted from actual fills, so that position and P&L are exact.
60. As the Operator, I want symbol filters and rate limits loaded dynamically, so that Binance rule changes fail safely.
61. As the Operator, I want startup and reconnection reconciliation before entries, so that restarts cannot assume stale SQLite state.
62. As the Operator, I want planned deployments allowed only while flat, so that routine releases do not alter live exposure.
63. As the Operator, I want separate Pause and Flatten and Lock controls, so that operational intent is unambiguous.
64. As the Operator, I want Mainnet endpoints rejected by the first version, so that a configuration error cannot risk real capital.
65. As the Operator, I want Testnet credentials loaded only from local or deployment secrets, so that keys never enter Git, images, reports, or logs.
66. As the Operator, I want Telegram alerts and a daily heartbeat, so that silence cannot be mistaken for healthy operation.
67. As the Operator, I want critical alert, data, clock, reconciliation, or persistence failures to block entries, so that degraded operation remains safe.
68. As the Operator, I want append-only audit events for signals, orders, reconciliation, and manual actions, so that every state transition is explainable.
69. As the Operator, I want reproducible static reports in Markdown and HTML, so that evidence can be reviewed without a custom web interface.
70. As the Operator, I want every scalp traceable from market events through P&L, so that anomalous behaviour can be investigated.
71. As the Operator, I want SQLite backed up, restored, and reconciled in drills, so that operational history is recoverable.
72. As the Operator, I want a Testnet Ready candidate to spend fourteen days in Live Shadow, so that live/replay drift is detected before orders.
73. As the Operator, I want Testnet Soak to require at least thirty days and 300 scalps, so that operational stability is observed over meaningful use.
74. As the Operator, I want Testnet profitability excluded as economic evidence, so that simulated liquidity is not confused with production execution.
75. As the Operator, I want supervised Testnet sessions before 24/7 operation, so that alerts and recovery are proven before unattended use.
76. As the Operator, I want the platform to report honestly when all candidates fail, so that research success does not require a profitable strategy.

## Implementation Decisions

- The first version is single-Operator, Binance Spot `BTCUSDT`, long-only, and Testnet-only. Mainnet configuration is rejected rather than merely disabled by default.
- The model account starts at 10,000 USDT without contributions, withdrawals, or profit-based risk expansion. Reports show percentage and optional EUR equivalence separately.
- The Research Target is at least 10% average net monthly return over twelve months, a positive monthly median, and at least eight positive months. It is not a guarantee or initial promotion gate.
- The first research batch contains no more than eight preregistered candidates: up to two each for momentum, volatility breakout, regime-conditioned mean reversion, and executed-trade pressure.
- Strategy sources must expose reproducible rules and lawful reuse. Academic or otherwise transparent primary material is preferred; rankings, screenshots, closed signals, and unclear licences are excluded as evidence.
- Candidate lifecycle is `proposed`, `specified`, `screened`, `walk-forward-validated`, `holdout-tested`, `testnet-ready`, with `rejected` terminal from any stage.
- Every experiment has an immutable manifest containing code identity, complete configuration, dataset checksums, Account Commission Snapshot, Execution Cost Model, evaluation period, and random seed.
- Official Binance one-minute klines support screening. Aggregate trades are required for final walk-forward and holdout validation. Raw downloads are immutable and derived datasets are reproducible.
- Primary Research Period is January 2020–September 2025. Walk-Forward Folds use twelve development months followed by three unseen months and advance three months. Historical Stress Period is August 2017–December 2019. Final Holdout is October 2025–September 2026.
- Final Holdout access requires a frozen candidate and explicit audited command. Any post-reveal modification creates a new candidate version and cannot reuse the holdout as unseen evidence.
- Public production streams collect aggregate trades, best bid/ask, and differential depth in read-only mode. Trade Flow has historical evidence; Book Flow begins as prospective Shadow Evaluation.
- Book Flow reconstruction follows exchange snapshot and sequence rules. Gaps invalidate a segment. Candidates declare Market Data Dependencies, and missing mandatory inputs block entries.
- Initial order-flow features are executed buyer/seller volume, cumulative delta, trade intensity, best bid/ask imbalance, depth imbalance, and liquidity replenishment/removal. The set remains deliberately small and preregistered.
- Every one-minute signal uses only closed observations and may execute only on a subsequent event. Strategy policies return Entry Proposals; they cannot size exposure or submit orders.
- An Entry Proposal contains candidate version, event time, long direction, structured rationale, feature snapshot, invalidation price, expiry, and Market Data Dependencies. It expires no later than the next one-minute close.
- At most one scalp may be open. A minimum one-bar cooldown follows exit. Stops and circuit breakers precede strategy exits, which precede entries.
- A scalp lasts no more than thirty minutes. From minute 25, break-even-or-better exits may close it; at minute 30, market exit is mandatory. Stops always retain priority.
- Entries use conservative post-confirmation market execution. Account commission is queried from Binance; spread and slippage are calibrated prospectively by size, time, and volatility. Finalists are also stressed at 1.5 and 2 times calibrated cost.
- Initial Position Risk is 0.25% of the lesser of current equity and 10,000 USDT, with position notional capped at 50% of the account. Invalidation is fixed before entry and rounding may never increase risk.
- Daily loss includes realised and unrealised P&L and costs from UTC-day opening equity. At 2%, exposure closes and entries lock until the next UTC day plus Operator confirmation. At 10% drawdown, exposure closes and the system remains locked for manual review; a candidate fails.
- Promoted Position Risk of 0.5% may be evaluated only after complete promotion and Testnet Soak at 0.25%, and only while all existing limits remain satisfied.
- Promotion requires risk compliance, at least 300 out-of-sample scalps, positive net results in at least 70% of folds, and a non-negative lower 95% block-bootstrap bound for net expectancy.
- Metrics include net return, mark-to-market drawdown, maximum daily loss, expectancy, profit factor, daily Sharpe and Sortino, win rate, exposure, turnover, and trade count. They are reported per fold and pooled, with undefined cases labelled non-evaluable.
- Profit concentration fails when one scalp exceeds 10% or one month exceeds 25% of total profit. Candidates are compared with no trade, buy-and-hold, and an unoptimised momentum baseline.
- The system is a Python 3.13 modular monolith with separate runtime processes for collection, research, and execution. One versioned image exposes different entry points.
- One deterministic event-driven engine accepts immutable market, exchange, clock, and Operator events and emits commands plus audit decisions. This interface is the highest shared testing seam.
- Market has live and replay adapters; exchange has Binance Testnet and simulated adapters; state has SQLite and in-memory adapters. The strategy-policy seam is internal to the engine.
- Vectorised Polars/Arrow/DuckDB screening may reject candidates but cannot promote them. Promotion evidence always comes from the shared event engine.
- Money and order values use decimal arithmetic; timestamps use integers; floating point is confined to statistical and indicator calculations with explicit conversion.
- Async I/O is confined to network and orchestration adapters. Domain evaluation remains deterministic and independent of sockets, tasks, databases, or wall clocks.
- The official modular Spot SDK is contained inside the Binance adapter and guarded by contract tests. Domain modules do not expose SDK types.
- Research datasets and raw event archives use S3-compatible object storage with a local filesystem adapter. Derived analytical data uses partitioned Parquet and DuckDB.
- Operational state uses durable SQLite in WAL mode on an exclusive persistent volume. Exactly one executor writes it. Multiple executors, high availability, remote concurrent access, or measured limits require PostgreSQL.
- SQLite records intended and reconciled state; Binance remains authoritative for balances, orders, and fills. Startup and reconnection block entries until reconciliation succeeds.
- Orders receive a deterministic client identifier persisted before submission. Timeouts are Unknown Order Outcomes and must be queried before retry. Filled exposure is protected immediately by an exchange-managed stop and linked take-profit where supported.
- Failure to protect, unreconciled divergence, invalid mandatory data, clock drift, persistence failure, critical alert failure, or rate-limit ban triggers Degraded Mode or Flatten and Lock as appropriate.
- Logs are structured and redacted. Metrics cover data freshness, gaps, latency, order lifecycle, reconciliation, exposure, P&L, circuit breakers, limits, SQLite, and storage.
- Telegram is the initial notification channel. Critical incidents block entries; protective exits remain active. A daily heartbeat makes silent failure visible.
- Audit history is append-only. Current views may update, but decisions, exchange interactions, reconciliation, and Operator actions are never rewritten.
- Local development and research run locally. Continuous collection and Testnet execution deploy as containers through Kubernetes and ArgoCD when ready. Planned deployments occur only while flat.
- Operational backups are encrypted with a five-minute recovery-point objective and thirty-minute recovery-time objective. Restoration is verified monthly in isolation before simulated reconciliation.
- Testnet Ready candidates enter at least fourteen days of Live Shadow. Testnet Soak then requires both thirty calendar days and 300 executed scalps at 0.25% risk.
- Testnet validates order mechanics and operations, not production profitability. One critical incident requires a fix, a regression scenario, and a new seven-day incident-free stability period.
- The delivery order is: project/CI safety; early live-data capture; datasets/replay/risk/reporting; one vertical momentum baseline; first eight-candidate batch; Testnet operations; Live Shadow and Testnet Soak.

## Testing Decisions

- Tests cross the highest useful seam: the event-driven engine interface. They submit immutable event sequences and assert observable commands, audit decisions, and persisted outcomes rather than internal helper behaviour.
- Pure domain tests cover Signal Expiry, feature warm-up, Strategy Rationale, Position Risk, rounding, Timed Exit, cooldown, prioritisation, daily loss, drawdown, and candidate lifecycle.
- Property-based tests assert that risk never exceeds its configured cap, rounding never increases exposure, duplicate events do not duplicate orders, stale proposals cannot execute, and every position is either protected or flattened and locked.
- Replay scenarios cover normal fills, partial fills, commission assets, dust, stop and take-profit races, minute-25 and minute-30 exits, gaps, duplicate and out-of-order market events, session transitions, and cost stress.
- Exchange-contract tests validate signing, time windows, symbol filters, account commission, order submission, linked protection, cancellation, query, rate limits, User Data events, and reconciliation against Binance Testnet.
- Fault-injection tests cover WebSocket loss, missed depth events, HTTP timeout after submission, exchange 5xx, `429`, `418`, clock skew, Telegram loss, process termination, SQLite locks, full storage, corrupt backup, and Kubernetes restart.
- Recovery tests prove that Unknown Order Outcomes are queried before retry, restart with exposure restores the opening configuration, invalid books restart from snapshot, and planned deployment is blocked while non-flat.
- Dataset tests validate checksums, schema, chronological order, timestamp units, gap detection, deterministic feature derivation, and immutable manifest reproduction.
- Statistical tests use synthetic known-edge and no-edge datasets to verify chronological splits, block bootstrap, selection adjustment, concentration checks, undefined-metric handling, and holdout access controls.
- Report tests reconstruct selected scalps end to end and prove that every displayed metric derives from the referenced manifest and immutable artifacts.
- CI requires formatting, lint, strict type checking, tests, secret detection, dependency audit, and container build. Release images are immutable, include an SBOM, and are identified by version and commit.
- Test quality is measured by externally observable behaviour and failure safety, not a coverage percentage. New incidents add replayable regression scenarios.

## Out of Scope

- Binance Mainnet or any use of real capital.
- Futures, margin, shorts, leverage, funding, or liquidation handling.
- Instruments or venues other than Binance Spot `BTCUSDT`.
- Multiple Operators, accounts, tenants, or simultaneous positions.
- A custom web interface or mobile application.
- Neural networks, genetic optimisation, opaque models, or autonomous parameter search.
- Strategy logic that averages down, martingales, uses account losses as an input, or delays stops after entry.
- Treating Testnet fills as evidence of production liquidity or economic edge.
- Purchasing commercial data, research, or subscriptions without a separate approved decision.
- Copying closed or incompatibly licensed strategy implementations.
- Enabling Book Flow to authorize entries before sufficient prospective validation.
- Reusing Spot evidence to approve a future Futures strategy.
- Tax, regulatory, or production-capital readiness; these require a separate Mainnet decision and ADR.

## Further Notes

- Existing release workflows refer to `mean-reversion-bot`, expect missing build artifacts, and are obsolete. They must not run unchanged and will be replaced as part of the project/CI safety phase.
- The Research Target is intentionally aggressive. The system must be able to reject it or conclude that no candidate satisfies it without relaxing risk, changing the holdout, or hiding failed experiments.
- Public source research begins with transparent academic work, open implementations, and fully specified strategies. A source's reported profit is retained as provenance, never as evidence.
- Book Flow collection begins early because prospective history cannot be recreated later. Its absence does not block historical Trade Flow research.
- A future Futures phase must define a new Research Market, dataset, Execution Cost Model, risk rules, and promotion evidence. A future Mainnet phase additionally requires an explicit Operator authorization, new safety review, new ADR, regulatory/tax review, least-privilege production credentials, and a capital-canary plan.
