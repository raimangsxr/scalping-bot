from __future__ import annotations

from dataclasses import dataclass

from scalping_bot.research.evidence import CandidateRecord


@dataclass(frozen=True)
class CandidateBatch:
    candidates: tuple[CandidateRecord, ...]

    def __post_init__(self) -> None:
        if len(self.candidates) > 8:
            raise ValueError("the first research batch is capped at eight candidates")
        identities = {candidate.identity for candidate in self.candidates}
        if len(identities) != len(self.candidates):
            raise ValueError("candidate identities must be unique")
        families = {candidate.family for candidate in self.candidates}
        for family in families:
            if sum(candidate.family == family for candidate in self.candidates) > 2:
                raise ValueError("the first batch permits at most two candidates per family")


def first_batch() -> CandidateBatch:
    common_rejections = (
        "non-positive calibrated net expectancy",
        "risk or concentration gate failure",
        "fewer than 300 out-of-sample scalps at promotion",
    )
    candidates = (
        CandidateRecord(
            "momentum-001",
            1,
            "momentum",
            "https://www.nber.org/papers/w24877",
            "open working-paper access; rules reimplemented from description",
            "short positive BTC returns may persist into the next completed interval",
            {"signal": "sign and magnitude of completed-bar return"},
            {"market": "BTCUSDT", "direction": "long-only", "holding_cap_minutes": 30},
            {"window_bars": [3, 5], "threshold": ["0.005", "0.01"]},
            common_rejections,
        ),
        CandidateRecord(
            "momentum-002",
            1,
            "momentum",
            "https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1992.tb04681.x",
            "published rule description; no source code reused",
            "a short moving-average trend may add value beyond raw return momentum",
            {"signal": "short completed-bar average above long completed-bar average"},
            {"direction": "long-only", "execution": "next event"},
            {"short_window": [3, 5], "long_window": [10, 15]},
            common_rejections,
        ),
        CandidateRecord(
            "breakout-001",
            1,
            "volatility-breakout",
            "https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1992.tb04681.x",
            "published trading-range-break description; no source code reused",
            "a close above the prior completed trading range may precede expansion",
            {"signal": "close above prior N-bar high"},
            {"confirmation": "completed close", "execution": "next event"},
            {"lookback": [10, 20], "buffer_bps": [0, 5]},
            common_rejections,
        ),
        CandidateRecord(
            "breakout-002",
            1,
            "volatility-breakout",
            "https://onlinelibrary.wiley.com/doi/10.1016/j.rfe.2013.05.004",
            "published out-of-sample replication; no source code reused",
            "range breaks may need an objective volatility expansion confirmation",
            {"signal": "prior range break"},
            {"confirmation": "true range above rolling median"},
            {"lookback": [10, 20], "range_multiplier": ["1", "1.5"]},
            common_rejections,
        ),
        CandidateRecord(
            "mean-reversion-001",
            1,
            "regime-conditioned-mean-reversion",
            "https://www.nber.org/papers/w2343",
            "open working-paper access; hypothesis only, no implementation reused",
            "transitory deviations may revert only when the local path is lateral",
            {"signal": "negative deviation from causal rolling mean"},
            {"benchmark": "causal rolling VWAP", "regime": "low efficiency ratio"},
            {"window": [20, 30], "deviation": ["0.005", "0.01"]},
            common_rejections,
        ),
        CandidateRecord(
            "mean-reversion-002",
            1,
            "regime-conditioned-mean-reversion",
            "https://arxiv.org/abs/2608.21888",
            "open preprint; measurement idea only, no source code reused",
            "short-horizon crypto reversal may survive costs in objectively quiet regimes",
            {"signal": "completed-bar downside displacement"},
            {"benchmark": "causal VWAP", "regime": "bounded range and low path efficiency"},
            {"window": [15, 30], "deviation": ["0.0075", "0.0125"]},
            common_rejections,
        ),
        CandidateRecord(
            "trade-flow-001",
            1,
            "executed-trade-pressure",
            "https://arxiv.org/abs/1011.6402",
            "open author preprint; feature definitions reimplemented",
            "executed buyer pressure may confirm an independent price entry",
            {"features": ["executed volume imbalance", "cumulative delta"]},
            {"role": "confirmation only", "visible liquidity": "excluded"},
            {"window_seconds": [10, 30], "minimum_delta": ["0", "0.2"]},
            common_rejections,
        ),
        CandidateRecord(
            "trade-flow-002",
            1,
            "executed-trade-pressure",
            "https://arxiv.org/abs/2004.08290",
            "open author preprint; feature definitions reimplemented",
            "trade intensity plus signed executed volume may filter weak breakouts",
            {"features": ["trade intensity", "executed volume imbalance"]},
            {"role": "confirmation only", "market": "BTCUSDT"},
            {"window_seconds": [5, 15], "minimum_intensity": [5, 10]},
            common_rejections,
        ),
    )
    return CandidateBatch(candidates)
