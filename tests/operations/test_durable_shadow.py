from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.operations.durable_shadow import DurableLiveShadow
from scalping_bot.operations.promotion import LiveReplayComparison
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.evidence import CandidateLifecycle

DAY = 86_400_000


def _candidate_manifest(path: Path) -> None:
    candidate = first_batch().candidates[0]
    for target in (
        CandidateLifecycle.SPECIFIED,
        CandidateLifecycle.SCREENED,
        CandidateLifecycle.WALK_FORWARD_VALIDATED,
        CandidateLifecycle.HOLDOUT_TESTED,
        CandidateLifecycle.TESTNET_READY,
    ):
        candidate = candidate.transition(target, evidence="fixture")
    PromotedCandidateManifest.from_candidate(
        candidate, parameters={"window_bars": 3, "threshold": "0.005"}
    ).persist(path)


def _cost_snapshot(path: Path) -> None:
    value = {
        "captured_ms": 1,
        "commission_rate": "0.001",
        "buckets": [
            {
                "hour_utc": None,
                "volatility_bucket": "low",
                "minimum_notional": "0",
                "maximum_notional": None,
                "half_spread_rate": "0.0001",
                "slippage_rate": "0.0001",
                "observations": 10,
            }
        ],
    }
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    value["sha256"] = hashlib.sha256(canonical).hexdigest()
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _comparison(event_time_ms: int) -> LiveReplayComparison:
    return LiveReplayComparison(
        event_time_ms=event_time_ms,
        live_features={"x": "1"},
        replay_features={"x": "1"},
        live_signal=False,
        replay_signal=False,
        expiry_match=True,
        latency_ms=10,
        data_quality="valid",
        hypothetical_cost="1.2",
    )


def test_live_shadow_state_survives_restart_and_only_finalizes_after_fourteen_days(
    tmp_path: Path,
) -> None:
    candidate_path = tmp_path / "candidate.json"
    cost_path = tmp_path / "cost.json"
    _candidate_manifest(candidate_path)
    _cost_snapshot(cost_path)
    store = LocalObjectStore(tmp_path / "objects")
    process = DurableLiveShadow(
        tmp_path / "shadow.db",
        candidate_manifest=candidate_path,
        cost_model=cost_path,
        store=store,
        started_ms=0,
    )
    for day in range(1, 15):
        process.record_comparison(_comparison(day * DAY))
    process.close()

    resumed = DurableLiveShadow(
        tmp_path / "shadow.db",
        candidate_manifest=candidate_path,
        cost_model=cost_path,
        store=store,
    )
    key = resumed.finalize(now_ms=14 * DAY)

    assert store.read(key).decode().startswith("# Live Shadow")
    assert "APPROVE Testnet Soak" in store.read(key).decode()
    resumed.close()
