from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.domain.events import CandleClosed, MarketTick
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.operations.durable_shadow import DurableLiveShadow
from scalping_bot.operations.live_shadow_runtime import SharedEngineLiveShadow
from scalping_bot.operations.promotion import LiveReplayComparison
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.evidence import CandidateLifecycle

DAY = 86_400_000


def _candidate_manifest(path: Path, cost_identity: str) -> None:
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
        candidate,
        parameters={"window_bars": 3, "threshold": "0.005"},
        execution_cost_model_identity=cost_identity,
    ).persist(path)


def _cost_snapshot(path: Path) -> str:
    source = b"shadow cost source\n"
    source_hash = hashlib.sha256(source).hexdigest()
    value = {
        "captured_ms": 1,
        "account_context": "testnet-shadow",
        "calibration_method": "fixture-v1",
        "source_evidence": [f"shadow-source#{source_hash}"],
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
    identity = hashlib.sha256(canonical).hexdigest()
    value["sha256"] = identity
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    return identity


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
    cost_identity = _cost_snapshot(cost_path)
    _candidate_manifest(candidate_path, cost_identity)
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
    status = resumed.health_status(now_ms=14 * DAY)
    assert status["expected_end_ms"] == 14 * DAY
    assert status["process_identity"]
    assert status["release_digest"]
    resumed.close()


def test_live_shadow_derives_comparisons_from_shared_engines_without_orders(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate.json"
    cost_path = tmp_path / "cost.json"
    cost_identity = _cost_snapshot(cost_path)
    _candidate_manifest(candidate_path, cost_identity)
    process = DurableLiveShadow(
        tmp_path / "shadow.db",
        candidate_manifest=candidate_path,
        cost_model=cost_path,
        store=LocalObjectStore(tmp_path / "objects"),
        started_ms=0,
    )
    runner = SharedEngineLiveShadow(
        process, candidate_manifest=candidate_path, cost_model=cost_path
    )
    for minute in range(3):
        runner.process_event(
            CandleClosed(
                event_id=f"kline-{minute}",
                event_time_ms=(minute + 1) * 60_000,
                sequence=minute,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal(str(100 + minute)),
                complete=True,
                volume=Decimal("10"),
                buy_volume=Decimal("7"),
                sell_volume=Decimal("3"),
                trade_count=10,
            ),
            receipt_time_ms=(minute + 1) * 60_000 + 10,
        )
    runner.process_event(
        MarketTick(event_id="tick-3", event_time_ms=180_001, price=Decimal("103")),
        receipt_time_ms=180_011,
    )

    assert len(process.gate.comparisons) == 3
    assert all(item.orders_submitted == 0 for item in process.gate.comparisons)
    process.close()

    resumed = DurableLiveShadow(
        tmp_path / "shadow.db",
        candidate_manifest=candidate_path,
        cost_model=cost_path,
        store=LocalObjectStore(tmp_path / "objects"),
    )
    assert "Hypothetical decisions: 3" in resumed.gate.evaluate(now_ms=180_011).report
    resumed.close()


def test_live_shadow_persists_missing_dependency_as_degraded_evidence(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate.json"
    cost_path = tmp_path / "cost.json"
    cost_identity = _cost_snapshot(cost_path)
    _candidate_manifest(candidate_path, cost_identity)
    store = LocalObjectStore(tmp_path / "objects")
    process = DurableLiveShadow(
        tmp_path / "shadow.db",
        candidate_manifest=candidate_path,
        cost_model=cost_path,
        store=store,
        started_ms=100,
    )
    process.record_missing_dependency(event_time_ms=100, dependency="live-stream")
    process.close()

    resumed = DurableLiveShadow(
        tmp_path / "shadow.db",
        candidate_manifest=candidate_path,
        cost_model=cost_path,
        store=store,
    )
    result = resumed.gate.evaluate(now_ms=14 * DAY)
    assert not result.approved
    assert "missing dependency: live-stream" in result.report
    resumed.close()


def test_live_shadow_health_fails_closed_after_a_continuity_break(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate.json"
    cost_path = tmp_path / "cost.json"
    cost_identity = _cost_snapshot(cost_path)
    _candidate_manifest(candidate_path, cost_identity)
    process = DurableLiveShadow(
        tmp_path / "shadow.db",
        candidate_manifest=candidate_path,
        cost_model=cost_path,
        store=LocalObjectStore(tmp_path / "objects"),
        started_ms=0,
    )
    process.record_comparison(_comparison(100))
    assert process.health_status(now_ms=110)["healthy"] is True

    process.mark_continuity_broken("stream reconnect")

    status = process.health_status(now_ms=110)
    assert status["continuity_broken"] is True
    assert status["healthy"] is False
    process.close()
