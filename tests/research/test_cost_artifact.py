from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.domain.costs import ExecutionCostSnapshot
from scalping_bot.research.costs import (
    AccountCommissionSnapshot,
    CalibrationObservation,
    ExecutionCostCalibrator,
    ExecutionCostModel,
    ExecutionCostModelPublisher,
)


def _observation(hour: int) -> CalibrationObservation:
    return CalibrationObservation(
        time_ms=1_700_000_000_000 + hour * 1_000,
        hour_utc=hour,
        volatility_bucket="low",
        size_bucket="small",
        bid=Decimal("99.90"),
        ask=Decimal("100.10"),
        simulated_buy_price=Decimal("100.15"),
    )


def test_cost_model_requires_evidence_for_declared_buckets_and_is_content_addressed(
    tmp_path: Path,
) -> None:
    commission = AccountCommissionSnapshot(
        symbol="BTCUSDT",
        maker=Decimal("0.001"),
        taker=Decimal("0.001"),
        captured_ms=1,
        account_context="testnet-account",
    )
    calibrator = ExecutionCostCalibrator()
    with pytest.raises(ValueError, match="missing calibration bucket"):
        calibrator.fit(
            (_observation(10),),
            commission=commission,
            required_buckets=((10, "low", "small"), (11, "low", "small")),
        )

    model = calibrator.fit(
        (_observation(10),),
        commission=commission,
        required_buckets=((10, "low", "small"),),
    )
    store = LocalObjectStore(tmp_path / "objects")
    key = ExecutionCostModelPublisher(store).publish(
        model, source_evidence=("catalogs/live/segment.json",)
    )
    loaded = ExecutionCostModel.load(store, key)

    assert key.endswith(".json")
    assert loaded.identity == key.rsplit("/", 1)[-1].removesuffix(".json")
    assert loaded.source_evidence == ("catalogs/live/segment.json",)
    assert loaded.estimate(
        notional=Decimal("1000"), hour_utc=10, volatility_bucket="low", size_bucket="small"
    ) == model.estimate(
        notional=Decimal("1000"), hour_utc=10, volatility_bucket="low", size_bucket="small"
    )

    runtime_path = tmp_path / f"{key.rsplit('/', 1)[-1]}"
    runtime_path.write_bytes(store.read(key))
    snapshot = ExecutionCostSnapshot.load(runtime_path)
    assert snapshot.snapshot_id == loaded.identity
    assert snapshot.commission_rate == commission.taker
    mounted_path = tmp_path / "snapshot.json"
    mounted_path.write_bytes(store.read(key))
    assert ExecutionCostSnapshot.load(mounted_path).snapshot_id == loaded.identity
