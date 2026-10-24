from __future__ import annotations

import hashlib
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.aggtrades import AggTradeArchivePipeline
from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.research.costs import (
    AccountCommissionSnapshot,
    CalibrationObservation,
    ExecutionCostCalibrator,
)
from scalping_bot.research.execution import AggTrade, AggTradeReplay


def test_official_aggtrades_are_immutable_validated_and_manifested(tmp_path: Path) -> None:
    archive = tmp_path / "agg.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(
            "BTCUSDT-aggTrades-2026-01.csv",
            "1,90000.00,0.01,10,10,1767225600000,false,true\n"
            "2,90001.00,0.02,11,12,1767225600100,true,true\n",
        )
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()

    manifest = AggTradeArchivePipeline(LocalObjectStore(tmp_path / "objects")).ingest_file(
        archive,
        expected_sha256=checksum,
        source_uri="https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/file.zip",
    )

    assert manifest.row_count == 2
    assert manifest.first_aggregate_trade_id == 1
    assert manifest.last_aggregate_trade_id == 2
    assert (tmp_path / "objects" / manifest.parquet_key).exists()


def test_execution_cost_model_is_calibrated_by_size_hour_and_volatility() -> None:
    observations = (
        CalibrationObservation(
            time_ms=1_700_000_000_000,
            hour_utc=10,
            volatility_bucket="low",
            size_bucket="small",
            bid=Decimal("99.90"),
            ask=Decimal("100.10"),
            simulated_buy_price=Decimal("100.15"),
        ),
        CalibrationObservation(
            time_ms=1_700_000_001_000,
            hour_utc=10,
            volatility_bucket="low",
            size_bucket="small",
            bid=Decimal("99.80"),
            ask=Decimal("100.20"),
            simulated_buy_price=Decimal("100.25"),
        ),
    )
    commission = AccountCommissionSnapshot(
        symbol="BTCUSDT", maker=Decimal("0.001"), taker=Decimal("0.001"), captured_ms=1
    )
    model = ExecutionCostCalibrator().fit(observations, commission=commission)

    calibrated = model.estimate(
        notional=Decimal("1000"), hour_utc=10, volatility_bucket="low", size_bucket="small"
    )
    stressed = model.estimate(
        notional=Decimal("1000"),
        hour_utc=10,
        volatility_bucket="low",
        size_bucket="small",
        stress_multiplier=Decimal("2"),
    )

    assert calibrated > Decimal("1")
    assert stressed == calibrated * 2
    assert model.scenarios == (Decimal("1"), Decimal("1.5"), Decimal("2"))


def test_aggregate_trade_execution_waits_for_post_confirmation_trade() -> None:
    trades = (
        AggTrade(1, 1_000, Decimal("100"), Decimal("1"), buyer_maker=False),
        AggTrade(2, 1_001, Decimal("100.10"), Decimal("1"), buyer_maker=False),
        AggTrade(3, 1_002, Decimal("102"), Decimal("1"), buyer_maker=True),
    )

    events = AggTradeReplay().events_after(1_000, trades)

    assert [event.event_id for event in events] == ["aggTrade-2", "aggTrade-3"]
    assert events[0].price == Decimal("100.10")


def test_out_of_order_aggregate_trades_are_rejected() -> None:
    trades = (
        AggTrade(2, 1_001, Decimal("100"), Decimal("1"), buyer_maker=False),
        AggTrade(1, 1_000, Decimal("99"), Decimal("1"), buyer_maker=True),
    )
    with pytest.raises(ValueError, match="chronological"):
        AggTradeReplay().events_after(999, trades)
