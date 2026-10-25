from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from scalping_bot.data.storage import LocalObjectStore
from scalping_bot.operations.durable_soak import DurableTestnetSoak

DAY = 86_400_000


def test_testnet_soak_evidence_survives_restart_and_excludes_profitability(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    process = DurableTestnetSoak(
        tmp_path / "soak.db", candidate_identity="candidate-v1", store=store, started_ms=0
    )
    process.record_supervised_window(start_ms=0, end_ms=7 * DAY, alerts_verified=True)
    process.begin_continuous(now_ms=7 * DAY, alerts_verified=True)
    for index in range(300):
        process.record_scalp(
            event_time_ms=7 * DAY + index * (23 * DAY) // 299,
            protected=True,
            duplicate_order=False,
            reconciled=True,
            testnet_pnl=Decimal("100"),
        )
    process.close()

    resumed = DurableTestnetSoak(
        tmp_path / "soak.db", candidate_identity="candidate-v1", store=store
    )
    key = resumed.finalize(now_ms=30 * DAY)

    report = store.read(key).decode()
    assert "Decision: COMPLETE" in report
    assert "Testnet profitability is not production evidence" in report
    assert "Expected end: 2592000000 ms" in report
    resumed.close()
