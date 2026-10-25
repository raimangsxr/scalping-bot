from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from scalping_bot.domain.engine import Engine
from scalping_bot.domain.events import CandleClosed
from scalping_bot.domain.state import InMemoryState
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.health import HealthSignals, OperationalHealthController
from scalping_bot.strategies.momentum import MomentumPolicy


def test_operational_health_fails_closed_and_records_redacted_transition(tmp_path: Path) -> None:
    with SQLiteState(tmp_path / "state.db") as state:
        controller = OperationalHealthController(state)
        result = controller.apply(
            HealthSignals(
                market_data_freshness_ms=20_000,
                clock_drift_ms=2_000,
                persistence_ok=True,
                storage_ok=True,
                notification_ok=False,
                reconciliation_ok=False,
                rate_limit_blocked=False,
                dependency_status={"aggTrades": False},
            ),
            event_time_ms=10,
        )

        assert result.entries_allowed is False
        assert result.reasons
        assert state.operating_mode.value == "DEGRADED"
        assert "operational-health-degraded" in state.audit_kinds()


def test_operational_health_blocks_entries_when_either_exchange_stream_is_down(
    tmp_path: Path,
) -> None:
    with SQLiteState(tmp_path / "state.db") as state:
        result = OperationalHealthController(state).apply(
            HealthSignals(
                market_data_freshness_ms=100,
                persistence_ok=True,
                storage_ok=True,
                notification_ok=True,
                reconciliation_ok=True,
                exchange_streams_connected=False,
            ),
            event_time_ms=10,
        )

        assert not result.entries_allowed
        assert "exchange event streams are not connected" in result.reasons


def test_engine_blocks_a_proposal_when_its_declared_dependency_is_invalid() -> None:
    state = InMemoryState(initial_equity=Decimal("10000"))
    state.dependency_status["closed-1m-klines"] = False
    engine = Engine(
        state,
        MomentumPolicy(candidate_version="momentum-v1", window=2, threshold=Decimal("0")),
    )
    engine.handle(
        CandleClosed(
            event_id="c-1",
            event_time_ms=1_000,
            sequence=1,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            close=Decimal("100"),
            complete=True,
        )
    )
    engine.handle(
        CandleClosed(
            event_id="c-2",
            event_time_ms=61_000,
            sequence=2,
            open=Decimal("100"),
            high=Decimal("102"),
            low=Decimal("100"),
            close=Decimal("102"),
            complete=True,
        )
    )

    assert engine.state.pending_proposal is None
    assert engine.state.audit[-1].kind == "entry-blocked-dependency"
