"""Durable Testnet Soak evidence assembled from the executor's state."""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

from scalping_bot.data.storage import ObjectStore
from scalping_bot.execution.sqlite_state import SQLiteState
from scalping_bot.operations.promotion import SoakGate, SoakResult


class DurableTestnetSoak:
    def __init__(
        self,
        path: Path,
        *,
        candidate_identity: str,
        store: ObjectStore,
        started_ms: int | None = None,
    ) -> None:
        self.store = store
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS supervised_windows (
                start_ms INTEGER PRIMARY KEY, end_ms INTEGER NOT NULL,
                alerts_verified INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scalps (
                event_time_ms INTEGER PRIMARY KEY, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS incidents (
                event_time_ms INTEGER PRIMARY KEY, payload TEXT NOT NULL
            );
            """
        )
        metadata = self._metadata()
        if not metadata:
            if started_ms is None or started_ms < 0:
                raise ValueError("a new Testnet Soak requires a non-negative start time")
            self._put_metadata(
                {"candidate_identity": candidate_identity, "started_ms": str(started_ms)}
            )
        elif metadata.get("candidate_identity") != candidate_identity:
            raise ValueError("Testnet Soak candidate changed during a restart")
        self.gate = SoakGate(
            candidate_identity=candidate_identity,
            started_ms=int(metadata.get("started_ms", str(started_ms))),
        )
        self._restore()

    @property
    def started_ms(self) -> int:
        return self.gate.started_ms

    @property
    def scalps(self) -> int:
        return self.gate.scalps

    def record_supervised_window(
        self, *, start_ms: int, end_ms: int, alerts_verified: bool
    ) -> None:
        existing = self.connection.execute(
            "SELECT end_ms, alerts_verified FROM supervised_windows WHERE start_ms = ?",
            (start_ms,),
        ).fetchone()
        expected = (end_ms, int(alerts_verified))
        if existing is not None:
            if (int(existing["end_ms"]), int(existing["alerts_verified"])) != expected:
                raise ValueError("supervised window is immutable")
            return
        self.gate.record_supervised_window(
            start_ms=start_ms, end_ms=end_ms, alerts_verified=alerts_verified
        )
        self.connection.execute(
            "INSERT INTO supervised_windows(start_ms, end_ms, alerts_verified) VALUES (?, ?, ?)",
            (start_ms, end_ms, int(alerts_verified)),
        )
        self.connection.commit()

    def begin_continuous(self, *, now_ms: int, alerts_verified: bool) -> None:
        self.gate.begin_continuous(now_ms=now_ms, alerts_verified=alerts_verified)
        self._put_metadata({"continuous_started_ms": str(now_ms), "alerts_verified": "1"})

    def record_scalp(
        self,
        *,
        event_time_ms: int,
        protected: bool,
        duplicate_order: bool,
        reconciled: bool,
        testnet_pnl: Decimal,
        position_risk: Decimal | None = None,
        notional_fraction: Decimal | None = None,
        unresolved_divergences: tuple[str, ...] = (),
        executed: bool = True,
    ) -> None:
        payload = {
            "event_time_ms": event_time_ms,
            "protected": protected,
            "duplicate_order": duplicate_order,
            "reconciled": reconciled,
            "testnet_pnl": str(testnet_pnl),
            "position_risk": str(position_risk) if position_risk is not None else None,
            "notional_fraction": (
                str(notional_fraction) if notional_fraction is not None else None
            ),
            "unresolved_divergences": list(unresolved_divergences),
            "executed": executed,
        }
        content = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        existing = self.connection.execute(
            "SELECT payload FROM scalps WHERE event_time_ms = ?", (event_time_ms,)
        ).fetchone()
        if existing is not None:
            if str(existing["payload"]) != content:
                raise ValueError("Testnet Soak scalp evidence is immutable")
            return
        self.gate.record_scalp(
            event_time_ms=event_time_ms,
            protected=protected,
            duplicate_order=duplicate_order,
            reconciled=reconciled,
            testnet_pnl=testnet_pnl,
            position_risk=position_risk,
            notional_fraction=notional_fraction,
            unresolved_divergences=unresolved_divergences,
            executed=executed,
        )
        self.connection.execute(
            "INSERT INTO scalps(event_time_ms, payload) VALUES (?, ?)",
            (event_time_ms, content),
        )
        self.connection.commit()

    def record_authoritative_scalp(
        self,
        state: SQLiteState,
        *,
        entry_client_order_id: str,
        event_time_ms: int,
        position_risk: Decimal,
        notional_fraction: Decimal,
    ) -> None:
        trace = state.reconstruct_scalp(entry_client_order_id)
        protection = trace["protection"]
        pnl_value = trace.get("pnl")
        pnl = pnl_value.get("net_pnl") if isinstance(pnl_value, dict) else None
        testnet_pnl = (
            Decimal(str(pnl)) if pnl is not None and not str(pnl).startswith("non-") else Decimal(0)
        )
        reconciliation = state.latest_reconciliation()
        self.record_scalp(
            event_time_ms=event_time_ms,
            protected=protection is not None,
            duplicate_order=False,
            reconciled=bool(reconciliation["consistent"]),
            testnet_pnl=testnet_pnl,
            position_risk=position_risk,
            notional_fraction=notional_fraction,
            unresolved_divergences=tuple(str(item) for item in reconciliation["divergences"]),
        )

    def record_incident(
        self, *, event_time_ms: int, critical: bool, regression: str | None
    ) -> None:
        payload = json.dumps(
            {"event_time_ms": event_time_ms, "critical": critical, "regression": regression},
            sort_keys=True,
            separators=(",", ":"),
        )
        existing = self.connection.execute(
            "SELECT payload FROM incidents WHERE event_time_ms = ?", (event_time_ms,)
        ).fetchone()
        if existing is not None:
            if str(existing["payload"]) != payload:
                raise ValueError("Testnet Soak incident is immutable")
            return
        self.gate.record_incident(
            event_time_ms=event_time_ms, critical=critical, regression=regression
        )
        self.connection.execute(
            "INSERT INTO incidents(event_time_ms, payload) VALUES (?, ?)",
            (event_time_ms, payload),
        )
        self.connection.commit()

    def finalize(self, *, now_ms: int) -> str:
        if now_ms - self.started_ms < 30 * 86_400_000:
            raise ValueError("Testnet Soak cannot finalize before thirty calendar days")
        result = self.gate.evaluate(now_ms=now_ms)
        key = self.gate.persist_report(result, self.store)
        self._put_metadata({"final_report_key": key})
        return key

    def evaluate(self, *, now_ms: int) -> SoakResult:
        return self.gate.evaluate(now_ms=now_ms)

    def close(self) -> None:
        self.connection.commit()
        self.connection.close()

    def _restore(self) -> None:
        for row in self.connection.execute(
            "SELECT start_ms, end_ms, alerts_verified FROM supervised_windows ORDER BY start_ms"
        ):
            self.gate.record_supervised_window(
                start_ms=int(row["start_ms"]),
                end_ms=int(row["end_ms"]),
                alerts_verified=bool(row["alerts_verified"]),
            )
        metadata = self._metadata()
        if metadata.get("continuous_started_ms") is not None:
            self.gate.continuous_started_ms = int(metadata["continuous_started_ms"])
            self.gate.alerts_verified = metadata.get("alerts_verified") == "1"
        for row in self.connection.execute(
            "SELECT event_time_ms, payload FROM incidents ORDER BY event_time_ms"
        ):
            value = json.loads(str(row["payload"]))
            self.gate.record_incident(
                event_time_ms=int(value["event_time_ms"]),
                critical=bool(value["critical"]),
                regression=(
                    str(value["regression"]) if value.get("regression") is not None else None
                ),
            )
        for row in self.connection.execute(
            "SELECT event_time_ms, payload FROM scalps ORDER BY event_time_ms"
        ):
            value = json.loads(str(row["payload"]))
            self.gate.record_scalp(
                event_time_ms=int(value["event_time_ms"]),
                protected=bool(value["protected"]),
                duplicate_order=bool(value["duplicate_order"]),
                reconciled=bool(value["reconciled"]),
                testnet_pnl=Decimal(str(value["testnet_pnl"])),
                position_risk=(
                    Decimal(str(value["position_risk"]))
                    if value.get("position_risk") is not None
                    else None
                ),
                notional_fraction=(
                    Decimal(str(value["notional_fraction"]))
                    if value.get("notional_fraction") is not None
                    else None
                ),
                unresolved_divergences=tuple(str(item) for item in value["unresolved_divergences"]),
                executed=bool(value["executed"]),
            )

    def _metadata(self) -> dict[str, str]:
        return {
            str(row["key"]): str(row["value"])
            for row in self.connection.execute("SELECT key, value FROM metadata").fetchall()
        }

    def _put_metadata(self, values: dict[str, str]) -> None:
        self.connection.executemany(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)", values.items()
        )
        self.connection.commit()
