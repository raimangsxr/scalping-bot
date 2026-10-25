"""Restart-safe Live Shadow evidence without an exchange adapter."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict
from pathlib import Path

from scalping_bot.data.storage import ObjectStore
from scalping_bot.domain.costs import ExecutionCostSnapshot
from scalping_bot.execution.candidate import PromotedCandidateManifest
from scalping_bot.operations.promotion import (
    DAY_MS,
    LIVE_SHADOW_DAYS,
    LiveReplayComparison,
    LiveShadowGate,
)


class DurableLiveShadow:
    """Persist every observation before allowing a final shadow decision."""

    def __init__(
        self,
        path: Path,
        *,
        candidate_manifest: Path,
        cost_model: Path,
        store: ObjectStore,
        started_ms: int | None = None,
    ) -> None:
        self.store = store
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS comparisons (
                event_time_ms INTEGER PRIMARY KEY, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS resolutions (
                divergence TEXT PRIMARY KEY, resolution TEXT NOT NULL, regression TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS continuity (
                id INTEGER PRIMARY KEY AUTOINCREMENT, reason TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS order_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT, event_time_ms INTEGER NOT NULL,
                client_order_id TEXT NOT NULL
            );
            """
        )
        candidate = PromotedCandidateManifest.load(candidate_manifest)
        # Loading both artifacts is part of construction: a process cannot run on
        # an unverified candidate or an unreadable cost model.
        ExecutionCostSnapshot.load(cost_model)
        candidate_sha = _sha256(candidate_manifest.read_bytes())
        cost_sha = _sha256(cost_model.read_bytes())
        stored = self._metadata()
        if not stored:
            if started_ms is None or started_ms < 0:
                raise ValueError("a new Live Shadow run requires a non-negative start time")
            self._put_metadata(
                {
                    "candidate_identity": candidate.candidate_identity,
                    "candidate_manifest_sha256": candidate_sha,
                    "cost_model_sha256": cost_sha,
                    "started_ms": str(started_ms),
                }
            )
        elif (
            stored.get("candidate_identity") != candidate.candidate_identity
            or stored.get("candidate_manifest_sha256") != candidate_sha
            or stored.get("cost_model_sha256") != cost_sha
        ):
            raise ValueError("Live Shadow artifacts changed during a restart")
        effective_start = int(stored.get("started_ms", str(started_ms)))
        self.gate = LiveShadowGate(
            candidate_identity=candidate.candidate_identity,
            started_ms=effective_start,
            candidate_manifest_sha256=candidate_sha,
        )
        self._restore()

    @property
    def started_ms(self) -> int:
        return self.gate.started_ms

    def record_comparison(self, comparison: LiveReplayComparison) -> None:
        payload = _comparison_bytes(comparison)
        row = self.connection.execute(
            "SELECT payload FROM comparisons WHERE event_time_ms = ?",
            (comparison.event_time_ms,),
        ).fetchone()
        if row is not None:
            if str(row["payload"]).encode() != payload:
                raise ValueError("Live Shadow comparison is immutable")
            return
        self.gate.record(comparison)
        self.connection.execute(
            "INSERT INTO comparisons(event_time_ms, payload) VALUES (?, ?)",
            (comparison.event_time_ms, payload.decode()),
        )
        self.connection.commit()

    def record_missing_dependency(self, *, event_time_ms: int, dependency: str) -> None:
        self.gate.missing_dependency(event_time_ms=event_time_ms, dependency=dependency)
        self._persist_last_comparison()

    def record_order_submission(self, *, event_time_ms: int, client_order_id: str) -> None:
        self.gate.record_order_submission(
            event_time_ms=event_time_ms, client_order_id=client_order_id
        )
        self.connection.execute(
            "INSERT INTO order_attempts(event_time_ms, client_order_id) VALUES (?, ?)",
            (event_time_ms, client_order_id),
        )
        self.connection.commit()

    def resolve_divergence(self, divergence: str, *, resolution: str, regression: str) -> None:
        self.gate.resolve_divergence(divergence, resolution=resolution, regression=regression)
        self.connection.execute(
            "INSERT OR REPLACE INTO resolutions(divergence, resolution, regression) "
            "VALUES (?, ?, ?)",
            (divergence, resolution, regression),
        )
        self.connection.commit()

    def mark_continuity_broken(self, reason: str) -> None:
        self.gate.mark_continuity_broken(reason)
        self.connection.execute("INSERT INTO continuity(reason) VALUES (?)", (reason,))
        self.connection.commit()

    def finalize(self, *, now_ms: int) -> str:
        if now_ms - self.started_ms < LIVE_SHADOW_DAYS * DAY_MS:
            raise ValueError("Live Shadow cannot finalize before fourteen continuous days")
        result = self.gate.evaluate(now_ms=now_ms)
        key = self.gate.persist_report(result, self.store)
        self._put_metadata({"final_report_key": key})
        self.connection.commit()
        return key

    def close(self) -> None:
        self.connection.commit()
        self.connection.close()

    def _restore(self) -> None:
        rows = self.connection.execute(
            "SELECT payload FROM comparisons ORDER BY event_time_ms"
        ).fetchall()
        for row in rows:
            self.gate.record(_comparison(str(row["payload"])))
        for row in self.connection.execute(
            "SELECT divergence, resolution, regression FROM resolutions ORDER BY divergence"
        ):
            self.gate.resolve_divergence(
                str(row["divergence"]),
                resolution=str(row["resolution"]),
                regression=str(row["regression"]),
            )
        for row in self.connection.execute("SELECT reason FROM continuity ORDER BY id"):
            self.gate.mark_continuity_broken(str(row["reason"]))
        for row in self.connection.execute(
            "SELECT event_time_ms, client_order_id FROM order_attempts ORDER BY id"
        ):
            self.gate.record_order_submission(
                event_time_ms=int(row["event_time_ms"]),
                client_order_id=str(row["client_order_id"]),
            )

    def _persist_last_comparison(self) -> None:
        comparison = self.gate.comparisons[-1]
        self.connection.execute(
            "INSERT OR IGNORE INTO comparisons(event_time_ms, payload) VALUES (?, ?)",
            (comparison.event_time_ms, _comparison_bytes(comparison).decode()),
        )
        self.connection.commit()

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


def _comparison_bytes(comparison: LiveReplayComparison) -> bytes:
    return json.dumps(asdict(comparison), sort_keys=True, separators=(",", ":")).encode()


def _comparison(value: str) -> LiveReplayComparison:
    raw = json.loads(value)
    if not isinstance(raw, dict):
        raise ValueError("stored Live Shadow comparison is malformed")
    return LiveReplayComparison(
        event_time_ms=int(str(raw["event_time_ms"])),
        live_features=dict(raw["live_features"]),
        replay_features=dict(raw["replay_features"]),
        live_signal=bool(raw["live_signal"]),
        replay_signal=bool(raw["replay_signal"]),
        expiry_match=bool(raw["expiry_match"]),
        latency_ms=int(str(raw["latency_ms"])),
        data_quality=str(raw["data_quality"]),
        hypothetical_cost=str(raw["hypothetical_cost"]),
        divergence=(str(raw["divergence"]) if raw.get("divergence") is not None else None),
        replay_hypothetical_cost=(
            str(raw["replay_hypothetical_cost"])
            if raw.get("replay_hypothetical_cost") is not None
            else None
        ),
        replay_data_quality=(
            str(raw["replay_data_quality"]) if raw.get("replay_data_quality") is not None else None
        ),
        replay_latency_ms=(
            int(str(raw["replay_latency_ms"])) if raw.get("replay_latency_ms") is not None else None
        ),
        live_expiry_ms=(
            int(str(raw["live_expiry_ms"])) if raw.get("live_expiry_ms") is not None else None
        ),
        replay_expiry_ms=(
            int(str(raw["replay_expiry_ms"])) if raw.get("replay_expiry_ms") is not None else None
        ),
        orders_submitted=int(str(raw.get("orders_submitted", 0))),
    )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
