"""Recorded operational recovery and release drills."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from scalping_bot.data.storage import ObjectStore
from scalping_bot.operations.backup import EncryptedSQLiteBackup
from scalping_bot.operations.release import ReleaseVerification


@dataclass(frozen=True)
class DrillOutcome:
    name: str
    passed: bool
    target_rpo_seconds: int
    target_rto_seconds: int
    measured_rpo_seconds: int | None
    measured_rto_seconds: int | None
    details: dict[str, object]

    @property
    def markdown(self) -> str:
        measured_rpo = self.measured_rpo_seconds if self.measured_rpo_seconds is not None else "n/e"
        measured_rto = self.measured_rto_seconds if self.measured_rto_seconds is not None else "n/e"
        return (
            f"# Operational drill — {self.name}\n\n"
            f"Decision: {'PASS' if self.passed else 'FAIL'}\n"
            f"Target RPO: {self.target_rpo_seconds}s\n"
            f"Target RTO: {self.target_rto_seconds}s\n"
            f"Measured RPO: {measured_rpo}s\n"
            f"Measured RTO: {measured_rto}s\n\n"
            "Details:\n"
            + "\n".join(f"- {key}: {value}" for key, value in sorted(self.details.items()))
            + "\n"
        )


class OperationalDrillRunner:
    """Run and persist finite, repeatable operational drills."""

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def backup_restore(
        self,
        backup: EncryptedSQLiteBackup,
        artifact: Path,
        *,
        destination: Path,
        backup_created_ms: int,
        observed_ms: int,
        restore_started_ms: int,
        restore_completed_ms: int,
    ) -> DrillOutcome:
        if observed_ms < backup_created_ms or restore_completed_ms < restore_started_ms:
            raise ValueError("drill timestamps must be chronological")
        restored = backup.restore_and_verify(artifact, destination=destination)
        measured_rpo = (observed_ms - backup_created_ms) // 1000
        measured_rto = (restore_completed_ms - restore_started_ms) // 1000
        outcome = DrillOutcome(
            name="encrypted-backup-restore",
            passed=measured_rpo <= backup.RPO_SECONDS and measured_rto <= backup.RTO_SECONDS,
            target_rpo_seconds=backup.RPO_SECONDS,
            target_rto_seconds=backup.RTO_SECONDS,
            measured_rpo_seconds=measured_rpo,
            measured_rto_seconds=measured_rto,
            details={"restored_database": str(restored), "integrity": "ok"},
        )
        return outcome

    def release_verification(self, verification: ReleaseVerification) -> DrillOutcome:
        outcome = DrillOutcome(
            name="immutable-release-verification",
            passed=True,
            target_rpo_seconds=EncryptedSQLiteBackup.RPO_SECONDS,
            target_rto_seconds=EncryptedSQLiteBackup.RTO_SECONDS,
            measured_rpo_seconds=None,
            measured_rto_seconds=None,
            details={
                "image_revision": verification.image_revision,
                "target_revision": verification.target_revision,
                "processes": sorted(verification.processes),
            },
        )
        return outcome

    def persist(self, outcome: DrillOutcome) -> str:
        payload = (
            json.dumps(asdict(outcome), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        identity = hashlib.sha256(payload).hexdigest()
        key = f"operations/drills/{outcome.name}/{identity}.json"
        self.store.put_immutable(key, payload)
        self.store.put_immutable(
            f"operations/drills/{outcome.name}/{identity}.md", outcome.markdown.encode()
        )
        return key
