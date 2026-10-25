# Executor deployment and rollback

ArgoCD deploys exactly one executor from the immutable release manifests. `targetRevision` is the
full commit containing the manifests; those manifests reference an image tagged with its verified
source commit, and the image must have a matching SPDX SBOM. The two immutable commits are
recorded together and advanced only after CI passes.

Before sync, the `PreSync` deployment gate reads SQLite without becoming a writer and requires:

1. no local order intent or protected exposure remains open;
2. the latest exchange reconciliation is successful and has zero divergences;
3. a non-empty encrypted backup exists at `state/backups/latest.sqlite.fernet`; and
4. the target and current SQLite schema versions are identical.

If any prerequisite fails, the ArgoCD sync fails without replacing the executor.

The executor publishes encrypted five-minute backups for its primary SQLite database and the
separate operator-command and Testnet-Soak databases under `state/backups/`. Live Shadow keeps its
own SQLite state and, when `SCALPING_BACKUP_ENCRYPTION_KEY` is configured, publishes a shutdown
backup under `state/backups/live-shadow/`. Auxiliary databases have their own migration history and
must be restored together with the primary state when investigating an operational incident.

The collector publishes immutable health heartbeats under `health/collector/`; the Live Shadow
database records the last event and receipt timestamps. Kubernetes readiness and liveness probes
consume these health records, so a stale stream, storage failure, or broken shadow continuity is
visible as an unhealthy process rather than silently appearing operational.

## Rollback drill

1. Keep the current pod stopped and retain its PVC. Never run two executors.
2. Restore the pre-deployment encrypted backup in an isolated directory and run SQLite
   `integrity_check`.
3. Confirm the rollback image supports the restored schema. Cross-schema rollback is prohibited
   unless a tested backward migration exists.
4. Change both the Git revision and image commit back to the compatible release.
5. Run the deployment gate, start one pod, and require startup reconciliation with Binance before
   entries are enabled.
6. If reconciliation diverges, keep the executor in Degraded Mode and do not retry the rollout.
