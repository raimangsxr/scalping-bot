# Executor deployment and rollback

ArgoCD deploys exactly one executor from the immutable release manifests. Those manifests reference
an image tagged with the verified source commit, and the image must have a matching SPDX SBOM. The
release-manifest revision and image source commit are advanced together only after CI passes.

Before sync, the `PreSync` deployment gate reads SQLite without becoming a writer and requires:

1. no local order intent or protected exposure remains open;
2. the latest exchange reconciliation is successful and has zero divergences;
3. a non-empty encrypted backup exists at `state/backups/latest.sqlite.fernet`; and
4. the target and current SQLite schema versions are identical.

If any prerequisite fails, the ArgoCD sync fails without replacing the executor.

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
