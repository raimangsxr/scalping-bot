# Runtime secret and storage boundary

Runtime secrets are provisioned only in the target environment. They must never
be committed, rendered into GitOps manifests, copied into an image, or printed
by diagnostics. `scalping-bot check-runtime-secrets --role <role>` checks only
presence and reports field names, never values.

| Process | Required secret inputs | Write boundary |
| --- | --- | --- |
| `collector` | none | `market-evidence` volume or the configured S3 prefix |
| `live-shadow` | `SCALPING_BACKUP_ENCRYPTION_KEY` | `shadow-state` and its backup prefix; no exchange credentials |
| `executor` | Binance Spot Testnet API key/secret, Telegram bot token/chat id, backup encryption key | the single SQLite volume and operational backup prefix |

The executor additionally requires the checksum-verified Testnet Ready candidate
manifest and Execution Cost Model snapshot. The collector uses public Binance
production streams only; the executor endpoints remain the official Spot
Testnet endpoints enforced by `Settings`.

Storage identities must be separate even when an S3-compatible service is used:
`market-evidence`, `shadow-state`, and `executor-state` receive distinct
prefixes/policies. Backups are encrypted before publication and the latest
pointer is atomically replaced. Use the deployment and backup drills before
admitting a workload to continuous operation.
