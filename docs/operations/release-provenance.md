# Release provenance

The GitOps release has two immutable links: the workload image is pinned to a
source commit and ArgoCD is pinned to the manifest commit. The release SBOM is
verified separately against its SHA-256 and the published image digest.

After obtaining the release metadata and SBOM from the release workflow, run:

```text
scalping-bot verify-release \
  --provenance release-info/provenance.json \
  --sbom release-info/scalping-bot-<commit>.spdx.json
```

The command fails closed if the source commit, Argo revision, image digest, or
SBOM checksum differs. It does not print credentials or inspect mutable image
tags as evidence.
