from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from scalping_bot.operations.release import (
    ReleaseProvenance,
    ReleaseVerificationError,
    ReleaseVerifier,
)

ROOT = Path(__file__).parents[2]


def test_gitops_release_has_one_immutable_image_and_three_process_identities() -> None:
    result = ReleaseVerifier.from_paths(
        ROOT / "deploy/k8s", ROOT / "deploy/argocd/application.yaml"
    ).verify()

    assert result.image_revision
    assert set(result.processes) == {"collector", "live-shadow", "executor"}
    assert result.processes["executor"].single_writer
    assert result.processes["collector"].secrets == ()
    assert result.processes["live-shadow"].can_submit_orders is False


def test_release_provenance_binds_source_manifest_image_digest_and_sbom() -> None:
    verification = ReleaseVerifier.from_paths(
        ROOT / "deploy/k8s", ROOT / "deploy/argocd/application.yaml"
    ).verify()
    sbom = b"immutable sbom"
    digest = "sha256:" + "a" * 64
    provenance = ReleaseProvenance(
        source_commit=verification.image_revision,
        image=(
            "ghcr.io/raimangsxr/scalping-bot:git-"
            f"{verification.image_revision}@{digest}"
        ),
        image_digest=digest,
        manifest_commit=verification.target_revision,
        sbom_sha256=hashlib.sha256(sbom).hexdigest(),
    )
    provenance.verify(verification, sbom=sbom)

    with pytest.raises(ReleaseVerificationError, match="SBOM"):
        provenance.verify(verification, sbom=b"different")
