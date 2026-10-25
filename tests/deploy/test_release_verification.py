from __future__ import annotations

from pathlib import Path

from scalping_bot.operations.release import ReleaseVerifier

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
