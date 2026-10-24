from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]


def _documents(path: str) -> list[dict[str, object]]:
    return [item for item in yaml.safe_load_all((ROOT / path).read_text()) if item]


def test_executor_is_a_single_writer_with_exclusive_sqlite_volume() -> None:
    documents = _documents("deploy/k8s/executor.yaml")
    stateful_set = next(item for item in documents if item["kind"] == "StatefulSet")
    spec = stateful_set["spec"]
    assert spec["replicas"] == 1
    assert spec["updateStrategy"]["type"] == "OnDelete"
    claim = spec["volumeClaimTemplates"][0]
    assert claim["spec"]["accessModes"] == ["ReadWriteOnce"]


def test_secrets_are_references_and_image_is_commit_pinned() -> None:
    documents = _documents("deploy/k8s/executor.yaml")
    stateful_set = next(item for item in documents if item["kind"] == "StatefulSet")
    container = stateful_set["spec"]["template"]["spec"]["containers"][0]

    assert container["image"].startswith("ghcr.io/raimangsxr/scalping-bot:git-")
    assert ":latest" not in container["image"]
    assert all("value" not in item for item in container["env"])
    assert all("secretKeyRef" in item["valueFrom"] for item in container["env"])
    assert "secret-key" not in (ROOT / "deploy/k8s/executor.yaml").read_text().lower()


def test_argocd_runs_flat_deployment_gate_before_sync() -> None:
    gate = _documents("deploy/k8s/deployment-gate.yaml")[0]
    annotations = gate["metadata"]["annotations"]

    assert annotations["argocd.argoproj.io/hook"] == "PreSync"
    assert gate["spec"]["template"]["spec"]["restartPolicy"] == "Never"
    args = gate["spec"]["template"]["spec"]["containers"][0]["args"]
    assert args == ["deployment-check"]


def test_argocd_application_uses_an_immutable_commit_revision() -> None:
    application = _documents("deploy/argocd/application.yaml")[0]
    revision = application["spec"]["source"]["targetRevision"]

    assert len(revision) == 40
    int(revision, 16)
