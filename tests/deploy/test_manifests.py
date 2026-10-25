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
    secret_env = {
        item["name"]: item
        for item in container["env"]
        if item["name"]
        in {
            "SCALPING_BINANCE_API_KEY",
            "SCALPING_BINANCE_API_SECRET",
            "SCALPING_TELEGRAM_BOT_TOKEN",
            "SCALPING_TELEGRAM_CHAT_ID",
            "SCALPING_BACKUP_ENCRYPTION_KEY",
            "SCALPING_CALIBRATED_HALF_SPREAD_RATE",
            "SCALPING_CALIBRATED_SLIPPAGE_RATE",
        }
    }
    assert all("value" not in item for item in secret_env.values())
    assert all("secretKeyRef" in item["valueFrom"] for item in secret_env.values())
    candidate_env = next(
        item for item in container["env"] if item["name"] == "SCALPING_CANDIDATE_MANIFEST"
    )
    cost_env = next(
        item for item in container["env"] if item["name"] == "SCALPING_COST_MODEL_SNAPSHOT"
    )
    assert candidate_env["value"] == "/run/scalping-candidate/manifest.json"
    assert cost_env["value"] == "/run/scalping-cost/snapshot.json"
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


def test_executor_and_gate_use_the_same_immutable_image_reference() -> None:
    executor = next(
        item for item in _documents("deploy/k8s/executor.yaml") if item["kind"] == "StatefulSet"
    )
    gate = _documents("deploy/k8s/deployment-gate.yaml")[0]
    image = executor["spec"]["template"]["spec"]["containers"][0]["image"]
    gate_image = gate["spec"]["template"]["spec"]["containers"][0]["image"]
    revision = _documents("deploy/argocd/application.yaml")[0]["spec"]["source"]["targetRevision"]
    image_revision = image.removeprefix("ghcr.io/raimangsxr/scalping-bot:git-")

    assert image == gate_image
    assert len(image_revision) == 40
    int(image_revision, 16)
    assert executor["metadata"]["annotations"]["scalping-bot.io/sbom"].endswith(
        f"{image_revision}.spdx.json"
    )
    assert len(revision) == 40
    int(revision, 16)


def test_collector_and_live_shadow_publish_distinct_metrics_ports() -> None:
    collector = _documents("deploy/k8s/collector.yaml")
    shadow = _documents("deploy/k8s/live-shadow.yaml")
    collector_service = next(item for item in collector if item["kind"] == "Service")
    collector_deployment = next(item for item in collector if item["kind"] == "Deployment")
    shadow_service = next(item for item in shadow if item["kind"] == "Service")
    shadow_deployment = next(item for item in shadow if item["kind"] == "Deployment")

    assert collector_service["spec"]["ports"][0]["port"] == 8001
    assert collector_deployment["spec"]["template"]["spec"]["containers"][0]["ports"][0][
        "containerPort"
    ] == 8001
    assert shadow_service["spec"]["ports"][0]["port"] == 8002
    assert shadow_deployment["spec"]["template"]["spec"]["containers"][0]["ports"][0][
        "containerPort"
    ] == 8002
