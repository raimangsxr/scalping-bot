"""Static verification of the immutable multi-process GitOps release."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]


@dataclass(frozen=True)
class ProcessRelease:
    image: str
    secrets: tuple[str, ...]
    can_submit_orders: bool
    single_writer: bool


@dataclass(frozen=True)
class ReleaseVerification:
    image_revision: str
    target_revision: str
    processes: dict[str, ProcessRelease]


class ReleaseVerificationError(ValueError):
    pass


class ReleaseVerifier:
    def __init__(self, documents: tuple[dict[str, Any], ...], application: dict[str, Any]) -> None:
        self.documents = documents
        self.application = application

    @classmethod
    def from_paths(cls, manifests: Path, application: Path) -> ReleaseVerifier:
        documents: list[dict[str, Any]] = []
        for path in sorted(manifests.glob("*.yaml")):
            documents.extend(
                item
                for item in yaml.safe_load_all(path.read_text(encoding="utf-8"))
                if isinstance(item, dict)
            )
        app_documents = [
            item
            for item in yaml.safe_load_all(application.read_text(encoding="utf-8"))
            if isinstance(item, dict)
        ]
        if len(app_documents) != 1:
            raise ReleaseVerificationError("GitOps application manifest must contain one object")
        return cls(tuple(documents), app_documents[0])

    def verify(self) -> ReleaseVerification:
        release_documents = [
            document
            for document in self.documents
            if document.get("kind") in {"StatefulSet", "Deployment", "Job"}
        ]
        workloads: dict[str, dict[str, Any]] = {}
        for document in self.documents:
            if document.get("kind") not in {"StatefulSet", "Deployment"}:
                continue
            component = _component(document)
            if component is not None:
                workloads[component] = document
        required = {"collector", "live-shadow", "executor"}
        if set(workloads) != required:
            raise ReleaseVerificationError(
                "release must define collector, live-shadow, and executor"
            )
        images = {
            _container(document)["image"]
            for document in release_documents
            if isinstance(_container(document).get("image"), str)
        }
        if len(images) != 1:
            raise ReleaseVerificationError("all process images must be identical")
        image = images.pop()
        match = re.fullmatch(r".+:git-([0-9a-fA-F]{40})", image)
        if match is None:
            raise ReleaseVerificationError("release image must be pinned to a full commit")
        image_revision = match.group(1).lower()
        for document in release_documents:
            workload_annotations = document.get("metadata", {}).get("annotations", {})
            template_annotations = (
                document.get("spec", {})
                .get("template", {})
                .get("metadata", {})
                .get("annotations", {})
            )
            annotations = {
                **(workload_annotations if isinstance(workload_annotations, dict) else {}),
                **(template_annotations if isinstance(template_annotations, dict) else {}),
            }
            sbom = (
                annotations.get("scalping-bot.io/sbom") if isinstance(annotations, dict) else None
            )
            if not isinstance(sbom, str) or not sbom.endswith(f":git-{image_revision}.spdx.json"):
                raise ReleaseVerificationError("SBOM must be pinned to the release image commit")
        target_revision = str(
            self.application.get("spec", {}).get("source", {}).get("targetRevision", "")
        )
        if not re.fullmatch(r"[0-9a-fA-F]{40}", target_revision):
            raise ReleaseVerificationError("ArgoCD release-manifest revision must be a full commit")
        application_annotations = self.application.get("metadata", {}).get("annotations", {})
        declared_image_revision = (
            application_annotations.get("scalping-bot.io/release-image-commit")
            if isinstance(application_annotations, dict)
            else None
        )
        if (
            declared_image_revision is not None
            and str(declared_image_revision).lower() != image_revision
        ):
            raise ReleaseVerificationError(
                "ArgoCD release image annotation and image commit must match"
            )
        processes: dict[str, ProcessRelease] = {}
        for component, document in workloads.items():
            container = _container(document)
            env = container.get("env", [])
            env_secret_names = tuple(
                str(item["name"]) for item in env if isinstance(item, dict) and "valueFrom" in item
            )
            volume_secret_names = tuple(
                str(volume.get("secret", {}).get("secretName"))
                for volume in document.get("spec", {})
                .get("template", {})
                .get("spec", {})
                .get("volumes", [])
                if isinstance(volume, dict)
                and isinstance(volume.get("secret"), dict)
                and volume.get("secret", {}).get("secretName") is not None
            )
            args = tuple(str(item) for item in container.get("args", []))
            processes[component] = ProcessRelease(
                image=image,
                secrets=tuple(dict.fromkeys((*env_secret_names, *volume_secret_names))),
                can_submit_orders=component == "executor" or "run-testnet-executor" in args,
                single_writer=(
                    component == "executor"
                    and document.get("kind") == "StatefulSet"
                    and document.get("spec", {}).get("replicas") == 1
                    and _has_read_write_once_claim(document)
                ),
            )
        if processes["collector"].secrets:
            raise ReleaseVerificationError("collector must not receive credentials")
        shadow_secrets = set(processes["live-shadow"].secrets)
        if not shadow_secrets <= {
            "scalping-bot-candidate-manifest",
            "scalping-bot-evidence",
        }:
            raise ReleaseVerificationError("Live Shadow must not receive executor credentials")
        if processes["live-shadow"].can_submit_orders:
            raise ReleaseVerificationError("Live Shadow must not have an order entry point")
        if not processes["executor"].single_writer:
            raise ReleaseVerificationError("executor must be the single SQLite writer")
        return ReleaseVerification(image_revision, target_revision, processes)


def _component(document: dict[str, Any]) -> str | None:
    labels = document.get("spec", {}).get("template", {}).get("metadata", {}).get("labels", {})
    if isinstance(labels, dict) and isinstance(labels.get("app.kubernetes.io/component"), str):
        return str(labels["app.kubernetes.io/component"])
    return None


def _container(document: dict[str, Any]) -> dict[str, Any]:
    containers = document.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    if (
        not isinstance(containers, list)
        or len(containers) != 1
        or not isinstance(containers[0], dict)
    ):
        raise ReleaseVerificationError("each process workload must have one container")
    return containers[0]


def _has_read_write_once_claim(document: dict[str, Any]) -> bool:
    claims = document.get("spec", {}).get("volumeClaimTemplates", [])
    return any(
        isinstance(claim, dict) and claim.get("spec", {}).get("accessModes") == ["ReadWriteOnce"]
        for claim in claims
    )
