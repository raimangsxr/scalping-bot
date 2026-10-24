from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from scalping_bot.domain.engine import StrategyPolicy
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.runner import build_policy


@dataclass(frozen=True)
class PromotedCandidateManifest:
    candidate_identity: str
    parameters: dict[str, object]
    rules_sha256: str
    lifecycle: str

    @classmethod
    def load(cls, path: Path) -> PromotedCandidateManifest:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("candidate deployment manifest must be a JSON object")
        parameters = value.get("parameters")
        if not isinstance(parameters, dict):
            raise ValueError("candidate deployment manifest has no frozen parameters")
        manifest = cls(
            candidate_identity=str(value.get("candidate_identity")),
            parameters=parameters,
            rules_sha256=str(value.get("rules_sha256")),
            lifecycle=str(value.get("lifecycle")),
        )
        if manifest.lifecycle != "testnet-ready":
            raise ValueError("executor accepts only a Testnet Ready candidate manifest")
        candidate = next(
            (
                item
                for item in first_batch().candidates
                if item.identity == manifest.candidate_identity
            ),
            None,
        )
        if candidate is None:
            raise ValueError("candidate identity is outside the preregistered first batch")
        canonical = json.dumps(
            {
                "identity": candidate.identity,
                "original_rules": dict(candidate.original_rules),
                "modifications": dict(candidate.modifications),
                "parameters": parameters,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if hashlib.sha256(canonical).hexdigest() != manifest.rules_sha256:
            raise ValueError("candidate rules hash does not match frozen manifest content")
        return manifest

    def policy(self) -> StrategyPolicy:
        candidate = next(
            item for item in first_batch().candidates if item.identity == self.candidate_identity
        )
        return build_policy(candidate, self.parameters)
