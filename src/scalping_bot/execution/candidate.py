from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from scalping_bot.domain.engine import StrategyPolicy
from scalping_bot.research.candidates import first_batch
from scalping_bot.research.evidence import CandidateLifecycle, CandidateRecord
from scalping_bot.research.runner import build_policy


@dataclass(frozen=True)
class PromotedCandidateManifest:
    candidate_identity: str
    parameters: dict[str, object]
    rules_sha256: str
    lifecycle: str

    @classmethod
    def from_candidate(
        cls, candidate: CandidateRecord, *, parameters: dict[str, object]
    ) -> PromotedCandidateManifest:
        if candidate.lifecycle is not CandidateLifecycle.TESTNET_READY:
            raise ValueError("only a Testnet Ready candidate can be deployed")
        canonical = _canonical_rules(candidate, parameters)
        return cls(
            candidate_identity=candidate.identity,
            parameters=dict(parameters),
            rules_sha256=hashlib.sha256(canonical).hexdigest(),
            lifecycle=CandidateLifecycle.TESTNET_READY.value,
        )

    def persist(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(
            {
                "candidate_identity": self.candidate_identity,
                "parameters": self.parameters,
                "rules_sha256": self.rules_sha256,
                "lifecycle": self.lifecycle,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        serialized = content + "\n"
        if path.exists():
            if path.read_text(encoding="utf-8") != serialized:
                raise ValueError("candidate deployment manifest is immutable")
            return
        path.write_text(serialized, encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> PromotedCandidateManifest:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("candidate deployment manifest must be a JSON object")
        parameters = value.get("parameters")
        if not isinstance(parameters, dict):
            raise ValueError("candidate deployment manifest has no frozen parameters")
        candidate_identity = value.get("candidate_identity")
        rules_sha256 = value.get("rules_sha256")
        lifecycle = value.get("lifecycle")
        if (
            not isinstance(candidate_identity, str)
            or not isinstance(rules_sha256, str)
            or len(rules_sha256) != 64
            or any(character not in "0123456789abcdef" for character in rules_sha256.lower())
            or not isinstance(lifecycle, str)
        ):
            raise ValueError("candidate deployment manifest has malformed identity or rule hash")
        manifest = cls(
            candidate_identity=candidate_identity,
            parameters=parameters,
            rules_sha256=rules_sha256,
            lifecycle=lifecycle,
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
        if set(parameters) != set(candidate.parameters):
            raise ValueError("candidate deployment parameters do not match the preregistered shape")
        for name, value_parameter in parameters.items():
            allowed = candidate.parameters[name]
            if isinstance(allowed, list) and value_parameter not in allowed:
                raise ValueError(f"candidate parameter {name} is outside the preregistered range")
        canonical = _canonical_rules(candidate, parameters)
        if hashlib.sha256(canonical).hexdigest() != manifest.rules_sha256:
            raise ValueError("candidate rules hash does not match frozen manifest content")
        return manifest

    def policy(self) -> StrategyPolicy:
        candidate = next(
            item for item in first_batch().candidates if item.identity == self.candidate_identity
        )
        return build_policy(candidate, self.parameters)


def _canonical_rules(candidate: CandidateRecord, parameters: dict[str, object]) -> bytes:
    return json.dumps(
        {
            "identity": candidate.identity,
            "original_rules": dict(candidate.original_rules),
            "modifications": dict(candidate.modifications),
            "parameters": parameters,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
