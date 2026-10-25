from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from scalping_bot.data.storage import ObjectStore
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
    market_data_dependencies: tuple[str, ...] = ()
    execution_cost_model_identity: str = ""

    @classmethod
    def from_candidate(
        cls,
        candidate: CandidateRecord,
        *,
        parameters: dict[str, object],
        execution_cost_model_identity: str = "",
    ) -> PromotedCandidateManifest:
        if candidate.lifecycle is not CandidateLifecycle.TESTNET_READY:
            raise ValueError("only a Testnet Ready candidate can be deployed")
        if len(execution_cost_model_identity) != 64 or any(
            character not in "0123456789abcdef"
            for character in execution_cost_model_identity.lower()
        ):
            raise ValueError(
                "a candidate manifest requires a content-addressed execution cost model"
            )
        canonical = _canonical_rules(candidate, parameters)
        return cls(
            candidate_identity=candidate.identity,
            parameters=dict(parameters),
            rules_sha256=hashlib.sha256(canonical).hexdigest(),
            lifecycle=CandidateLifecycle.TESTNET_READY.value,
            market_data_dependencies=candidate.market_data_dependencies,
            execution_cost_model_identity=execution_cost_model_identity,
        )

    def persist(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = self.canonical_bytes()
        content = serialized.decode()
        if path.exists():
            if path.read_text(encoding="utf-8") != content:
                raise ValueError("candidate deployment manifest is immutable")
            return
        path.write_text(content, encoding="utf-8")

    def canonical_bytes(self) -> bytes:
        content = json.dumps(
            {
                "candidate_identity": self.candidate_identity,
                "parameters": self.parameters,
                "rules_sha256": self.rules_sha256,
                "lifecycle": self.lifecycle,
                "market_data_dependencies": list(self.market_data_dependencies),
                "execution_cost_model_identity": self.execution_cost_model_identity,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return (content + "\n").encode()

    def persist_store(self, store: ObjectStore) -> str:
        identity = hashlib.sha256(self.canonical_bytes()).hexdigest()
        key = f"candidate-manifests/{self.candidate_identity}/{identity}.json"
        store.put_immutable(key, self.canonical_bytes())
        return key

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
        dependencies = value.get("market_data_dependencies", [])
        execution_cost_model_identity = value.get("execution_cost_model_identity")
        if (
            not isinstance(candidate_identity, str)
            or not isinstance(rules_sha256, str)
            or len(rules_sha256) != 64
            or any(character not in "0123456789abcdef" for character in rules_sha256.lower())
            or not isinstance(lifecycle, str)
            or not isinstance(dependencies, list)
            or not isinstance(execution_cost_model_identity, str)
            or len(execution_cost_model_identity) != 64
            or any(
                character not in "0123456789abcdef"
                for character in execution_cost_model_identity.lower()
            )
        ):
            raise ValueError(
                "candidate deployment manifest has malformed identity, rule, or cost hash"
            )
        manifest = cls(
            candidate_identity=candidate_identity,
            parameters=parameters,
            rules_sha256=rules_sha256,
            lifecycle=lifecycle,
            market_data_dependencies=tuple(str(item) for item in dependencies),
            execution_cost_model_identity=execution_cost_model_identity.lower(),
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
