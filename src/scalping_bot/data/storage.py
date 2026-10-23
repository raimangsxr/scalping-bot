from __future__ import annotations

from pathlib import Path


class ImmutableObjectError(RuntimeError):
    pass


class LocalObjectStore:
    """Filesystem implementation of immutable object storage."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, key: str) -> Path:
        candidate = (self.root / key).resolve()
        root = self.root.resolve()
        if not candidate.is_relative_to(root):
            raise ValueError("object key escapes storage root")
        return candidate

    def put_immutable(self, key: str, content: bytes) -> Path:
        target = self.path(key)
        if target.exists():
            if target.read_bytes() != content:
                raise ImmutableObjectError(f"immutable object differs: {key}")
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        return target

    def read(self, key: str) -> bytes:
        return self.path(key).read_bytes()
