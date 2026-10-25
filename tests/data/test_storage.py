from __future__ import annotations

from pathlib import Path

import pytest

from scalping_bot.data.storage import LocalObjectStore, object_store


def test_object_store_defaults_to_local_and_rejects_mixed_s3_options(tmp_path: Path) -> None:
    store = object_store(local_root=tmp_path / "objects")
    assert isinstance(store, LocalObjectStore)
    store.put_immutable("evidence/item", b"immutable")
    assert store.read("evidence/item") == b"immutable"

    with pytest.raises(ValueError, match="require --bucket"):
        object_store(local_root=tmp_path, endpoint_url="http://localhost:9000")
