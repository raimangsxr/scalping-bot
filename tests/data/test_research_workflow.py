from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import pytest

from scalping_bot.data.acquisition import (
    ArchiveIncompleteError,
    ArchiveKind,
    ArchiveRequest,
    ResearchArchiveMaterializer,
)
from scalping_bot.data.storage import LocalObjectStore


def _archive(path: Path, name: str, content: str) -> tuple[bytes, str]:
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr(name, content)
    payload = path.read_bytes()
    return payload, hashlib.sha256(payload).hexdigest()


def test_archive_materializer_is_resumable_and_keeps_a_catalog(tmp_path: Path) -> None:
    payload, checksum = _archive(
        tmp_path / "agg.zip",
        "BTCUSDT-aggTrades-2026-01.csv",
        "1,90000.00,0.01,10,10,1767225600000,false,true\n",
    )
    request = ArchiveRequest(
        kind=ArchiveKind.AGG_TRADES,
        period="final-holdout",
        month="2026-01",
        source_uri="https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/2026-01.zip",
    )
    calls = 0

    def download(_request: ArchiveRequest) -> tuple[bytes, str]:
        nonlocal calls
        calls += 1
        return payload, checksum

    materializer = ResearchArchiveMaterializer(
        LocalObjectStore(tmp_path / "objects"), downloader=download
    )
    first = materializer.materialize((request,))
    second = materializer.materialize((request,), existing=first)

    assert calls == 1
    assert second.entries == first.entries
    assert second.missing == ()
    assert second.catalog_key
    assert LocalObjectStore(tmp_path / "objects").read(second.catalog_key)


def test_incomplete_catalog_cannot_be_finalized(tmp_path: Path) -> None:
    request = ArchiveRequest(
        kind=ArchiveKind.KLINES,
        period="primary-research",
        month="2020-01",
        source_uri="https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/2020-01.zip",
    )

    catalog = ResearchArchiveMaterializer(
        LocalObjectStore(tmp_path / "objects"),
        downloader=lambda _request: (_ for _ in ()).throw(RuntimeError("network unavailable")),
    ).materialize((request,), require_complete=False)

    assert catalog.missing == (request,)
    with pytest.raises(ArchiveIncompleteError, match="continuous"):
        catalog.require_complete()
