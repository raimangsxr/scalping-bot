from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import pytest

from scalping_bot.data.acquisition import (
    HISTORICAL_STRESS,
    PRIMARY_RESEARCH,
    RESEARCH_PERIODS,
    ArchiveIncompleteError,
    ArchiveKind,
    ArchiveRequest,
    ResearchArchiveCatalog,
    ResearchArchiveMaterializer,
    research_archive_requests,
    select_archive_kinds,
    select_research_periods,
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


def test_archive_selection_is_canonical_and_supports_resumable_catalogs(tmp_path: Path) -> None:
    selected = select_research_periods(("primary-research", "historical-stress"))
    assert selected == (HISTORICAL_STRESS, PRIMARY_RESEARCH)
    assert select_research_periods() == RESEARCH_PERIODS
    assert select_archive_kinds(("aggTrades",)) == (ArchiveKind.AGG_TRADES,)
    requests = research_archive_requests(selected, kinds=select_archive_kinds(("klines",)))
    assert requests[0].period == "historical-stress"
    assert requests[-1].month == "2025-09"
    assert all(item.kind is ArchiveKind.KLINES for item in requests)

    with pytest.raises(ValueError, match="unknown archive period"):
        select_research_periods(("not-a-period",))
    with pytest.raises(ValueError, match="unknown archive kind"):
        select_archive_kinds(("depth",))

    store = LocalObjectStore(tmp_path / "objects")
    catalog = ResearchArchiveCatalog().persist(store)
    assert ResearchArchiveCatalog.load(store, catalog.catalog_key or "") == catalog
