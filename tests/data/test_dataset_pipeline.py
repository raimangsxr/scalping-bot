from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import duckdb
import pytest

from scalping_bot.data.archive import ArchiveIntegrityError, BinanceArchivePipeline
from scalping_bot.data.storage import LocalObjectStore


def _archive(path: Path, rows: list[str]) -> tuple[Path, str]:
    csv_name = "BTCUSDT-1m-2026-01.csv"
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr(csv_name, "\n".join(rows) + "\n")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


VALID_ROWS = [
    "1767225600000,90000.00,90010.00,89990.00,90005.00,1.2,1767225659999,108003.00,10,0.7,63001.00,0",
    "1767225660000,90005.00,90020.00,90000.00,90015.00,1.1,1767225719999,99011.00,11,0.6,54007.00,0",
]


def test_verified_archive_produces_queryable_parquet_and_immutable_manifest(tmp_path: Path) -> None:
    archive, checksum = _archive(tmp_path / "month.zip", VALID_ROWS)
    pipeline = BinanceArchivePipeline(LocalObjectStore(tmp_path / "objects"))

    manifest = pipeline.ingest_file(
        archive,
        expected_sha256=checksum,
        source_uri="https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/file.zip",
    )
    repeated = pipeline.ingest_file(
        archive,
        expected_sha256=checksum,
        source_uri="https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/file.zip",
    )

    assert manifest == repeated
    assert manifest.row_count == 2
    manifest_file = tmp_path / "objects" / manifest.manifest_key
    assert json.loads(manifest_file.read_text())["source_sha256"] == checksum
    parquet = tmp_path / "objects" / manifest.parquet_keys[0]
    result = duckdb.sql(f"SELECT count(*), min(open_time_ms) FROM '{parquet}'").fetchone()
    assert result == (2, 1767225600000)


def test_checksum_mismatch_is_rejected_before_derivation(tmp_path: Path) -> None:
    archive, _ = _archive(tmp_path / "month.zip", VALID_ROWS)

    with pytest.raises(ArchiveIntegrityError, match="checksum"):
        BinanceArchivePipeline(LocalObjectStore(tmp_path / "objects")).ingest_file(
            archive, expected_sha256="0" * 64, source_uri="https://data.binance.vision/file.zip"
        )


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([VALID_ROWS[0], VALID_ROWS[0]], "duplicate"),
        ([VALID_ROWS[1], VALID_ROWS[0]], "chronological"),
        ([VALID_ROWS[0], VALID_ROWS[1].replace("1767225660000", "1767225720000", 1)], "gap"),
        ([VALID_ROWS[0].replace("1767225600000", "1767225600", 1)], "timestamp"),
    ],
)
def test_invalid_market_history_cannot_be_derived(
    tmp_path: Path, rows: list[str], message: str
) -> None:
    archive, checksum = _archive(tmp_path / "month.zip", rows)

    with pytest.raises(ArchiveIntegrityError, match=message):
        BinanceArchivePipeline(LocalObjectStore(tmp_path / "objects")).ingest_file(
            archive,
            expected_sha256=checksum,
            source_uri="https://data.binance.vision/file.zip",
        )
