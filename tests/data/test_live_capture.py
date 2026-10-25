from __future__ import annotations

import asyncio
import gzip
import json
from decimal import Decimal
from pathlib import Path

import pytest

from scalping_bot.data.book import BookGap, LocalOrderBook
from scalping_bot.data.capture import CaptureHealth, CaptureLedger, RawSegmentWriter
from scalping_bot.data.live import BinancePublicCollector
from scalping_bot.data.storage import LocalObjectStore


def test_local_book_follows_snapshot_and_sequence_rules() -> None:
    book = LocalOrderBook("BTCUSDT")
    book.apply_snapshot(
        last_update_id=100,
        bids=(("90000", "1.5"), ("89990", "2")),
        asks=(("90010", "1"), ("90020", "3")),
        exchange_time_ms=1_000,
        receipt_time_ms=1_010,
    )

    book.apply_depth(
        first_update_id=99,
        final_update_id=101,
        bids=(("90000", "0"), ("90005", "2.1")),
        asks=(("90010", "0.5"),),
        exchange_time_ms=1_020,
        receipt_time_ms=1_025,
    )

    assert book.valid
    assert book.best_bid == (Decimal("90005"), Decimal("2.1"))
    assert book.best_ask == (Decimal("90010"), Decimal("0.5"))
    assert book.latency_ms == 5


def test_depth_gap_invalidates_segment_and_requires_new_snapshot() -> None:
    book = LocalOrderBook("BTCUSDT")
    book.apply_snapshot(
        last_update_id=100,
        bids=(("90000", "1"),),
        asks=(("90010", "1"),),
        exchange_time_ms=1_000,
        receipt_time_ms=1_000,
    )

    with pytest.raises(BookGap, match="snapshot"):
        book.apply_depth(
            first_update_id=102,
            final_update_id=103,
            bids=(),
            asks=(),
            exchange_time_ms=1_100,
            receipt_time_ms=1_101,
        )

    assert not book.valid
    with pytest.raises(BookGap, match="snapshot"):
        _ = book.best_bid


def test_raw_events_are_compressed_checksummed_and_immutable(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    writer = RawSegmentWriter(store, stream="aggTrade", segment_started_ms=1_000)
    writer.append(
        {
            "event_type": "aggTrade",
            "exchange_time_ms": 1_001,
            "receipt_time_ms": 1_004,
            "price": "90000",
            "quantity": "0.01",
            "buyer_maker": False,
        }
    )

    segment = writer.close(valid=True)
    repeated = writer.close(valid=True)

    assert segment == repeated
    compressed = store.read(segment.object_key)
    assert segment.sha256 == __import__("hashlib").sha256(compressed).hexdigest()
    events = [json.loads(line) for line in gzip.decompress(compressed).splitlines()]
    assert events[0]["receipt_time_ms"] == 1_004
    assert json.loads(store.read(segment.manifest_key))["valid"] is True


def test_capture_health_reports_gaps_drift_storage_and_reconnects() -> None:
    health = CaptureHealth()
    health.record_event(exchange_time_ms=1_000, receipt_time_ms=1_025)
    health.record_gap()
    health.record_reconnect()
    health.record_storage_failure()

    assert health.snapshot(now_ms=1_100) == {
        "freshness_ms": 75,
        "clock_drift_ms": 25,
        "gaps": 1,
        "reconnects": 1,
        "storage_failures": 1,
    }


def test_capture_ledger_keeps_completed_segments_and_exposes_only_valid_book_evidence(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    ledger = CaptureLedger(store)
    writer = RawSegmentWriter(store, stream="btcusdt-depth-100ms", segment_started_ms=1_000)
    writer.append(
        {
            "event_type": "depth",
            "exchange_time_ms": 1_001,
            "receipt_time_ms": 1_002,
            "payload": {"U": 101, "u": 101},
        }
    )
    segment = writer.close(valid=True)
    ledger.record(segment, health={"freshness_ms": 0, "gaps": 0})

    catalog = ledger.catalog("btcusdt-depth-100ms")

    assert catalog.completed == (segment,)
    assert catalog.valid_book_segments == (segment,)
    assert catalog.invalid_segments == ()
    assert "health" in json.loads(store.read(catalog.entries[0].catalog_key))


def test_invalid_stream_payload_isolated_without_poisoning_other_streams(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    collector = BinancePublicCollector(store)

    asyncio.run(
        collector.handle_message(
            {
                "stream": "btcusdt@aggTrade",
                "data": {"e": "aggTrade", "E": 1, "a": 1, "p": "bad", "q": "1", "m": False},
            },
            receipt_time_ms=2,
        )
    )
    asyncio.run(
        collector.handle_message(
            {
                "stream": "btcusdt@bookTicker",
                "data": {"e": "bookTicker", "E": 3, "u": 1, "b": "99", "a": "100"},
            },
            receipt_time_ms=4,
        )
    )
    collector._rotate("btcusdt@bookTicker", valid=True, invalid_reason=None)

    trade_catalog = collector.ledger.catalog("btcusdt-aggTrade")
    quote_catalog = collector.ledger.catalog("btcusdt-bookTicker")
    assert len(trade_catalog.invalid_segments) == 1
    assert len(quote_catalog.completed) == 1
    assert quote_catalog.completed[0].valid is True
