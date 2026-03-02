"""Tests for the Parquet archive builder."""

import io
import json

import pytest
import pyarrow.parquet as pq

from wesense_gateway.archive.builder import ParquetArchiveBuilder, PARQUET_SCHEMA
from wesense_gateway.archive.manifest import (
    build_trust_snapshot,
    compute_readings_hash,
)


def test_parquet_schema_has_18_columns():
    """Parquet schema should have 18 columns matching the archiver."""
    assert len(PARQUET_SCHEMA) == 18
    names = [f.name for f in PARQUET_SCHEMA]
    assert names[0] == "reading_id"
    assert names[-1] == "signature"


def test_compute_readings_hash_deterministic():
    """Same reading IDs should always produce the same hash."""
    ids = ["abc123", "def456", "ghi789"]
    h1 = compute_readings_hash(ids)
    h2 = compute_readings_hash(ids)
    assert h1 == h2
    assert len(h1) == 64


def test_compute_readings_hash_order_independent():
    """Hash should be the same regardless of input order (sorted internally)."""
    h1 = compute_readings_hash(["c", "a", "b"])
    h2 = compute_readings_hash(["a", "b", "c"])
    assert h1 == h2


def test_export_parquet_produces_valid_file():
    """_export_parquet should produce a valid Parquet file."""
    from unittest.mock import MagicMock

    builder = ParquetArchiveBuilder.__new__(ParquetArchiveBuilder)

    readings = [
        {
            "reading_id": "abc123",
            "device_id": "dev-001",
            "timestamp": "2026-03-01T00:00:00+00:00",
            "reading_type": "temperature",
            "value": 22.5,
            "unit": "°C",
            "latitude": -41.286,
            "longitude": 174.776,
            "altitude": 30.0,
            "geo_country": "nz",
            "geo_subdivision": "wgn",
            "data_source": "WESENSE",
            "board_model": "ESP32-S3",
            "node_name": "test",
            "transport_type": "WIFI",
            "ingester_id": "wsi_test1234",
            "key_version": 1,
            "signature": "deadbeef",
        }
    ]

    parquet_bytes = builder._export_parquet(readings)

    # Should be valid Parquet
    table = pq.read_table(io.BytesIO(parquet_bytes))
    assert table.num_rows == 1
    assert table.num_columns == 18
    assert table.column("reading_id")[0].as_py() == "abc123"
    assert table.column("value")[0].as_py() == 22.5


def test_export_parquet_sorted_by_reading_id():
    """Parquet output should be sorted by reading_id."""
    builder = ParquetArchiveBuilder.__new__(ParquetArchiveBuilder)

    readings = [
        {"reading_id": "zzz", "device_id": "d", "value": 1.0, "key_version": 1},
        {"reading_id": "aaa", "device_id": "d", "value": 2.0, "key_version": 1},
        {"reading_id": "mmm", "device_id": "d", "value": 3.0, "key_version": 1},
    ]

    parquet_bytes = builder._export_parquet(readings)
    table = pq.read_table(io.BytesIO(parquet_bytes))

    ids = [r.as_py() for r in table.column("reading_id")]
    assert ids == ["aaa", "mmm", "zzz"]


def test_build_trust_snapshot():
    """Trust snapshot should include snapshot_time."""
    from unittest.mock import MagicMock
    store = MagicMock()
    store.export_snapshot.return_value = {"keys": {"wsi_test": {"1": {"status": "active"}}}}

    snapshot = build_trust_snapshot(store, {"wsi_test"})

    assert "snapshot_time" in snapshot
    assert "keys" in snapshot
    store.export_snapshot.assert_called_once_with(["wsi_test"])
