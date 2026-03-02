"""Shared test fixtures."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from wesense_gateway.config import GatewayConfig
from wesense_gateway.models.reading import ReadingBatch, ReadingIn


@pytest.fixture
def config():
    """Test gateway config."""
    return GatewayConfig(
        clickhouse_host="localhost",
        clickhouse_port=8123,
        clickhouse_user="test",
        clickhouse_password="test",
        clickhouse_database="test",
        archive_data_dir="/tmp/wesense-test-archives",
        trust_file="/tmp/wesense-test-trust.json",
        key_dir="/tmp/wesense-test-keys",
    )


@pytest.fixture
def sample_reading():
    """A single sample reading."""
    return ReadingIn(
        timestamp=1709337600,
        device_id="test-device-001",
        data_source="WESENSE",
        network_source="WIFI",
        ingestion_node_id="test-node",
        reading_type="temperature",
        value=22.5,
        unit="°C",
        latitude=-41.286,
        longitude=174.776,
        altitude=30.0,
        board_model="ESP32-S3",
        sensor_model="BME280",
        transport_type="WIFI",
        signature="abcd1234",
        ingester_id="wsi_test1234",
        key_version=1,
    )


@pytest.fixture
def sample_batch(sample_reading):
    """A batch with one reading."""
    return ReadingBatch(readings=[sample_reading])


@pytest.fixture
def mock_geocoder():
    """Mock ReverseGeocoder that returns NZ Wellington."""
    geocoder = MagicMock()
    geocoder.reverse_geocode.return_value = {
        "city": "Wellington",
        "admin1": "Wellington",
        "country_code": "NZ",
        "geo_country": "nz",
        "geo_subdivision": "wgn",
    }
    geocoder.cache_info.return_value = {
        "hits": 0, "misses": 0, "size": 0, "maxsize": 4096,
    }
    return geocoder


@pytest.fixture
def mock_dedup():
    """Mock DeduplicationCache that never flags duplicates."""
    dedup = MagicMock()
    dedup.is_duplicate.return_value = False
    dedup.get_stats.return_value = {
        "cache_size": 0, "duplicates_blocked": 0, "unique_processed": 0,
    }
    return dedup


@pytest.fixture
def mock_ch_writer():
    """Mock AsyncClickHouseWriter."""
    writer = MagicMock()
    writer.add = AsyncMock()
    writer.flush = AsyncMock()
    writer.close = AsyncMock()
    writer.get_stats.return_value = {
        "buffer_size": 0, "total_written": 0, "total_failed": 0,
    }
    return writer
