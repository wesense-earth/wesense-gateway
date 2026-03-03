"""Tests for the reading processor pipeline."""

import pytest
from datetime import datetime, timezone

from wesense_gateway.models.reading import ReadingBatch, ReadingIn
from wesense_gateway.pipeline.processor import ReadingProcessor


@pytest.fixture
def processor(mock_ch_writer, mock_dedup):
    return ReadingProcessor(mock_ch_writer, mock_dedup)


async def test_process_single_reading(processor, sample_batch, mock_ch_writer):
    """Single reading with geo fields should be accepted and written."""
    result = await processor.process_batch(sample_batch)

    assert result.accepted == 1
    assert result.rejected == 0
    assert result.duplicates == 0
    assert result.errors == 0
    mock_ch_writer.add.assert_called_once()


async def test_process_duplicate_reading(processor, sample_batch, mock_dedup, mock_ch_writer):
    """Duplicate reading should be counted, not written."""
    mock_dedup.is_duplicate.return_value = True

    result = await processor.process_batch(sample_batch)

    assert result.accepted == 0
    assert result.duplicates == 1
    mock_ch_writer.add.assert_not_called()


async def test_process_batch_multiple(processor, mock_ch_writer, mock_dedup):
    """Multiple readings with geo fields should all be processed."""
    readings = [
        ReadingIn(
            timestamp=1709337600 + i,
            device_id="dev-001",
            data_source="WESENSE",
            reading_type="temperature",
            value=20.0 + i,
            geo_country="nz",
            geo_subdivision="auk",
        )
        for i in range(5)
    ]
    batch = ReadingBatch(readings=readings)

    result = await processor.process_batch(batch)

    assert result.accepted == 5
    assert mock_ch_writer.add.call_count == 5


async def test_row_has_geo_fields_from_reading(processor, sample_batch, mock_ch_writer):
    """Row tuple should contain geo_country/geo_subdivision from the reading."""
    await processor.process_batch(sample_batch)

    row = mock_ch_writer.add.call_args[0][0]
    # geo_country is at index 11, geo_subdivision at index 12
    assert row[11] == "nz"
    assert row[12] == "wgn"


async def test_row_timestamp_is_datetime(processor, sample_batch, mock_ch_writer):
    """Row timestamp should be a datetime object."""
    await processor.process_batch(sample_batch)

    row = mock_ch_writer.add.call_args[0][0]
    assert isinstance(row[0], datetime)
    assert row[0].tzinfo == timezone.utc


async def test_reject_reading_without_geo_country(processor, mock_ch_writer):
    """Reading without geo_country should be rejected."""
    reading = ReadingIn(
        timestamp=1709337600,
        device_id="dev-001",
        data_source="WESENSE",
        reading_type="temperature",
        value=22.5,
        geo_subdivision="auk",
    )
    batch = ReadingBatch(readings=[reading])

    result = await processor.process_batch(batch)

    assert result.accepted == 0
    assert result.rejected == 1
    mock_ch_writer.add.assert_not_called()


async def test_reject_reading_without_geo_subdivision(processor, mock_ch_writer):
    """Reading without geo_subdivision should be rejected."""
    reading = ReadingIn(
        timestamp=1709337600,
        device_id="dev-001",
        data_source="WESENSE",
        reading_type="temperature",
        value=22.5,
        geo_country="nz",
    )
    batch = ReadingBatch(readings=[reading])

    result = await processor.process_batch(batch)

    assert result.accepted == 0
    assert result.rejected == 1
    mock_ch_writer.add.assert_not_called()


async def test_reject_reading_without_any_geo(processor, mock_ch_writer):
    """Reading without any geo fields should be rejected."""
    reading = ReadingIn(
        timestamp=1709337600,
        device_id="dev-001",
        data_source="WESENSE",
        reading_type="temperature",
        value=22.5,
        latitude=-36.848,
        longitude=174.763,
    )
    batch = ReadingBatch(readings=[reading])

    result = await processor.process_batch(batch)

    assert result.accepted == 0
    assert result.rejected == 1
    mock_ch_writer.add.assert_not_called()


async def test_mixed_batch_accepted_and_rejected(processor, mock_ch_writer):
    """Batch with some geo and some without should partially accept."""
    readings = [
        ReadingIn(
            timestamp=1709337600,
            device_id="dev-001",
            data_source="WESENSE",
            reading_type="temperature",
            value=22.5,
            geo_country="nz",
            geo_subdivision="auk",
        ),
        ReadingIn(
            timestamp=1709337601,
            device_id="dev-002",
            data_source="WESENSE",
            reading_type="temperature",
            value=23.0,
            # No geo fields
        ),
    ]
    batch = ReadingBatch(readings=readings)

    result = await processor.process_batch(batch)

    assert result.accepted == 1
    assert result.rejected == 1
    mock_ch_writer.add.assert_called_once()


async def test_get_stats(processor):
    """Stats should aggregate all sub-component stats."""
    stats = processor.get_stats()
    assert "clickhouse" in stats
    assert "dedup" in stats
    assert "geocoder" not in stats
