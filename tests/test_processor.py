"""Tests for the reading processor pipeline."""

import pytest
from datetime import datetime, timezone

from wesense_gateway.models.reading import ReadingBatch, ReadingIn
from wesense_gateway.pipeline.processor import ReadingProcessor


@pytest.fixture
def processor(mock_ch_writer, mock_geocoder, mock_dedup):
    return ReadingProcessor(mock_ch_writer, mock_geocoder, mock_dedup)


async def test_process_single_reading(processor, sample_batch, mock_ch_writer):
    """Single reading should be accepted and written."""
    result = await processor.process_batch(sample_batch)

    assert result.accepted == 1
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
    """Multiple readings should all be processed."""
    readings = [
        ReadingIn(
            timestamp=1709337600 + i,
            device_id="dev-001",
            data_source="WESENSE",
            reading_type="temperature",
            value=20.0 + i,
        )
        for i in range(5)
    ]
    batch = ReadingBatch(readings=readings)

    result = await processor.process_batch(batch)

    assert result.accepted == 5
    assert mock_ch_writer.add.call_count == 5


async def test_row_has_geocoded_fields(processor, sample_batch, mock_ch_writer, mock_geocoder):
    """Row tuple should contain geocoded country/subdivision."""
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


async def test_process_reading_no_coords(processor, mock_ch_writer, mock_geocoder):
    """Reading without coordinates should still be accepted (empty geo fields)."""
    reading = ReadingIn(
        timestamp=1709337600,
        device_id="dev-001",
        data_source="WESENSE",
        reading_type="temperature",
        value=22.5,
    )
    batch = ReadingBatch(readings=[reading])

    result = await processor.process_batch(batch)

    assert result.accepted == 1
    mock_geocoder.reverse_geocode.assert_not_called()

    row = mock_ch_writer.add.call_args[0][0]
    assert row[11] == ""  # geo_country
    assert row[12] == ""  # geo_subdivision


async def test_get_stats(processor):
    """Stats should aggregate all sub-component stats."""
    stats = processor.get_stats()
    assert "clickhouse" in stats
    assert "dedup" in stats
    assert "geocoder" in stats
