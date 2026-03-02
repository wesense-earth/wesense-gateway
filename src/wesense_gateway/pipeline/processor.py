"""Reading processor — geocode, dedup, generate ID, write to ClickHouse."""

import logging
from datetime import datetime, timezone

from wesense_ingester.cache.dedup import DeduplicationCache
from wesense_ingester.geocoding.geocoder import ReverseGeocoder
from wesense_ingester.ids.reading_id import generate_reading_id

from wesense_gateway.models.reading import ProcessResult, ReadingBatch, ReadingIn
from wesense_gateway.storage.clickhouse import AsyncClickHouseWriter

logger = logging.getLogger(__name__)


class ReadingProcessor:
    """Orchestrates the per-reading pipeline: geocode -> dedup -> ID -> CH write."""

    def __init__(
        self,
        ch_writer: AsyncClickHouseWriter,
        geocoder: ReverseGeocoder,
        dedup_cache: DeduplicationCache,
    ):
        self._ch_writer = ch_writer
        self._geocoder = geocoder
        self._dedup = dedup_cache

    async def process_batch(self, batch: ReadingBatch) -> ProcessResult:
        """Process a batch of readings through the pipeline."""
        result = ProcessResult()

        for reading in batch.readings:
            try:
                if self._dedup.is_duplicate(
                    reading.device_id, reading.reading_type, reading.timestamp
                ):
                    result.duplicates += 1
                    continue

                row = self._build_row(reading)
                await self._ch_writer.add(row)
                result.accepted += 1

            except Exception as e:
                logger.error("Failed to process reading: %s", e)
                result.errors += 1

        return result

    def _build_row(self, reading: ReadingIn) -> tuple:
        """Build a ClickHouse row tuple from a reading."""
        geo_country = ""
        geo_subdivision = ""

        if reading.latitude is not None and reading.longitude is not None:
            geo = self._geocoder.reverse_geocode(reading.latitude, reading.longitude)
            if geo:
                geo_country = geo["geo_country"]
                geo_subdivision = geo["geo_subdivision"]

        reading_id = generate_reading_id(
            reading.device_id, reading.timestamp, reading.reading_type, reading.value
        )
        _ = reading_id  # Used for dedup/IPFS; not stored in ClickHouse 25-col schema

        ts = datetime.fromtimestamp(reading.timestamp, tz=timezone.utc)

        return (
            ts,
            reading.device_id,
            reading.data_source,
            reading.network_source,
            reading.ingestion_node_id,
            reading.reading_type,
            reading.value,
            reading.unit,
            reading.latitude,
            reading.longitude,
            reading.altitude,
            geo_country,
            geo_subdivision,
            reading.board_model,
            reading.sensor_model,
            reading.deployment_type,
            reading.deployment_type_source,
            reading.transport_type,
            reading.deployment_location,
            reading.node_name,
            reading.node_info,
            reading.node_info_url,
            reading.signature,
            reading.ingester_id,
            reading.key_version,
        )

    def get_stats(self) -> dict:
        """Aggregate stats from all sub-components."""
        return {
            "clickhouse": self._ch_writer.get_stats(),
            "dedup": self._dedup.get_stats(),
            "geocoder": self._geocoder.cache_info(),
        }
