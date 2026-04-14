"""Reading processor — dedup, validate geo, generate ID, write to ClickHouse."""

import logging
from datetime import datetime, timezone

from wesense_ingester.cache.dedup import DeduplicationCache
from wesense_ingester.ids.reading_id import generate_reading_id
from wesense_ingester.pipeline import CURRENT_CANONICAL_VERSION

from wesense_gateway.models.reading import ProcessResult, ReadingBatch, ReadingIn
from wesense_gateway.storage.clickhouse import AsyncClickHouseWriter

logger = logging.getLogger(__name__)

# Track first warning per newer version seen (rate-limit noisy logs).
_seen_newer_versions: set[int] = set()


class ReadingProcessor:
    """Orchestrates the per-reading pipeline: validate geo -> dedup -> ID -> CH write."""

    def __init__(
        self,
        ch_writer: AsyncClickHouseWriter,
        dedup_cache: DeduplicationCache,
    ):
        self._ch_writer = ch_writer
        self._dedup = dedup_cache

    async def process_batch(self, batch: ReadingBatch) -> ProcessResult:
        """Process a batch of readings through the pipeline."""
        result = ProcessResult()

        for reading in batch.readings:
            try:
                # Forward rejection — refuse readings with a signing payload
                # version this broker doesn't understand. Dropping the reading
                # entirely (rather than storing a partial interpretation) is
                # what guarantees byte-identical archives across stations that
                # accept the reading. See data-integrity.md §"How Version Skew
                # Is Handled: Forward Rejection".
                if reading.signing_payload_version > CURRENT_CANONICAL_VERSION:
                    result.rejected += 1
                    if reading.signing_payload_version not in _seen_newer_versions:
                        _seen_newer_versions.add(reading.signing_payload_version)
                        logger.warning(
                            "REJECTING readings with signing_payload_version=%d from "
                            "ingester %s — storage broker only supports up to v%d. "
                            "Upgrade the storage broker to accept and archive newer data.",
                            reading.signing_payload_version,
                            reading.ingester_id or "?",
                            CURRENT_CANONICAL_VERSION,
                        )
                    continue

                if not reading.geo_country or not reading.geo_subdivision:
                    result.rejected += 1
                    continue

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
        reading_id = generate_reading_id(
            reading.device_id, reading.timestamp, reading.reading_type, reading.value
        )
        _ = reading_id  # Used for dedup/IPFS; not stored in ClickHouse 26-col schema

        ts = datetime.fromtimestamp(reading.timestamp, tz=timezone.utc)

        return (
            ts,
            reading.device_id,
            reading.data_source,
            reading.data_source_name,
            reading.network_source,
            reading.ingestion_node_id,
            reading.reading_type,
            reading.reading_type_name,
            reading.value,
            reading.unit,
            reading.latitude,
            reading.longitude,
            reading.altitude,
            reading.geo_country,
            reading.geo_subdivision,
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
            reading.data_license,
            reading.signing_payload_version,
        )

    def get_stats(self) -> dict:
        """Aggregate stats from all sub-components."""
        return {
            "clickhouse": self._ch_writer.get_stats(),
            "dedup": self._dedup.get_stats(),
        }
