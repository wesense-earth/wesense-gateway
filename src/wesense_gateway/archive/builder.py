"""Parquet archive builder — exports readings to deterministic Parquet files."""

import asyncio
import hashlib
import io
import json
import logging
from datetime import datetime, timezone


import pyarrow as pa
import pyarrow.parquet as pq

from wesense_ingester.ids.reading_id import generate_reading_id
from wesense_ingester.signing.keys import IngesterKeyManager
from wesense_ingester.signing.trust import TrustStore

from wesense_gateway.archive.manifest import (
    build_manifest,
    build_trust_snapshot,
    compute_readings_hash,
)
from wesense_gateway.archive.verifier import verify_signatures
from wesense_gateway.backends.base import StorageBackend

logger = logging.getLogger(__name__)

# 18-column Parquet schema (matches wesense-archiver)
PARQUET_SCHEMA = pa.schema([
    ("reading_id", pa.string()),
    ("device_id", pa.string()),
    ("timestamp", pa.string()),
    ("reading_type", pa.string()),
    ("reading_type_name", pa.string()),
    ("value", pa.float64()),
    ("unit", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("altitude", pa.float64()),
    ("geo_country", pa.string()),
    ("geo_subdivision", pa.string()),
    ("data_source", pa.string()),
    ("data_license", pa.string()),
    ("board_model", pa.string()),
    ("node_name", pa.string()),
    ("transport_type", pa.string()),
    ("ingester_id", pa.string()),
    ("key_version", pa.uint32()),
    ("signing_payload_version", pa.uint32()),
    ("signature", pa.string()),
])


class ParquetArchiveBuilder:
    """
    Builds Parquet archives from ClickHouse readings.

    Subdivision-level partitioning:
        {country}/{subdivision}/{YYYY}/{MM}/{DD}/readings.parquet
    """

    def __init__(
        self,
        ch_client,
        trust_store: TrustStore,
        key_manager: IngesterKeyManager,
        backend: StorageBackend,
    ):
        self._ch = ch_client
        self._trust_store = trust_store
        self._key_manager = key_manager
        self._backend = backend

    async def archive_period(
        self, period: str, country: str, subdivision: str
    ) -> dict | None:
        """
        Archive a single country/subdivision/day.

        1. Query ClickHouse for signed readings
        2. Verify signatures
        3. Build trust snapshot
        4. Export deterministic Parquet
        5. Build and sign manifest
        6. Store all files via backend

        Returns the manifest dict, or None if no readings.
        """
        logger.info("Archiving %s/%s/%s", country, subdivision, period)

        loop = asyncio.get_event_loop()
        readings = await loop.run_in_executor(
            None, self._query_readings, period, country, subdivision
        )
        if not readings:
            logger.info("No signed readings for %s/%s/%s — skipping", country, subdivision, period)
            return None

        logger.info("Fetched %d signed readings for %s/%s/%s", len(readings), country, subdivision, period)

        # Verify signatures
        verified, failed = verify_signatures(readings, self._trust_store)
        logger.info(
            "Signature verification: %d verified, %d failed for %s/%s/%s",
            len(verified), failed, country, subdivision, period,
        )

        if not verified:
            logger.warning("No verified readings for %s/%s/%s — skipping", country, subdivision, period)
            return None

        # Trust snapshot
        ingester_ids = {r["ingester_id"] for r in verified if r.get("ingester_id")}
        trust_snapshot = build_trust_snapshot(self._trust_store, ingester_ids)

        # Parquet
        parquet_bytes = self._export_parquet(verified)

        # Readings hash
        reading_ids = sorted(r["reading_id"] for r in verified if r.get("reading_id"))
        readings_hash = compute_readings_hash(reading_ids)

        # Trust snapshot hash
        trust_snapshot_json = json.dumps(trust_snapshot, sort_keys=True, indent=2)
        trust_snapshot_hash = hashlib.sha256(trust_snapshot_json.encode()).hexdigest()

        # Manifest
        manifest = build_manifest(
            period=period,
            region=country,
            subdivision=subdivision,
            verified_count=len(verified),
            failed_count=failed,
            readings_hash=readings_hash,
            trust_snapshot_hash=trust_snapshot_hash,
            key_manager=self._key_manager,
        )

        # Store files via backend
        date_parts = period.split("-")
        base_path = f"{country}/{subdivision}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}"

        blake3_hash = await self._backend.store(f"{base_path}/readings.parquet", parquet_bytes)
        await self._backend.store(f"{base_path}/trust_snapshot.json", trust_snapshot_json.encode())

        manifest_json = json.dumps(manifest, indent=2)
        await self._backend.store(f"{base_path}/manifest.json", manifest_json.encode())

        logger.info(
            "Archived %s/%s/%s — %d readings, hash=%s...",
            country, subdivision, period, len(verified), readings_hash[:16],
        )

        return manifest

    def _query_readings(
        self, period: str, country: str, subdivision: str
    ) -> list[dict]:
        """Query ClickHouse for signed readings for a country/subdivision/day.

        All accepted readings are archived (both locally ingested and
        P2P-received). Stations running the same canonical version produce
        byte-identical archives; iroh gossip deduplicates. Older stations
        reject newer readings at ingestion (forward rejection), so they
        never produce divergent archives.
        """
        query = """
            SELECT
                device_id, timestamp, reading_type, reading_type_name, value, unit,
                latitude, longitude, altitude, geo_country, geo_subdivision,
                data_source, data_license, board_model, node_name, transport_type,
                ingester_id, key_version, signing_payload_version, signature
            FROM sensor_readings FINAL
            WHERE toDate(timestamp) = {period:String}
              AND geo_country = {country:String}
              AND geo_subdivision = {subdivision:String}
              AND signature != ''
            ORDER BY device_id, reading_type, timestamp
        """
        result = self._ch.query(
            query,
            parameters={
                "period": period,
                "country": country,
                "subdivision": subdivision,
            },
        )

        columns = [
            "device_id", "timestamp", "reading_type", "reading_type_name", "value",
            "unit", "latitude", "longitude", "altitude", "geo_country",
            "geo_subdivision", "data_source", "data_license", "board_model", "node_name",
            "transport_type", "ingester_id", "key_version", "signing_payload_version",
            "signature",
        ]

        readings = []
        for row in result.result_rows:
            reading = dict(zip(columns, row))

            # Preserve unix timestamp for signature verification and reading_id
            ts = reading["timestamp"]
            if hasattr(ts, "timestamp"):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                reading["_ts_unix"] = int(ts.timestamp())
                reading["timestamp"] = ts.isoformat()
            else:
                ts = datetime.fromisoformat(str(ts))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                reading["_ts_unix"] = int(ts.timestamp())

            reading["reading_id"] = generate_reading_id(
                reading["device_id"],
                reading["_ts_unix"],
                reading["reading_type"],
                reading["value"],
            )
            readings.append(reading)

        return readings

    def _export_parquet(self, readings: list[dict]) -> bytes:
        """Export readings to a deterministic Parquet file."""
        readings_sorted = sorted(readings, key=lambda r: r.get("reading_id", ""))

        columns = {}
        for field in PARQUET_SCHEMA:
            col_name = field.name
            if field.type == pa.float64():
                columns[col_name] = [float(r.get(col_name, 0) or 0) for r in readings_sorted]
            elif field.type == pa.uint32():
                columns[col_name] = [int(r.get(col_name, 0) or 0) for r in readings_sorted]
            else:
                columns[col_name] = [str(r.get(col_name, "") or "") for r in readings_sorted]

        table = pa.table(columns, schema=PARQUET_SCHEMA)

        buf = io.BytesIO()
        pq.write_table(
            table, buf,
            compression="zstd",
            use_dictionary=False,
            write_statistics=False,
        )
        return buf.getvalue()
