"""
Backfill missing geo data and signatures for historical readings.

Three modes:
  --mode geocode (default)
      Fills in geo_country and geo_subdivision for readings that have
      coordinates but missing geo fields. Uses offline GeoNames reverse
      geocoder (no network dependency).

  --mode sign
      Signs local readings where signature = ''.
      For data ingested before Ed25519 signing was deployed.

  --mode both
      Runs geocode first, then sign.

Usage:
    python backfill_geo_and_signatures.py --mode both \
        --ch-host 192.168.43.11 --ch-user default --ch-password 'v3r8^t1m77'

    # Or from inside the gateway container:
    python scripts/backfill_geo_and_signatures.py --mode both
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [backfill] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backfill")


def geocode_batch(client, batch_size=10000):
    """Re-geocode readings with coordinates but missing geo_country or geo_subdivision."""
    from wesense_ingester.geocoding.geocoder import ReverseGeocoder

    geocoder = ReverseGeocoder(cache_size=8192)

    # Count how many need fixing
    result = client.query("""
        SELECT
            countIf(geo_country = '' AND latitude != 0) as no_country,
            countIf(geo_country != '' AND geo_subdivision = '' AND latitude != 0) as no_subdiv
        FROM sensor_readings
        WHERE timestamp > '2020-01-01'
    """)
    no_country, no_subdiv = result.result_rows[0]
    total = no_country + no_subdiv
    logger.info("Readings to geocode: %d no-country + %d no-subdivision = %d total", no_country, no_subdiv, total)

    if total == 0:
        logger.info("Nothing to geocode")
        return

    # Process in batches using DISTINCT coordinates to minimize geocoder calls
    # Step 1: Fix missing countries (also fills subdivision)
    if no_country > 0:
        _geocode_missing_country(client, geocoder, batch_size)

    # Step 2: Fix missing subdivisions (country already set)
    if no_subdiv > 0:
        _geocode_missing_subdivision(client, geocoder, batch_size)

    logger.info("Geocoding complete. Cache stats: %s", geocoder.cache_info())


def _geocode_missing_country(client, geocoder, batch_size):
    """Fix readings with no geo_country but valid coordinates."""
    logger.info("--- Phase: geocode missing countries ---")

    # Get distinct coordinate bins (rounded to 3dp = ~100m)
    result = client.query("""
        SELECT DISTINCT
            round(latitude, 3) as lat,
            round(longitude, 3) as lon
        FROM sensor_readings
        WHERE geo_country = '' AND latitude != 0 AND longitude != 0
          AND timestamp > '2020-01-01'
    """)
    coords = result.result_rows
    logger.info("Distinct coordinate bins to geocode: %d", len(coords))

    # Build a mapping of rounded coords → (country, subdivision)
    updates = {}  # (lat, lon) → (country, subdiv)
    failed = 0
    for i, (lat, lon) in enumerate(coords):
        geo = geocoder.reverse_geocode(float(lat), float(lon))
        if geo and geo.get("geo_country") and geo["geo_country"] != "unknown":
            updates[(float(lat), float(lon))] = (geo["geo_country"], geo.get("geo_subdivision", ""))
        else:
            failed += 1

        if (i + 1) % 1000 == 0:
            logger.info("  Geocoded %d/%d coordinate bins (%d failed)", i + 1, len(coords), failed)

    logger.info("Geocoded %d coordinate bins, %d failed", len(updates), failed)

    # Apply updates in batches grouped by result
    # Group coords by their geocode result to minimize ALTER statements
    result_groups = {}  # (country, subdiv) → [(lat, lon), ...]
    for coord, geo_result in updates.items():
        result_groups.setdefault(geo_result, []).append(coord)

    logger.info("Unique geo results: %d — applying updates...", len(result_groups))

    total_updated = 0
    for (country, subdiv), coord_list in result_groups.items():
        # Process in chunks to avoid oversized WHERE clauses
        for chunk_start in range(0, len(coord_list), 500):
            chunk = coord_list[chunk_start:chunk_start + 500]
            conditions = " OR ".join(
                f"(round(latitude, 3) = {lat} AND round(longitude, 3) = {lon})"
                for lat, lon in chunk
            )
            query = f"""
                ALTER TABLE sensor_readings UPDATE
                    geo_country = '{country}',
                    geo_subdivision = '{subdiv}'
                WHERE geo_country = ''
                  AND ({conditions})
                  AND timestamp > '2020-01-01'
            """
            client.command(query)
            total_updated += len(chunk)

        logger.info("  Updated %s/%s: %d coord bins", country, subdiv, len(coord_list))

    logger.info("Applied country+subdivision updates for %d coordinate bins", total_updated)


def _geocode_missing_subdivision(client, geocoder, batch_size):
    """Fix readings with geo_country set but no geo_subdivision."""
    logger.info("--- Phase: geocode missing subdivisions ---")

    # Get distinct coordinate bins that have country but no subdivision
    result = client.query("""
        SELECT DISTINCT
            round(latitude, 3) as lat,
            round(longitude, 3) as lon,
            geo_country
        FROM sensor_readings
        WHERE geo_country != '' AND geo_subdivision = ''
          AND latitude != 0 AND longitude != 0
          AND timestamp > '2020-01-01'
    """)
    coords = result.result_rows
    logger.info("Distinct coordinate bins to geocode: %d", len(coords))

    updates = {}  # (lat, lon, country) → subdivision
    failed = 0
    for i, (lat, lon, country) in enumerate(coords):
        geo = geocoder.reverse_geocode(float(lat), float(lon))
        if geo and geo.get("geo_subdivision") and geo["geo_subdivision"] != "unknown":
            updates[(float(lat), float(lon), country)] = geo["geo_subdivision"]
        else:
            failed += 1

        if (i + 1) % 1000 == 0:
            logger.info("  Geocoded %d/%d coordinate bins (%d failed)", i + 1, len(coords), failed)

    logger.info("Geocoded %d coordinate bins, %d failed", len(updates), failed)

    # Group by (country, subdivision) result
    result_groups = {}  # (country, subdiv) → [(lat, lon), ...]
    for (lat, lon, country), subdiv in updates.items():
        result_groups.setdefault((country, subdiv), []).append((lat, lon))

    logger.info("Unique geo results: %d — applying updates...", len(result_groups))

    total_updated = 0
    for (country, subdiv), coord_list in result_groups.items():
        for chunk_start in range(0, len(coord_list), 500):
            chunk = coord_list[chunk_start:chunk_start + 500]
            conditions = " OR ".join(
                f"(round(latitude, 3) = {lat} AND round(longitude, 3) = {lon})"
                for lat, lon in chunk
            )
            query = f"""
                ALTER TABLE sensor_readings UPDATE
                    geo_subdivision = '{subdiv}'
                WHERE geo_country = '{country}'
                  AND geo_subdivision = ''
                  AND ({conditions})
                  AND timestamp > '2020-01-01'
            """
            client.command(query)
            total_updated += len(chunk)

        logger.info("  Updated %s/%s: %d coord bins", country, subdiv, len(coord_list))

    logger.info("Applied subdivision updates for %d coordinate bins", total_updated)


def sign_batch(client, key_dir):
    """Sign unsigned readings with the station's Ed25519 key."""
    from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig

    key_manager = IngesterKeyManager(KeyConfig(key_dir=key_dir))
    key_manager.load_or_generate()
    private_key = key_manager.private_key
    ingester_id = key_manager.ingester_id
    key_version = key_manager.key_version

    logger.info("Signing with ingester_id=%s, key_version=%d", ingester_id, key_version)

    # Get date range of unsigned readings
    result = client.query("""
        SELECT min(toDate(timestamp)), max(toDate(timestamp)), count()
        FROM sensor_readings
        WHERE signature = '' AND received_via = 'local' AND timestamp > '2020-01-01'
    """)
    row = result.result_rows[0]
    if row[0] is None:
        logger.info("No unsigned local readings to sign")
        return

    start = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
    end = row[1] if isinstance(row[1], date) else date.fromisoformat(str(row[1]))
    total = row[2]
    logger.info("Unsigned local readings: %d from %s to %s", total, start, end)

    # All columns in sensor_readings schema order
    all_columns = [
        "timestamp", "device_id", "data_source", "network_source", "ingestion_node_id",
        "reading_type", "value", "unit", "sample_count", "sample_interval_avg",
        "value_min", "value_max", "latitude", "longitude", "altitude",
        "geo_country", "geo_subdivision", "geo_h3_res8", "sensor_model", "board_model",
        "calibration_status", "data_quality_flag", "deployment_type", "transport_type",
        "location_source", "firmware_version", "deployment_location", "node_name",
        "deployment_type_source", "node_info", "node_info_url",
        "signature", "ingester_id", "key_version", "received_via",
    ]

    idx = {col: i for i, col in enumerate(all_columns)}

    total_signed = 0
    current = start
    while current <= end:
        day_str = current.isoformat()
        columns_sql = ", ".join(all_columns)

        result = client.query(f"""
            SELECT {columns_sql}
            FROM sensor_readings
            WHERE toDate(timestamp) = {{day:String}}
              AND signature = ''
              AND received_via = 'local'
        """, parameters={"day": day_str})

        rows = result.result_rows
        if rows:
            signed_rows = []
            for row in rows:
                row = list(row)

                # Build signing payload
                ts = row[idx["timestamp"]]
                if hasattr(ts, "timestamp"):
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    ts_unix = int(ts.timestamp())
                else:
                    ts = datetime.fromisoformat(str(ts))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    ts_unix = int(ts.timestamp())

                payload_dict = {
                    "data_source": row[idx["data_source"]],
                    "device_id": row[idx["device_id"]],
                    "latitude": row[idx["latitude"]],
                    "longitude": row[idx["longitude"]],
                    "reading_type": row[idx["reading_type"]],
                    "timestamp": ts_unix,
                    "transport_type": row[idx["transport_type"]],
                    "value": row[idx["value"]],
                }
                payload = json.dumps(payload_dict, sort_keys=True).encode()
                signature = private_key.sign(payload)

                row[idx["signature"]] = signature.hex()
                row[idx["ingester_id"]] = ingester_id
                row[idx["key_version"]] = key_version
                signed_rows.append(row)

            client.insert("sensor_readings", signed_rows, column_names=all_columns)
            total_signed += len(signed_rows)
            logger.info("  %s: signed %d readings (total: %d)", day_str, len(signed_rows), total_signed)

        current += timedelta(days=1)

    logger.info("Signature backfill complete: %d readings signed", total_signed)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing geo data and signatures for historical readings"
    )
    parser.add_argument(
        "--mode", choices=["geocode", "sign", "both"], default="geocode",
        help="geocode: fill missing geo fields; sign: sign unsigned readings; both: geocode then sign"
    )
    parser.add_argument("--key-dir", default=os.environ.get("KEY_DIR", "/app/data/keys"),
                        help="Directory containing ingester_key.pem (for sign mode)")
    parser.add_argument("--ch-host", default=os.environ.get("CLICKHOUSE_HOST", "clickhouse"))
    parser.add_argument("--ch-port", type=int, default=int(os.environ.get("CLICKHOUSE_PORT", "8123")))
    parser.add_argument("--ch-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    parser.add_argument("--ch-database", default=os.environ.get("CLICKHOUSE_DATABASE", "wesense"))
    args = parser.parse_args()

    client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
    )
    logger.info("Connected to ClickHouse at %s:%d/%s", args.ch_host, args.ch_port, args.ch_database)

    if args.mode in ("geocode", "both"):
        geocode_batch(client)

    if args.mode in ("sign", "both"):
        sign_batch(client, args.key_dir)

    logger.info("All done.")


if __name__ == "__main__":
    main()
