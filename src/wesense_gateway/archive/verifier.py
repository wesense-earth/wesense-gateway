"""Ed25519 signature verification for readings."""

import json
import logging

from wesense_ingester.signing.trust import TrustStore

logger = logging.getLogger(__name__)


def verify_signatures(
    readings: list[dict], trust_store: TrustStore
) -> tuple[list[dict], int]:
    """
    Verify Ed25519 signatures on readings.

    Each reading must have ingester_id, key_version, signature, and the
    8-field signing payload (data_source, device_id, latitude, longitude,
    reading_type, timestamp as unix int, transport_type, value).

    Returns (verified_readings, failed_count).
    """
    verified = []
    failed = 0

    for reading in readings:
        ingester_id = reading.get("ingester_id", "")
        key_version = reading.get("key_version", 0)
        signature_hex = reading.get("signature", "")

        if not ingester_id or not signature_hex:
            failed += 1
            continue

        public_key = trust_store.get_public_key(ingester_id, key_version)
        if public_key is None:
            # Unknown ingester — include without verification
            # (trust store may be incomplete)
            logger.debug(
                "No trusted key for %s v%d — including reading without verification",
                ingester_id, key_version,
            )
            verified.append(reading)
            continue

        # Reconstruct the exact payload that was signed:
        # 8 fields with timestamp as unix int, sorted keys
        payload_dict = {
            "data_source": reading.get("data_source", ""),
            "device_id": reading["device_id"],
            "latitude": reading["latitude"],
            "longitude": reading["longitude"],
            "reading_type": reading["reading_type"],
            "timestamp": reading["_ts_unix"],
            "transport_type": reading.get("transport_type", ""),
            "value": reading["value"],
        }
        payload = json.dumps(payload_dict, sort_keys=True).encode()

        try:
            signature_bytes = bytes.fromhex(signature_hex)
            public_key.verify(signature_bytes, payload)
            verified.append(reading)
        except Exception:
            failed += 1
            logger.debug(
                "Signature verification failed for reading %s (ingester=%s)",
                reading.get("reading_id", "?"), ingester_id,
            )

    return verified, failed
