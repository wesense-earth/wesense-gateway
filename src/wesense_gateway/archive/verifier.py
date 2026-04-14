"""Ed25519 signature verification for readings."""

import logging

from wesense_ingester.pipeline import build_canonical, canonical_to_json
from wesense_ingester.signing.trust import TrustStore

logger = logging.getLogger(__name__)


def verify_signatures(
    readings: list[dict], trust_store: TrustStore
) -> tuple[list[dict], int]:
    """
    Verify Ed25519 signatures on readings.

    Reconstructs the canonical reading from the stored fields and verifies
    the stored signature against it. This matches what the ingester's
    ReadingPipeline signs (see wesense_ingester.pipeline).

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

        # Reconstruct the canonical reading that was signed.
        # Use the reading's own signing_payload_version to pick the right
        # builder — old readings signed with v1 always verify against v1,
        # even if newer canonical versions exist.
        #
        # Default to version 1 for any reading missing the column (historical
        # readings from before migration 007 were all v1).
        src = dict(reading)
        src["timestamp"] = reading["_ts_unix"]
        src["sensor_transport"] = reading.get("transport_type", "")
        signing_version = int(reading.get("signing_payload_version") or 1)
        try:
            canonical = build_canonical(src, version=signing_version)
            payload = canonical_to_json(canonical)
        except (KeyError, ValueError, TypeError) as e:
            failed += 1
            logger.debug(
                "Failed to rebuild canonical v%d for reading %s: %s",
                signing_version, reading.get("reading_id", "?"), e,
            )
            continue

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
