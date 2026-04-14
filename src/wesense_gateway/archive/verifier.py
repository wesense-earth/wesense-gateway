"""Ed25519 signature verification for readings."""

import base64
import logging

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from wesense_ingester.pipeline import build_canonical, canonical_to_json
from wesense_ingester.signing.trust import TrustStore

logger = logging.getLogger(__name__)


def verify_signatures(
    readings: list[dict], trust_store: TrustStore
) -> tuple[list[dict], int]:
    """
    Verify Ed25519 signatures on readings.

    Each reading carries the public_key used for signing alongside the
    signature itself. Verification uses the reading's own public_key
    (self-verifying row) — the trust_store is only consulted as a fallback
    for legacy readings from before the public_key column existed, and
    for cross-checking identity when strict policy requires it.

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

        # Prefer the reading's own public_key (self-verifying row). Fall
        # back to the trust store for legacy readings that predate the
        # public_key column.
        public_key = None
        public_key_b64 = reading.get("public_key") or ""
        if public_key_b64:
            try:
                key_bytes = base64.b64decode(public_key_b64)
                public_key = Ed25519PublicKey.from_public_bytes(key_bytes)
            except Exception as e:
                logger.debug(
                    "Invalid public_key on reading %s (ingester=%s): %s",
                    reading.get("reading_id", "?"), ingester_id, e,
                )
        if public_key is None:
            public_key = trust_store.get_public_key(ingester_id, key_version)
        if public_key is None:
            # Unknown ingester and no embedded key — include without
            # verification (trust store and reading row both incomplete)
            logger.debug(
                "No public key for %s v%d — including reading without verification",
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
