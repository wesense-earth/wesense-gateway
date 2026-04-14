"""Manifest and trust snapshot construction for archives."""

import hashlib
import json
from datetime import datetime, timezone

from wesense_ingester.pipeline import CURRENT_CANONICAL_VERSION
from wesense_ingester.signing.keys import IngesterKeyManager
from wesense_ingester.signing.trust import TrustStore


# The Parquet schema version written by this archiver. Bump whenever the
# Parquet column set changes (adding/removing/renaming columns). This is
# for consumer dispatch — "I see schema v2, I know it has data_license";
# entirely separate from the signing_payload_version per reading.
PARQUET_SCHEMA_VERSION = "v1"


def build_trust_snapshot(
    trust_store: TrustStore, ingester_ids: set[str]
) -> dict:
    """Build a trust snapshot containing only keys referenced in the batch."""
    snapshot = trust_store.export_snapshot(list(ingester_ids))
    snapshot["snapshot_time"] = datetime.now(timezone.utc).isoformat()
    return snapshot


def compute_readings_hash(reading_ids: list[str]) -> str:
    """Compute deterministic hash from sorted reading IDs."""
    concatenated = "".join(sorted(reading_ids))
    return hashlib.sha256(concatenated.encode()).hexdigest()


def build_manifest(
    period: str,
    region: str,
    subdivision: str,
    verified_count: int,
    failed_count: int,
    readings_hash: str,
    trust_snapshot_hash: str,
    key_manager: IngesterKeyManager,
) -> dict:
    """Build and sign an archive manifest."""
    manifest = {
        "version": 1,
        "parquet_schema_version": PARQUET_SCHEMA_VERSION,
        "current_signing_payload_version": CURRENT_CANONICAL_VERSION,
        "period": period,
        "region": region,
        "subdivision": subdivision,
        "reading_count": verified_count,
        "readings_hash": readings_hash,
        "trust_snapshot_hash": trust_snapshot_hash,
        "signatures_verified": verified_count,
        "signatures_failed": failed_count,
        "archiver_id": key_manager.ingester_id,
        "created": datetime.now(timezone.utc).isoformat(),
    }

    # Sign the manifest (exclude archiver_signature field)
    manifest_content = json.dumps(
        {k: v for k, v in manifest.items() if k != "archiver_signature"},
        sort_keys=True,
    ).encode()
    signature = key_manager.private_key.sign(manifest_content)
    manifest["archiver_signature"] = signature.hex()

    return manifest
