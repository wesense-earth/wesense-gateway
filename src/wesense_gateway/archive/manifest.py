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
    """Build a trust snapshot from the local TrustStore (legacy path).

    Kept for backward compatibility. Prefer build_trust_snapshot_from_readings()
    which sources public keys directly from the reading rows, making archives
    self-contained without any TrustStore dependency. See
    governance-and-trust.md §"Trust Retention — Two Distinct Concerns".
    """
    snapshot = trust_store.export_snapshot(list(ingester_ids))
    snapshot["snapshot_time"] = datetime.now(timezone.utc).isoformat()
    return snapshot


def build_trust_snapshot_from_readings(readings: list[dict]) -> dict:
    """Build a trust snapshot from the public_key column on each reading row.

    This is the preferred approach: every reading row carries the public key
    used to sign it, so the archive's trust snapshot is a deterministic
    projection of the readings themselves. No dependence on a live TrustStore,
    no risk of OrbitDB TTL expiring keys we still need.

    Returns a dict in the same shape as TrustStore.export_snapshot():
        {
            "keys": {
                "<ingester_id>": {
                    "<key_version>": {
                        "public_key": "<base64>",
                        "status": "active",
                    }
                }
            },
            "snapshot_time": "<iso>",
        }
    """
    keys: dict[str, dict[str, dict[str, str]]] = {}
    for r in readings:
        ingester_id = r.get("ingester_id") or ""
        public_key = r.get("public_key") or ""
        if not ingester_id or not public_key:
            continue
        key_version = str(r.get("key_version") or 1)
        if ingester_id not in keys:
            keys[ingester_id] = {}
        # First occurrence wins — if the same (ingester_id, key_version) pair
        # has different public_key values (which should never happen but is
        # defensive), we take the first. Later additions at the same key are
        # no-ops.
        if key_version not in keys[ingester_id]:
            keys[ingester_id][key_version] = {
                "public_key": public_key,
                "status": "active",
            }
    return {
        "keys": keys,
        "snapshot_time": datetime.now(timezone.utc).isoformat(),
    }


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
