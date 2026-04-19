#!/usr/bin/env python3
"""
Backfill OrbitDB attestations with iroh_blake3_hash and path fields.

Existing attestations (from the old Kubo archiver) have readings_hash but no
iroh_blake3_hash or path. This script uses the path_index.json (from the iroh
sidecar) to find readings.parquet BLAKE3 hashes, matches them to attestations
by reading manifest.json files (either via sidecar HTTP or from the index),
and updates OrbitDB.

Usage:
    python3 backfill_attestation_hashes.py [--sidecar URL] [--orbitdb URL] [--dry-run]
    python3 backfill_attestation_hashes.py --path-index /path/to/path_index.json --orbitdb URL

Defaults:
    --sidecar  http://localhost:4400
    --orbitdb  http://localhost:5200
"""

import argparse
import json
import sys
import urllib.request
import urllib.error


def fetch_json(url: str) -> dict | list:
    """GET a URL and parse JSON response."""
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def put_json(url: str, data: dict) -> dict:
    """PUT JSON to a URL and parse response."""
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def get_blob(sidecar_url: str, path: str) -> bytes:
    """GET raw blob bytes from the sidecar."""
    url = f"{sidecar_url}/blobs/{path}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def load_path_index(sidecar_url: str, path_index_file: str) -> dict:
    """Load the path index from a file or from the sidecar HTTP API."""
    # Try CLI-provided file first
    if path_index_file:
        with open(path_index_file) as f:
            idx = json.load(f)
            print(f"  Loaded path index from {path_index_file} ({len(idx)} entries)")
            return idx

    # Try common file paths
    for p in ["/tmp/path_index.json", "/app/data/path_index.json"]:
        try:
            with open(p) as f:
                idx = json.load(f)
                print(f"  Loaded path index from {p} ({len(idx)} entries)")
                return idx
        except (FileNotFoundError, PermissionError):
            continue

    # Try sidecar /path-index endpoint
    try:
        idx = fetch_json(f"{sidecar_url}/path-index")
        print(f"  Loaded path index from sidecar API ({len(idx)} entries)")
        return idx
    except Exception:
        pass

    print("  Could not load path_index.json. Use --path-index or ensure sidecar is reachable.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Backfill attestation iroh hashes")
    parser.add_argument("--sidecar", default="http://localhost:4400", help="Iroh sidecar URL")
    parser.add_argument("--orbitdb", default="http://localhost:5200", help="OrbitDB URL")
    parser.add_argument("--path-index", default="", help="Path to path_index.json file")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be updated without writing")
    args = parser.parse_args()

    sidecar_url = args.sidecar.rstrip("/")
    orbitdb_url = args.orbitdb.rstrip("/")

    # Step 1: Get all attestations
    print("Fetching attestations from OrbitDB...")
    att_data = fetch_json(f"{orbitdb_url}/attestations")
    attestations = att_data.get("attestations", [])
    print(f"  Total attestations: {len(attestations)}")

    # Filter to ones missing iroh fields
    missing = [a for a in attestations if not a.get("iroh_blake3_hash")]
    print(f"  Missing iroh_blake3_hash: {len(missing)}")

    if not missing:
        print("Nothing to backfill.")
        return

    # Build a lookup by readings_hash (= manifest_hash = _id)
    att_by_hash = {}
    for a in missing:
        rh = a.get("manifest_hash") or a.get("_id")
        if rh:
            att_by_hash[rh] = a

    # Step 2: Load path index
    print("Loading path index...")
    path_index = load_path_index(sidecar_url, args.path_index)

    # Step 3: Find manifest.json paths and read them to get readings_hash
    manifest_paths = [p for p in path_index if p.endswith("manifest.json")]
    print(f"  Found {len(manifest_paths)} manifest entries in path index")

    updated = 0
    skipped = 0
    errors = 0
    matched = 0

    for manifest_path in manifest_paths:
        try:
            # Read manifest content from sidecar
            manifest_bytes = get_blob(sidecar_url, manifest_path)
            manifest = json.loads(manifest_bytes)
            readings_hash = manifest.get("readings_hash", "")

            if not readings_hash or readings_hash not in att_by_hash:
                skipped += 1
                continue

            # Derive the readings.parquet path
            base_dir = manifest_path.rsplit("/", 1)[0]
            parquet_path = f"{base_dir}/readings.parquet"

            # Look up BLAKE3 hash from path index
            index_entry = path_index.get(parquet_path)
            if not index_entry:
                skipped += 1
                continue

            blake3_hash = index_entry.get("hash", "")
            if not blake3_hash:
                skipped += 1
                continue

            matched += 1

            # Get ingester_id from existing attestation
            ingester_id = ""
            att = att_by_hash[readings_hash]
            if att.get("attestations"):
                ingester_id = att["attestations"][0].get("ingester_id", "")

            if args.dry_run:
                print(f"  Would update {readings_hash[:16]}... -> blake3={blake3_hash[:16]}... path={parquet_path}")
                updated += 1
            else:
                try:
                    put_json(
                        f"{orbitdb_url}/attestations/{readings_hash}",
                        {
                            "ingester_id": ingester_id or "backfill",
                            "iroh_blake3_hash": blake3_hash,
                            "path": parquet_path,
                        },
                    )
                    updated += 1
                    if updated % 100 == 0:
                        print(f"  Updated {updated}...")
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  Error updating {readings_hash[:16]}...: {e}")

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error reading {manifest_path}: {e}")

    print(f"\nDone: {matched} matched, {updated} updated, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    main()
