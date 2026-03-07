#!/usr/bin/env python3
"""
Backfill OrbitDB attestations with iroh_blake3_hash and path fields.

Existing attestations (from the old Kubo archiver) have readings_hash but no
iroh_blake3_hash or path. This script reads the iroh sidecar's path index,
matches manifest.json files to attestations via readings_hash, and updates
OrbitDB with the BLAKE3 hash and path of the corresponding readings.parquet.

Usage:
    python3 backfill_attestation_hashes.py [--sidecar URL] [--orbitdb URL] [--dry-run]

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


def list_dir_recursive(sidecar_url: str, prefix: str) -> list[str]:
    """Recursively list all entries under a prefix."""
    url = f"{sidecar_url}/list/{prefix}" if prefix else f"{sidecar_url}/list/"
    try:
        entries = fetch_json(url)
    except urllib.error.HTTPError:
        return []

    results = []
    for entry in entries:
        full_path = f"{prefix}/{entry}" if prefix else entry
        if entry.endswith(".parquet") or entry.endswith(".json"):
            results.append(full_path)
        else:
            # It's a directory — recurse
            results.extend(list_dir_recursive(sidecar_url, full_path))
    return results


def main():
    parser = argparse.ArgumentParser(description="Backfill attestation iroh hashes")
    parser.add_argument("--sidecar", default="http://localhost:4400", help="Iroh sidecar URL")
    parser.add_argument("--orbitdb", default="http://localhost:5200", help="OrbitDB URL")
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

    # Step 2: Find all manifest.json files in the sidecar
    print("Scanning sidecar for manifest.json files...")
    all_files = list_dir_recursive(sidecar_url, "")
    manifest_paths = [f for f in all_files if f.endswith("manifest.json")]
    print(f"  Found {len(manifest_paths)} manifest files, {len(all_files)} total files")

    # Step 3: Match manifests to attestations
    updated = 0
    skipped = 0
    errors = 0

    for i, manifest_path in enumerate(manifest_paths, 1):
        try:
            # Read manifest to get readings_hash
            manifest_bytes = get_blob(sidecar_url, manifest_path)
            manifest = json.loads(manifest_bytes)
            readings_hash = manifest.get("readings_hash", "")

            if not readings_hash or readings_hash not in att_by_hash:
                skipped += 1
                continue

            # Find corresponding readings.parquet path and its BLAKE3 hash
            base_dir = manifest_path.rsplit("/", 1)[0]
            parquet_path = f"{base_dir}/readings.parquet"

            # Get the BLAKE3 hash by doing HEAD on the blob
            # Actually, we need the hash from the index — use the status/list approach
            # The simplest: HEAD the blob and check if it exists, then get hash from index
            # But the sidecar doesn't expose hash via HEAD. Let's read the path_index.

            # We'll batch this — collect all matches first, then read index once
            att_by_hash[readings_hash]["_parquet_path"] = parquet_path
            att_by_hash[readings_hash]["_matched"] = True

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error reading {manifest_path}: {e}")

    # Step 4: Get BLAKE3 hashes from the sidecar's status/index
    # The path_index.json is on the host mount, but we can also fetch blob metadata
    # via the sidecar. Let's try GET /blobs/{path} with HEAD to check existence,
    # then store the blob to get the hash back... but that's wasteful.
    #
    # Better approach: read the path_index.json directly if accessible,
    # or use the sidecar's store endpoint.
    # Let's try fetching the index via the status endpoint or a direct file read.

    print("Fetching path index from sidecar...")
    try:
        # The path_index.json is served as part of the data dir
        # Try reading it from the sidecar's data dir mount
        status = fetch_json(f"{sidecar_url}/status")
        print(f"  Sidecar has {status.get('blob_count', '?')} blobs")
    except Exception as e:
        print(f"  Warning: could not get sidecar status: {e}")

    # We need the BLAKE3 hashes. The cleanest way is to PUT the parquet blob again
    # (idempotent, same hash) — but that's expensive for 2811 files.
    #
    # Instead, let's use a HEAD request approach: store a dummy, or better,
    # just read the path_index.json from the host mount.
    # Since this script runs ON the host, we can read the mounted data dir.

    # Try to find path_index.json on common mount paths
    index_paths_to_try = [
        "/data/iroh-sidecar/path_index.json",  # typical docker mount
        "data/path_index.json",
        "/mnt/ssd1pool_yoda/docker/wesense/data/iroh-sidecar/path_index.json",
    ]

    path_index = None
    for idx_path in index_paths_to_try:
        try:
            with open(idx_path) as f:
                path_index = json.load(f)
                print(f"  Loaded path index from {idx_path} ({len(path_index)} entries)")
                break
        except (FileNotFoundError, PermissionError):
            continue

    if path_index is None:
        # Fallback: ask user for the path
        print("\n  Could not find path_index.json automatically.")
        print("  Please provide the path to the iroh sidecar's path_index.json:")
        print("  (e.g., /mnt/ssd1pool_yoda/docker/wesense/data/iroh-sidecar/path_index.json)")
        idx_input = input("  Path: ").strip()
        if idx_input:
            with open(idx_input) as f:
                path_index = json.load(f)
                print(f"  Loaded {len(path_index)} entries")
        else:
            print("  No path provided. Exiting.")
            sys.exit(1)

    # Step 5: Update attestations
    matched = [a for a in att_by_hash.values() if a.get("_matched")]
    print(f"\nMatched {len(matched)} attestations to manifest files")

    for a in matched:
        parquet_path = a["_parquet_path"]
        readings_hash = a.get("manifest_hash") or a.get("_id")

        # Look up BLAKE3 hash from path index
        index_entry = path_index.get(parquet_path)
        if not index_entry:
            skipped += 1
            continue

        blake3_hash = index_entry.get("hash", "")
        if not blake3_hash:
            skipped += 1
            continue

        # Get ingester_id from existing attestation
        ingester_id = ""
        if a.get("attestations"):
            ingester_id = a["attestations"][0].get("ingester_id", "")

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

    print(f"\nDone: {updated} updated, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    main()
