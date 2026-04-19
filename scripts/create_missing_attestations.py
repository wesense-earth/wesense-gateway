#!/usr/bin/env python3
"""
Create OrbitDB attestations for iroh archives that don't have one.

The gateway archiver creates archives in iroh but attestation submission was
added later. This script reads every manifest.json from the iroh sidecar,
checks if an attestation already exists for that readings_hash, and creates
one if missing.

Usage:
    python3 create_missing_attestations.py --sidecar URL --orbitdb URL [--dry-run]
    python3 create_missing_attestations.py --path-index FILE --sidecar URL --orbitdb URL

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
    if path_index_file:
        with open(path_index_file) as f:
            idx = json.load(f)
            print(f"  Loaded path index from {path_index_file} ({len(idx)} entries)")
            return idx

    for p in ["/tmp/path_index.json", "/app/data/path_index.json"]:
        try:
            with open(p) as f:
                idx = json.load(f)
                print(f"  Loaded path index from {p} ({len(idx)} entries)")
                return idx
        except (FileNotFoundError, PermissionError):
            continue

    try:
        idx = fetch_json(f"{sidecar_url}/path-index")
        print(f"  Loaded path index from sidecar API ({len(idx)} entries)")
        return idx
    except Exception:
        pass

    print("  Could not load path_index.json. Use --path-index or ensure sidecar has /path-index endpoint.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Create attestations for iroh archives missing them")
    parser.add_argument("--sidecar", default="http://localhost:4400", help="Iroh sidecar URL")
    parser.add_argument("--orbitdb", default="http://localhost:5200", help="OrbitDB URL")
    parser.add_argument("--path-index", default="", help="Path to path_index.json file")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be created without writing")
    args = parser.parse_args()

    sidecar_url = args.sidecar.rstrip("/")
    orbitdb_url = args.orbitdb.rstrip("/")

    # Step 1: Get existing attestations to know which readings_hash values already exist
    print("Fetching existing attestations from OrbitDB...")
    att_data = fetch_json(f"{orbitdb_url}/attestations")
    attestations = att_data.get("attestations", [])
    existing_hashes = set()
    for a in attestations:
        rh = a.get("manifest_hash") or a.get("_id")
        if rh:
            existing_hashes.add(rh)
    print(f"  Existing attestations: {len(existing_hashes)}")

    # Step 2: Load path index
    print("Loading path index...")
    path_index = load_path_index(sidecar_url, args.path_index)

    # Step 3: Find all manifest.json entries
    manifest_paths = sorted(p for p in path_index if p.endswith("manifest.json"))
    print(f"  Found {len(manifest_paths)} manifest entries")

    created = 0
    skipped_existing = 0
    skipped_no_parquet = 0
    errors = 0

    for i, manifest_path in enumerate(manifest_paths, 1):
        try:
            # Read manifest from sidecar
            manifest_bytes = get_blob(sidecar_url, manifest_path)
            manifest = json.loads(manifest_bytes)
            readings_hash = manifest.get("readings_hash", "")

            if not readings_hash:
                errors += 1
                if errors <= 5:
                    print(f"  No readings_hash in {manifest_path}")
                continue

            # Skip if attestation already exists
            if readings_hash in existing_hashes:
                skipped_existing += 1
                continue

            # Find corresponding readings.parquet BLAKE3 hash
            base_dir = manifest_path.rsplit("/", 1)[0]
            parquet_path = f"{base_dir}/readings.parquet"
            index_entry = path_index.get(parquet_path)

            if not index_entry or not index_entry.get("hash"):
                skipped_no_parquet += 1
                continue

            blake3_hash = index_entry["hash"]
            ingester_id = manifest.get("archiver_id", "backfill")

            if args.dry_run:
                print(f"  Would create: {readings_hash[:16]}... path={parquet_path} "
                      f"region={manifest.get('region','?')}/{manifest.get('subdivision','?')} "
                      f"period={manifest.get('period','?')} readings={manifest.get('reading_count','?')}")
                created += 1
            else:
                put_json(
                    f"{orbitdb_url}/attestations/{readings_hash}",
                    {
                        "ingester_id": ingester_id,
                        "iroh_blake3_hash": blake3_hash,
                        "path": parquet_path,
                        "period": manifest.get("period", ""),
                        "region": manifest.get("region", ""),
                        "subdivision": manifest.get("subdivision", ""),
                        "reading_count": manifest.get("reading_count", 0),
                        "readings_hash": readings_hash,
                        "signatures_verified": manifest.get("signatures_verified", 0),
                        "signatures_failed": manifest.get("signatures_failed", 0),
                    },
                )
                created += 1
                existing_hashes.add(readings_hash)
                if created % 500 == 0:
                    print(f"  Created {created}...")

        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  Error processing {manifest_path}: {e}")

        if i % 5000 == 0:
            print(f"  Progress: {i}/{len(manifest_paths)} manifests processed...")

    print(f"\nDone: {created} created, {skipped_existing} already existed, "
          f"{skipped_no_parquet} missing parquet, {errors} errors")


if __name__ == "__main__":
    main()
