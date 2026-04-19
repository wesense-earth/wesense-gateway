"""Archive stats and trigger endpoints."""

import asyncio
import logging
import time
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()
logger = logging.getLogger(__name__)

# Rate limit: one manual trigger per 5 minutes
_last_trigger: float = 0.0

# 24h coverage cache — (timestamp, data) to avoid querying ClickHouse on every request
_coverage_cache: tuple[float, dict] = (0.0, {})
_COVERAGE_CACHE_TTL = 60.0  # seconds

# Expected readings per 24h for "full coverage" — 48 30-min buckets
COVERAGE_BUCKETS_24H = 48


@router.get("/archive/stats")
async def archive_stats(request: Request):
    """
    Per-region archive summary with gap detection.

    Combines iroh sidecar status, per-region date enumeration,
    and gateway scheduler stats.
    """
    backend = request.app.state.backend
    scheduler = getattr(request.app.state, "archive_scheduler", None)

    # Get iroh sidecar status
    sidecar_status = {}
    try:
        response = await backend._client.get("/status")
        if response.status_code == 200:
            sidecar_status = response.json()
    except Exception as e:
        logger.warning("Failed to get iroh sidecar status: %s", e)

    # Get 24h coverage data (cached) — maps (country, subdivision) → list of missing bucket ISO timestamps
    # Also returns the full list of expected buckets, used as default for regions with zero recent readings.
    coverage, all_buckets = await _get_24h_coverage(scheduler)

    # Get all regions from the backend's directory listing
    regions = []
    try:
        top_level = await backend.list_dir("")
        for country in sorted(top_level):
            country = country.strip("/")
            if not country or len(country) > 3:
                continue
            subdivisions = await backend.list_dir(country)
            for subdiv in sorted(subdivisions):
                subdiv = subdiv.strip("/")
                if not subdiv:
                    continue
                try:
                    dates = await backend.get_archived_dates(country, subdiv)
                    if not dates:
                        continue
                    sorted_dates = sorted(dates)
                    earliest = sorted_dates[0]
                    latest = sorted_dates[-1]

                    # 24h coverage gaps — 30-min buckets missing in last 24h.
                    # Default (no recent readings at all) is ALL 48 buckets missing.
                    gaps = coverage.get((country, subdiv), all_buckets)

                    regions.append({
                        "country": country,
                        "subdivision": subdiv,
                        "day_count": len(dates),
                        "earliest": earliest,
                        "latest": latest,
                        "gaps": gaps,
                    })
                except Exception as e:
                    logger.warning("Failed to get dates for %s/%s: %s", country, subdiv, e)
    except Exception as e:
        logger.warning("Failed to list archive regions: %s", e)

    total_days = sum(r["day_count"] for r in regions)

    # Scheduler stats
    sched_stats = scheduler.get_stats() if scheduler else {}

    return {
        "node_id": sidecar_status.get("node_id", ""),
        "total_blobs": sidecar_status.get("blob_count", 0),
        "total_regions": len(regions),
        "total_days": total_days,
        "store_scope": sidecar_status.get("store_scope", []),
        "replication": sidecar_status.get("replication", {}),
        "last_archive_cycle": sched_stats.get("last_cycle", ""),
        "archive_interval_hours": sched_stats.get("interval_hours", 0),
        "total_archived_this_session": sched_stats.get("total_archived", 0),
        "regions": regions,
    }


@router.post("/archive/trigger")
async def trigger_archive(request: Request):
    """Trigger an immediate archive cycle. Rate-limited to once per 5 minutes."""
    global _last_trigger

    scheduler = getattr(request.app.state, "archive_scheduler", None)
    if not scheduler:
        raise HTTPException(status_code=503, detail="Archive scheduler not running")

    now = time.monotonic()
    if now - _last_trigger < 300:
        remaining = int(300 - (now - _last_trigger))
        raise HTTPException(
            status_code=429,
            detail=f"Rate limited. Try again in {remaining}s",
        )

    _last_trigger = now
    asyncio.create_task(scheduler._run_cycle())
    return {"message": "Archive cycle triggered"}


def _find_gaps(sorted_dates: list[str]) -> list[str]:
    """Find missing dates between earliest and latest in a sorted date list.

    Kept for backward compatibility — the active archive stats now uses
    24h coverage (see _get_24h_coverage) which is more useful for
    operational monitoring.
    """
    if len(sorted_dates) < 2:
        return []

    gaps = []
    try:
        prev = date.fromisoformat(sorted_dates[0])
        for ds in sorted_dates[1:]:
            curr = date.fromisoformat(ds)
            diff = (curr - prev).days
            if diff > 1:
                # Add all missing dates in the gap
                for i in range(1, diff):
                    gaps.append((prev + timedelta(days=i)).isoformat())
            prev = curr
    except (ValueError, TypeError):
        pass

    return gaps


async def _get_24h_coverage(scheduler) -> tuple[dict[tuple[str, str], list[str]], list[str]]:
    """Compute per-region 30-minute bucket coverage over the last 24 hours.

    Returns a tuple of:
      - dict mapping (country, subdivision) → list of missing bucket ISO timestamps
        (empty list means full coverage; list of all 48 means nothing present)
      - list of all 48 expected bucket ISO timestamps (used as default for
        regions with zero recent readings, so they show as "all missing")

    Cached for _COVERAGE_CACHE_TTL seconds.
    """
    global _coverage_cache

    # Compute expected 48 buckets aligned to 30-min boundaries ending at now
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    minute = (now_utc.minute // 30) * 30
    latest_bucket = now_utc.replace(minute=minute)
    expected_buckets = [
        latest_bucket - timedelta(minutes=30 * i)
        for i in range(COVERAGE_BUCKETS_24H)
    ]
    all_missing = [b.isoformat() for b in expected_buckets]

    # Return cache if still fresh
    now_ts = time.monotonic()
    cached_at, cached_data = _coverage_cache
    if now_ts - cached_at < _COVERAGE_CACHE_TTL:
        return cached_data, all_missing

    if scheduler is None or scheduler._ch_client is None:
        return {}, all_missing

    query = """
        SELECT
            geo_country,
            geo_subdivision,
            toStartOfInterval(timestamp, INTERVAL 30 MINUTE) AS bucket
        FROM sensor_readings
        WHERE timestamp > now() - INTERVAL 24 HOUR
          AND geo_country != ''
          AND geo_subdivision != ''
        GROUP BY geo_country, geo_subdivision, bucket
    """

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: scheduler._ch_client.query(query)
        )
    except Exception as e:
        logger.warning("24h coverage query failed: %s", e)
        return {}, all_missing

    # Build set of present buckets per region
    present_by_region: dict[tuple[str, str], set[datetime]] = {}
    for row in result.result_rows:
        country, subdivision, bucket = row[0], row[1], row[2]
        key = (country, subdivision)
        if key not in present_by_region:
            present_by_region[key] = set()
        if bucket.tzinfo is None:
            bucket = bucket.replace(tzinfo=timezone.utc)
        present_by_region[key].add(bucket)

    coverage: dict[tuple[str, str], list[str]] = {}
    for key, present in present_by_region.items():
        missing = [b.isoformat() for b in expected_buckets if b not in present]
        coverage[key] = missing

    _coverage_cache = (now_ts, coverage)
    logger.info("24h coverage computed: %d regions have recent readings", len(coverage))
    return coverage, all_missing
