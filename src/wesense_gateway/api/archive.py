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

                    # Detect gaps
                    gaps = _find_gaps(sorted_dates)

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
    """Find missing dates between earliest and latest in a sorted date list."""
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
