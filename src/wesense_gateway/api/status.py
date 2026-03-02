"""GET /health and GET /status endpoints."""

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/health")
async def health():
    """Simple health check for Docker HEALTHCHECK."""
    return {"status": "ok"}


@router.get("/status")
async def status(request: Request):
    """Detailed status with metrics from all subsystems."""
    stats = {}

    processor = getattr(request.app.state, "processor", None)
    if processor:
        stats["pipeline"] = processor.get_stats()

    scheduler = getattr(request.app.state, "archive_scheduler", None)
    if scheduler:
        stats["archive"] = scheduler.get_stats()

    return stats
