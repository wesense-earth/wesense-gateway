"""GET /data/{path} endpoint — serve archive Parquet files over HTTP."""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

router = APIRouter()


@router.get("/data/{path:path}")
async def serve_archive(path: str, request: Request):
    """
    Serve archive files (Parquet, JSON) over HTTP.

    Enables ClickHouse url() queries like:
        SELECT * FROM url('http://gateway:8080/data/nz/wgn/2026/03/01/readings.parquet', Parquet)
    """
    backend = request.app.state.backend
    data = await backend.retrieve(path)

    if data is None:
        raise HTTPException(status_code=404, detail=f"Not found: {path}")

    if path.endswith(".parquet"):
        media_type = "application/octet-stream"
    elif path.endswith(".json"):
        media_type = "application/json"
    else:
        media_type = "application/octet-stream"

    return Response(content=data, media_type=media_type)
