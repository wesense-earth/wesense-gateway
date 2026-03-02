"""POST /readings endpoint."""

from fastapi import APIRouter, Request

from wesense_gateway.models.reading import ProcessResult, ReadingBatch

router = APIRouter()


@router.post("/readings", response_model=ProcessResult)
async def ingest_readings(batch: ReadingBatch, request: Request) -> ProcessResult:
    """Ingest a batch of sensor readings."""
    processor = request.app.state.processor
    return await processor.process_batch(batch)
