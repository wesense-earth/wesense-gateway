"""FastAPI application factory with lifespan management."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from wesense_ingester.cache.dedup import DeduplicationCache
from wesense_ingester.geocoding.geocoder import ReverseGeocoder

from wesense_gateway.api.data import router as data_router
from wesense_gateway.api.readings import router as readings_router
from wesense_gateway.api.status import router as status_router
from wesense_gateway.archive.scheduler import ArchiveScheduler
from wesense_gateway.backends.filesystem import FilesystemBackend
from wesense_gateway.config import GatewayConfig
from wesense_gateway.pipeline.processor import ReadingProcessor
from wesense_gateway.storage.clickhouse import AsyncClickHouseWriter

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start and stop all subsystems."""
    config: GatewayConfig = app.state.config

    # ClickHouse writer
    ch_writer = AsyncClickHouseWriter(config)
    await ch_writer.start()

    # Geocoder + dedup
    geocoder = ReverseGeocoder()
    dedup_cache = DeduplicationCache()

    # Processor
    processor = ReadingProcessor(ch_writer, geocoder, dedup_cache)
    app.state.processor = processor

    # Storage backend
    backend = FilesystemBackend(config.archive_data_dir)
    app.state.backend = backend

    # Archive scheduler
    scheduler = ArchiveScheduler(config, backend)
    await scheduler.start()
    app.state.archive_scheduler = scheduler

    logger.info("Gateway started")
    yield

    # Shutdown
    await scheduler.stop()
    await ch_writer.close()
    logger.info("Gateway stopped")


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    if config is None:
        config = GatewayConfig()

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    app = FastAPI(title="WeSense Gateway", version="0.1.0", lifespan=lifespan)
    app.state.config = config

    app.include_router(readings_router)
    app.include_router(status_router)
    app.include_router(data_router)

    return app
