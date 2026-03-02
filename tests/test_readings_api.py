"""Tests for the POST /readings API endpoint."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from wesense_gateway.app import create_app
from wesense_gateway.config import GatewayConfig
from wesense_gateway.models.reading import ProcessResult


@pytest.fixture
def mock_processor():
    """Mock ReadingProcessor."""
    proc = MagicMock()
    proc.process_batch = AsyncMock(
        return_value=ProcessResult(accepted=1, duplicates=0, errors=0)
    )
    proc.get_stats.return_value = {
        "clickhouse": {"buffer_size": 0, "total_written": 5, "total_failed": 0},
        "dedup": {"cache_size": 0, "duplicates_blocked": 0, "unique_processed": 5},
        "geocoder": {"hits": 3, "misses": 2, "size": 2, "maxsize": 4096},
    }
    return proc


@pytest.fixture
def client(config, mock_processor):
    """TestClient with mocked lifespan (no real ClickHouse or geocoder)."""
    app = create_app.__wrapped__(config) if hasattr(create_app, "__wrapped__") else None

    # Build the app without lifespan to avoid needing real services
    from fastapi import FastAPI
    from wesense_gateway.api.readings import router as readings_router
    from wesense_gateway.api.status import router as status_router
    from wesense_gateway.api.data import router as data_router

    app = FastAPI(title="WeSense Gateway Test")
    app.state.config = config
    app.state.processor = mock_processor
    app.state.archive_scheduler = MagicMock(
        get_stats=MagicMock(return_value={"last_cycle": "", "total_archived": 0, "interval_hours": 6.0})
    )
    app.state.backend = MagicMock()

    app.include_router(readings_router)
    app.include_router(status_router)
    app.include_router(data_router)

    return TestClient(app)


def test_post_readings(client, mock_processor):
    """POST /readings should accept a batch and return counts."""
    payload = {
        "readings": [
            {
                "timestamp": 1709337600,
                "device_id": "test-001",
                "data_source": "WESENSE",
                "reading_type": "temperature",
                "value": 22.5,
            }
        ]
    }
    response = client.post("/readings", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["accepted"] == 1
    assert data["duplicates"] == 0
    assert data["errors"] == 0
    mock_processor.process_batch.assert_called_once()


def test_post_readings_empty_batch(client, mock_processor):
    """POST /readings with empty batch should succeed."""
    mock_processor.process_batch.return_value = ProcessResult()

    response = client.post("/readings", json={"readings": []})
    assert response.status_code == 200
    data = response.json()
    assert data["accepted"] == 0


def test_post_readings_invalid_body(client):
    """POST /readings with invalid JSON should return 422."""
    response = client.post("/readings", json={"bad": "data"})
    assert response.status_code == 422


def test_health(client):
    """GET /health should return 200."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_status(client):
    """GET /status should return pipeline and archive stats."""
    response = client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert "pipeline" in data
    assert "archive" in data
