"""Unit tests for IrohBackend — mock HTTP responses to verify StorageBackend contract."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from wesense_gateway.backends.base import StorageBackend
from wesense_gateway.backends.iroh import IrohBackend


@pytest.fixture
def backend():
    """IrohBackend pointed at a dummy sidecar URL."""
    return IrohBackend(sidecar_url="http://localhost:4002")


def test_implements_storage_backend():
    """IrohBackend must be a subclass of StorageBackend."""
    assert issubclass(IrohBackend, StorageBackend)


async def test_store(backend):
    """store() should PUT bytes and return the hash from the response."""
    mock_response = httpx.Response(
        200,
        json={"hash": "abc123def456"},
        request=httpx.Request("PUT", "http://localhost:4002/blobs/test/file.parquet"),
    )

    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.put = AsyncMock(return_value=mock_response)

    result = await backend.store("test/file.parquet", b"parquet-data")

    assert result == "abc123def456"
    backend._client.put.assert_called_once_with(
        "/blobs/test/file.parquet",
        content=b"parquet-data",
        headers={"Content-Type": "application/octet-stream"},
    )


async def test_retrieve_found(backend):
    """retrieve() should return bytes when the blob exists."""
    mock_response = httpx.Response(
        200,
        content=b"parquet-data",
        request=httpx.Request("GET", "http://localhost:4002/blobs/test/file.parquet"),
    )

    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.get = AsyncMock(return_value=mock_response)

    result = await backend.retrieve("test/file.parquet")

    assert result == b"parquet-data"
    backend._client.get.assert_called_once_with("/blobs/test/file.parquet")


async def test_retrieve_not_found(backend):
    """retrieve() should return None when the blob doesn't exist."""
    mock_response = httpx.Response(
        404,
        request=httpx.Request("GET", "http://localhost:4002/blobs/nope"),
    )

    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.get = AsyncMock(return_value=mock_response)

    result = await backend.retrieve("nope")

    assert result is None


async def test_exists_true(backend):
    """exists() should return True when HEAD returns 200."""
    mock_response = httpx.Response(
        200,
        request=httpx.Request("HEAD", "http://localhost:4002/blobs/test/file.parquet"),
    )

    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.head = AsyncMock(return_value=mock_response)

    assert await backend.exists("test/file.parquet") is True


async def test_exists_false(backend):
    """exists() should return False when HEAD returns 404."""
    mock_response = httpx.Response(
        404,
        request=httpx.Request("HEAD", "http://localhost:4002/blobs/test/file.parquet"),
    )

    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.head = AsyncMock(return_value=mock_response)

    assert await backend.exists("test/file.parquet") is False


async def test_list_dir(backend):
    """list_dir() should return the JSON array from the sidecar."""
    mock_response = httpx.Response(
        200,
        json=["2026", "2025"],
        request=httpx.Request("GET", "http://localhost:4002/list/nz/wgn"),
    )

    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.get = AsyncMock(return_value=mock_response)

    result = await backend.list_dir("nz/wgn")

    assert result == ["2026", "2025"]
    backend._client.get.assert_called_once_with("/list/nz/wgn")


async def test_get_archived_dates(backend):
    """get_archived_dates() should return a set of ISO date strings."""
    mock_response = httpx.Response(
        200,
        json=["2026-03-01", "2026-03-02", "2026-02-28"],
        request=httpx.Request(
            "GET", "http://localhost:4002/archived-dates/nz/wgn"
        ),
    )

    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.get = AsyncMock(return_value=mock_response)

    result = await backend.get_archived_dates("nz", "wgn")

    assert result == {"2026-03-01", "2026-03-02", "2026-02-28"}
    backend._client.get.assert_called_once_with("/archived-dates/nz/wgn")


async def test_close(backend):
    """close() should close the httpx client."""
    backend._client = AsyncMock(spec=httpx.AsyncClient)
    backend._client.aclose = AsyncMock()

    await backend.close()

    backend._client.aclose.assert_called_once()
