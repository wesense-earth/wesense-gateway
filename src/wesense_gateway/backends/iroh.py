"""Archive replicator storage backend — delegates to the Rust archive replicator HTTP API."""

import logging
import os

import httpx

from wesense_gateway.backends.base import StorageBackend

logger = logging.getLogger(__name__)


class IrohBackend(StorageBackend):
    """
    Storage backend that talks to the wesense-archive-replicator.

    All blob operations are proxied as HTTP calls to the archive replicator's API.
    The replicator handles iroh-blobs storage, tagging, and gossip announcements.
    """

    def __init__(self, sidecar_url: str = "http://localhost:4002"):
        self._url = sidecar_url.rstrip("/")
        # When TLS_ENABLED, upgrade http:// to https://
        if os.getenv("TLS_ENABLED", "").lower() == "true":
            self._url = self._url.replace("http://", "https://")
        self._client = self._create_client()

    def _create_client(self) -> httpx.AsyncClient:
        """Create an httpx client with connection retries.

        Retries on TCP connection failure (e.g. archive replicator restarted)
        and expires idle connections after 30s to prevent stale pool entries.
        When TLS_ENABLED, trusts the deployment CA for self-signed certs.
        """
        ca_certfile = os.getenv("TLS_CA_CERTFILE", "")
        verify = ca_certfile if ca_certfile and os.path.exists(ca_certfile) else True
        transport = httpx.AsyncHTTPTransport(
            retries=2,
            limits=httpx.Limits(keepalive_expiry=30.0),
            verify=verify,
        )
        return httpx.AsyncClient(
            base_url=self._url,
            timeout=30.0,
            transport=transport,
        )

    async def store(self, path: str, data: bytes) -> str:
        """Store data via PUT /blobs/{path}. Returns BLAKE3 hash hex string."""
        response = await self._client.put(
            f"/blobs/{path}",
            content=data,
            headers={"Content-Type": "application/octet-stream"},
        )
        response.raise_for_status()
        result = response.json()
        logger.debug(
            "Stored %d bytes at %s (hash=%s...)",
            len(data),
            path,
            result["hash"][:16],
        )
        return result["hash"]

    async def retrieve(self, path: str) -> bytes | None:
        """Retrieve blob via GET /blobs/{path}. Returns None if not found."""
        response = await self._client.get(f"/blobs/{path}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.content

    async def exists(self, path: str) -> bool:
        """Check blob existence via HEAD /blobs/{path}."""
        response = await self._client.head(f"/blobs/{path}")
        return response.status_code == 200

    async def list_dir(self, path: str) -> list[str]:
        """List entries under a directory prefix via GET /list/{path}."""
        response = await self._client.get(f"/list/{path}")
        response.raise_for_status()
        return response.json()

    async def get_archived_dates(
        self, country: str, subdivision: str
    ) -> set[str]:
        """Get archived ISO date strings via GET /archived-dates/{country}/{subdivision}."""
        response = await self._client.get(
            f"/archived-dates/{country}/{subdivision}"
        )
        response.raise_for_status()
        return set(response.json())

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()
