"""Filesystem storage backend for archive files."""

import hashlib
import logging
from pathlib import Path

from wesense_gateway.backends.base import StorageBackend

logger = logging.getLogger(__name__)


class FilesystemBackend(StorageBackend):
    """
    Plain filesystem storage.

    Structure: {base_dir}/{country}/{subdivision}/{YYYY}/{MM}/{DD}/readings.parquet
    """

    def __init__(self, base_dir: str):
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)

    async def store(self, path: str, data: bytes) -> str:
        """Write data to filesystem. Returns SHA-256 hex digest."""
        full = self._base / path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(data)
        sha = hashlib.sha256(data).hexdigest()
        logger.debug("Stored %d bytes at %s (sha256=%s...)", len(data), path, sha[:16])
        return sha

    async def retrieve(self, path: str) -> bytes | None:
        """Read data from filesystem."""
        full = self._base / path
        if not full.is_file():
            return None
        return full.read_bytes()

    async def exists(self, path: str) -> bool:
        """Check if file exists."""
        return (self._base / path).is_file()

    async def list_dir(self, path: str) -> list[str]:
        """List directory entries."""
        full = self._base / path
        if not full.is_dir():
            return []
        return sorted(e.name for e in full.iterdir())

    async def get_archived_dates(
        self, country: str, subdivision: str
    ) -> set[str]:
        """Walk {country}/{subdivision}/{YYYY}/{MM}/{DD}/ tree to find archived dates."""
        archived: set[str] = set()
        root = self._base / country / subdivision

        if not root.is_dir():
            return archived

        for year_dir in sorted(root.iterdir()):
            if not year_dir.is_dir():
                continue
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue
                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir():
                        continue
                    archived.add(
                        f"{year_dir.name}-{month_dir.name}-{day_dir.name}"
                    )

        return archived
