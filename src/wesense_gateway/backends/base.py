"""Abstract storage backend for archive files."""

from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """Async interface for storing and retrieving archive files."""

    @abstractmethod
    async def store(self, path: str, data: bytes) -> str:
        """Store data at path. Returns SHA-256 hex digest of data."""
        ...

    @abstractmethod
    async def retrieve(self, path: str) -> bytes | None:
        """Retrieve data from path. Returns None if not found."""
        ...

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if a file exists at path."""
        ...

    @abstractmethod
    async def list_dir(self, path: str) -> list[str]:
        """List entries in a directory."""
        ...

    @abstractmethod
    async def get_archived_dates(
        self, country: str, subdivision: str
    ) -> set[str]:
        """Get ISO date strings for already-archived days under country/subdivision."""
        ...
