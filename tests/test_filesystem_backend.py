"""Tests for the filesystem storage backend."""

import os
import tempfile

import pytest

from wesense_gateway.backends.filesystem import FilesystemBackend


@pytest.fixture
def backend(tmp_path):
    return FilesystemBackend(str(tmp_path))


async def test_store_and_retrieve(backend):
    """Store data and retrieve it."""
    data = b"hello archive"
    sha = await backend.store("test/file.txt", data)

    assert len(sha) == 64  # SHA-256 hex
    result = await backend.retrieve("test/file.txt")
    assert result == data


async def test_retrieve_missing(backend):
    """Retrieve non-existent file returns None."""
    result = await backend.retrieve("does/not/exist.txt")
    assert result is None


async def test_exists(backend):
    """exists() should return True after store."""
    assert not await backend.exists("test/file.txt")
    await backend.store("test/file.txt", b"data")
    assert await backend.exists("test/file.txt")


async def test_list_dir(backend):
    """list_dir should return sorted entry names."""
    await backend.store("dir/b.txt", b"b")
    await backend.store("dir/a.txt", b"a")
    await backend.store("dir/c.txt", b"c")

    entries = await backend.list_dir("dir")
    assert entries == ["a.txt", "b.txt", "c.txt"]


async def test_list_dir_missing(backend):
    """list_dir on non-existent path returns empty list."""
    entries = await backend.list_dir("nope")
    assert entries == []


async def test_get_archived_dates(backend):
    """get_archived_dates should walk the YYYY/MM/DD tree."""
    # Create archive structure: nz/wgn/2026/03/01/readings.parquet
    await backend.store("nz/wgn/2026/03/01/readings.parquet", b"parquet1")
    await backend.store("nz/wgn/2026/03/02/readings.parquet", b"parquet2")
    await backend.store("nz/wgn/2026/02/28/readings.parquet", b"parquet3")

    dates = await backend.get_archived_dates("nz", "wgn")
    assert dates == {"2026-03-01", "2026-03-02", "2026-02-28"}


async def test_get_archived_dates_empty(backend):
    """get_archived_dates returns empty set for non-existent path."""
    dates = await backend.get_archived_dates("xx", "yyy")
    assert dates == set()


async def test_store_creates_parents(backend):
    """Store should create intermediate directories."""
    await backend.store("a/b/c/d/file.txt", b"deep")
    result = await backend.retrieve("a/b/c/d/file.txt")
    assert result == b"deep"
