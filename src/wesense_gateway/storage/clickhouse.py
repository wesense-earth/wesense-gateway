"""Async ClickHouse writer with batch-flush pattern."""

import asyncio
import logging
from typing import Any

import clickhouse_connect

from wesense_gateway.config import GatewayConfig

logger = logging.getLogger(__name__)

# 27-column unified schema
CLICKHOUSE_COLUMNS = [
    "timestamp", "device_id", "data_source", "data_source_name",
    "network_source", "ingestion_node_id",
    "reading_type", "value", "unit",
    "latitude", "longitude", "altitude", "geo_country", "geo_subdivision",
    "board_model", "sensor_model", "deployment_type", "deployment_type_source",
    "transport_type", "deployment_location", "node_name", "node_info", "node_info_url",
    "signature", "ingester_id", "key_version", "data_license",
]


class AsyncClickHouseWriter:
    """
    Asyncio-native buffered writer for ClickHouse.

    Rows accumulate in an in-memory buffer and flush either when the batch
    size is reached or on a periodic interval. On flush failure, rows are
    returned to the buffer for retry.
    """

    def __init__(self, config: GatewayConfig):
        self._config = config
        self._buffer: list[tuple] = []
        self._lock = asyncio.Lock()
        self._client: Any = None
        self._flush_task: asyncio.Task | None = None
        self._total_written = 0
        self._total_failed = 0

    async def start(self) -> None:
        """Connect to ClickHouse and start periodic flush."""
        self._connect()
        self._flush_task = asyncio.create_task(self._periodic_flush())

    def _connect(self) -> None:
        """Create ClickHouse client."""
        try:
            ch_kwargs = dict(
                host=self._config.clickhouse_host,
                port=8443 if self._config.tls_enabled else self._config.clickhouse_port,
                username=self._config.clickhouse_user,
                password=self._config.clickhouse_password,
                database=self._config.clickhouse_database,
            )
            if self._config.tls_enabled:
                ch_kwargs["secure"] = True
                if self._config.tls_ca_certfile:
                    ch_kwargs["verify"] = True
                    ch_kwargs["ca_cert"] = self._config.tls_ca_certfile
                else:
                    ch_kwargs["verify"] = False
            self._client = clickhouse_connect.get_client(**ch_kwargs)
            logger.info(
                "Connected to ClickHouse at %s:%d/%s%s",
                self._config.clickhouse_host,
                ch_kwargs["port"],
                self._config.clickhouse_database,
                " (TLS)" if self._config.tls_enabled else "",
            )
        except Exception as e:
            logger.error("Failed to connect to ClickHouse: %s", e)
            self._client = None

    async def _periodic_flush(self) -> None:
        """Background task: flush buffer on interval."""
        while True:
            await asyncio.sleep(self._config.clickhouse_flush_interval)
            await self.flush()

    async def add(self, row: tuple) -> None:
        """Add a row to the buffer. Flushes if batch_size is reached."""
        async with self._lock:
            self._buffer.append(row)
            size = len(self._buffer)

        if size >= self._config.clickhouse_batch_size:
            await self.flush()

    async def flush(self) -> None:
        """Flush the buffer to ClickHouse."""
        async with self._lock:
            if not self._buffer or not self._client:
                return
            rows = self._buffer
            self._buffer = []

        table = f"{self._config.clickhouse_database}.{self._config.clickhouse_table}"
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None, self._client.insert, table, rows, CLICKHOUSE_COLUMNS,
            )
            self._total_written += len(rows)
            logger.info("Flushed %d rows to ClickHouse", len(rows))
        except Exception as e:
            logger.error("ClickHouse flush failed (%d rows): %s", len(rows), e)
            self._total_failed += len(rows)
            async with self._lock:
                self._buffer = rows + self._buffer

    def get_stats(self) -> dict[str, int]:
        """Return write statistics (safe to call without await)."""
        return {
            "buffer_size": len(self._buffer),
            "total_written": self._total_written,
            "total_failed": self._total_failed,
        }

    async def close(self) -> None:
        """Flush remaining rows and cancel periodic task."""
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        await self.flush()
