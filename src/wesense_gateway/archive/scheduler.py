"""Background archive scheduler — runs archive cycles on an interval."""

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone

import clickhouse_connect

from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig
from wesense_ingester.signing.trust import TrustStore

from wesense_gateway.archive.builder import ParquetArchiveBuilder
from wesense_gateway.backends.base import StorageBackend
from wesense_gateway.config import GatewayConfig

logger = logging.getLogger(__name__)


class ArchiveScheduler:
    """
    Background asyncio task that runs archive cycles.

    Each cycle:
    1. Query ClickHouse for distinct (country, subdivision) pairs with signed readings
    2. Subtract already-archived dates via backend
    3. For each gap: build and store Parquet archive
    """

    def __init__(self, config: GatewayConfig, backend: StorageBackend):
        self._config = config
        self._backend = backend
        self._task: asyncio.Task | None = None
        self._ch_client = None
        self._trust_store: TrustStore | None = None
        self._key_manager: IngesterKeyManager | None = None
        self._last_cycle: str = ""
        self._total_archived: int = 0

    async def start(self) -> None:
        """Initialize resources and start the background loop."""
        self._ch_client = clickhouse_connect.get_client(
            host=self._config.clickhouse_host,
            port=self._config.clickhouse_port,
            username=self._config.clickhouse_user,
            password=self._config.clickhouse_password,
            database=self._config.clickhouse_database,
        )

        self._trust_store = TrustStore(self._config.trust_file)

        self._key_manager = IngesterKeyManager(
            KeyConfig(key_dir=self._config.key_dir)
        )
        self._key_manager.load_or_generate()

        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Archive scheduler started (interval=%sh)", self._config.archive_interval_hours
        )

    async def _loop(self) -> None:
        """Run archive cycles on interval."""
        while True:
            try:
                await self._run_cycle()
            except Exception as e:
                logger.error("Archive cycle failed: %s", e, exc_info=True)

            await asyncio.sleep(self._config.archive_interval_hours * 3600)

    async def _run_cycle(self) -> None:
        """Execute a single gap-aware archive cycle."""
        logger.info("Starting archive cycle...")
        self._last_cycle = datetime.now(timezone.utc).isoformat()

        # Don't archive today — incomplete data would change readings_hash
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

        start_date = None
        if self._config.archive_start_date:
            start_date = date.fromisoformat(self._config.archive_start_date)

        # Get all (country, subdivision) pairs with signed data
        loop = asyncio.get_event_loop()
        regions = await loop.run_in_executor(
            None, self._get_regions_with_data, start_date, yesterday
        )
        if not regions:
            logger.info("No regions with signed readings to archive")
            return

        logger.info("Regions with signed data: %d", len(regions))

        builder = ParquetArchiveBuilder(
            ch_client=self._ch_client,
            trust_store=self._trust_store,
            key_manager=self._key_manager,
            backend=self._backend,
            orbitdb_url=self._config.orbitdb_url,
        )

        for country, subdivision in regions:
            data_dates = await loop.run_in_executor(
                None, self._get_dates_with_data, country, subdivision, start_date, yesterday
            )
            if not data_dates:
                continue

            archived = await self._backend.get_archived_dates(country, subdivision)
            missing = sorted(data_dates - archived)

            if not missing:
                logger.info("%s/%s: fully archived (%d days)", country, subdivision, len(data_dates))
                continue

            logger.info(
                "%s/%s: %d to archive of %d total days",
                country, subdivision, len(missing), len(data_dates),
            )

            for i, period in enumerate(missing, 1):
                try:
                    manifest = await builder.archive_period(period, country, subdivision)
                    if manifest:
                        self._total_archived += 1
                    logger.info(
                        "%s/%s: archived %d/%d (%s)",
                        country, subdivision, i, len(missing), period,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to archive %s/%s/%s: %s",
                        country, subdivision, period, e, exc_info=True,
                    )

        logger.info("Archive cycle complete")

    def _get_regions_with_data(
        self, start: date | None, end: date
    ) -> list[tuple[str, str]]:
        """Get distinct (country, subdivision) pairs with signed readings."""
        conditions = [
            "signature != ''",
            "toDate(timestamp) <= {end:String}",
        ]
        params = {"end": end.isoformat()}
        if start:
            conditions.append("toDate(timestamp) >= {start:String}")
            params["start"] = start.isoformat()

        query = f"""
            SELECT DISTINCT geo_country, geo_subdivision
            FROM sensor_readings
            WHERE {' AND '.join(conditions)}
              AND geo_country != ''
              AND geo_subdivision != ''
            ORDER BY geo_country, geo_subdivision
        """
        result = self._ch_client.query(query, parameters=params)
        return [(row[0], row[1]) for row in result.result_rows if row[0] and row[1]]

    def _get_dates_with_data(
        self, country: str, subdivision: str, start: date | None, end: date
    ) -> set[str]:
        """Get dates with signed readings for a country/subdivision."""
        conditions = [
            "signature != ''",
            "geo_country = {country:String}",
            "geo_subdivision = {subdivision:String}",
            "toDate(timestamp) <= {end:String}",
        ]
        params = {"country": country, "subdivision": subdivision, "end": end.isoformat()}
        if start:
            conditions.append("toDate(timestamp) >= {start:String}")
            params["start"] = start.isoformat()

        query = f"""
            SELECT DISTINCT toDate(timestamp) as d
            FROM sensor_readings
            WHERE {' AND '.join(conditions)}
            ORDER BY d
        """
        result = self._ch_client.query(query, parameters=params)
        dates = set()
        for row in result.result_rows:
            d = row[0]
            if isinstance(d, date):
                dates.add(d.isoformat())
            else:
                dates.add(str(d))
        return dates

    def get_stats(self) -> dict:
        """Return scheduler statistics."""
        return {
            "last_cycle": self._last_cycle,
            "total_archived": self._total_archived,
            "interval_hours": self._config.archive_interval_hours,
        }

    async def stop(self) -> None:
        """Cancel the background task."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
