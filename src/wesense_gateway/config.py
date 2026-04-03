"""Gateway configuration via environment variables."""

from pydantic_settings import BaseSettings


class GatewayConfig(BaseSettings):
    """All gateway configuration, loaded from environment variables."""

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "wesense"
    clickhouse_password: str = ""
    clickhouse_database: str = "wesense"
    clickhouse_table: str = "sensor_readings"
    clickhouse_batch_size: int = 100
    clickhouse_flush_interval: float = 10.0

    # Storage backend (archive replicator)
    archive_data_dir: str = "data/archives"
    archive_replicator_url: str = "http://localhost:4400"

    # Archive scheduler
    archive_interval_hours: float = 6.0
    archive_start_date: str = ""

    # Signing / trust
    key_dir: str = "data/keys"
    trust_file: str = "data/trust_list.json"

    # TLS
    tls_enabled: bool = False
    tls_ca_certfile: str = ""

    # Server
    log_level: str = "INFO"

    model_config = {"env_prefix": ""}
