"""Pydantic models for reading ingestion."""

from pydantic import BaseModel


class ReadingIn(BaseModel):
    """A single sensor reading submitted by an ingester."""

    timestamp: int  # Unix epoch seconds
    device_id: str
    data_source: str
    network_source: str = ""
    ingestion_node_id: str = ""
    reading_type: str
    value: float
    unit: str = ""
    latitude: float | None = None
    longitude: float | None = None
    altitude: float | None = None
    board_model: str = ""
    sensor_model: str = ""
    deployment_type: str = ""
    deployment_type_source: str = ""
    transport_type: str = ""
    deployment_location: str = ""
    node_name: str = ""
    node_info: str = ""
    node_info_url: str = ""
    signature: str = ""
    ingester_id: str = ""
    key_version: int = 0


class ReadingBatch(BaseModel):
    """Batch of readings for ingestion."""

    readings: list[ReadingIn]


class ProcessResult(BaseModel):
    """Result of processing a batch of readings."""

    accepted: int = 0
    duplicates: int = 0
    errors: int = 0
