"""Pydantic models for reading ingestion."""

from pydantic import BaseModel, field_validator


class ReadingIn(BaseModel):
    """A single sensor reading submitted by an ingester."""

    timestamp: int  # Unix epoch seconds
    device_id: str
    data_source: str
    data_source_name: str = ""
    network_source: str = ""
    ingestion_node_id: str = ""
    reading_type: str
    reading_type_name: str = ""
    value: float
    unit: str = ""
    latitude: float | None = None
    longitude: float | None = None
    altitude: float | None = None
    geo_country: str = ""
    geo_subdivision: str = ""
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
    data_license: str = "CC-BY-4.0"
    signing_payload_version: int = 1
    public_key: str = ""

    @field_validator(
        "data_source_name", "network_source", "ingestion_node_id", "unit", "geo_country",
        "geo_subdivision", "board_model", "sensor_model", "deployment_type",
        "deployment_type_source", "transport_type", "deployment_location",
        "node_name", "node_info", "node_info_url", "signature", "ingester_id", "data_license",
        "reading_type_name", "public_key",
        mode="before",
    )
    @classmethod
    def none_to_empty_string(cls, v):
        """Coerce None to empty string for optional string fields."""
        return v if v is not None else ""


class ReadingBatch(BaseModel):
    """Batch of readings for ingestion."""

    readings: list[ReadingIn]


class ProcessResult(BaseModel):
    """Result of processing a batch of readings."""

    accepted: int = 0
    rejected: int = 0
    duplicates: int = 0
    errors: int = 0
