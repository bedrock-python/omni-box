"""Unit tests for event converters."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from omni_box.core.converters.event import (
    EnvelopeEventConverter,
    RawEventConverter,
    SchemaVersionedConverter,
)
from omni_box.core.models.entities import OutboxEvent

pytestmark = pytest.mark.unit


def _make_event(
    payload: dict | None = None,
    event_type: str = "order.created",
    schema_version: str | None = None,
    trace_id: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> OutboxEvent:
    created = datetime.now(UTC)
    return OutboxEvent(
        id=uuid4(),
        aggregate_type="Order",
        aggregate_id=uuid4(),
        event_type=event_type,
        topic="orders",
        partition_key="key-1",
        payload=payload or {"order_id": "123"},
        headers=None,
        trace_id=trace_id,
        correlation_id=correlation_id,
        causation_id=causation_id,
        schema_version=schema_version,
        created_at=created,
        scheduled_at=created,
    )


def test__raw_event_converter__event__returns_payload_unchanged() -> None:
    # Arrange
    payload = {"a": 1, "b": "two"}
    event = _make_event(payload=payload)
    converter = RawEventConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result == payload


def test__schema_versioned_converter__no_schema_version__includes_none_and_payload() -> None:
    # Arrange
    payload = {"x": 42}
    event = _make_event(payload=payload, schema_version=None)
    converter = SchemaVersionedConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result["schema_version"] is None
    assert result["payload"] == payload


def test__schema_versioned_converter__event_with_schema_version__preserves_version() -> None:
    # Arrange
    event = _make_event(payload={}, schema_version="2.1")
    converter = SchemaVersionedConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result["schema_version"] == "2.1"


def test__envelope_event_converter__full_event__includes_all_envelope_fields() -> None:
    # Arrange
    agg_id = uuid4()
    created = datetime.now(UTC)
    event = OutboxEvent(
        id=uuid4(),
        aggregate_type="Order",
        aggregate_id=agg_id,
        event_type="order.created",
        topic="orders",
        partition_key="pk",
        payload={"total": 99},
        headers=None,
        trace_id="trace-1",
        correlation_id="corr-1",
        causation_id="caus-1",
        schema_version="1.0",
        created_at=created,
        scheduled_at=created,
    )
    converter = EnvelopeEventConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result["schema_version"] == "1.0"
    assert result["event_type"] == "order.created"
    assert result["aggregate_type"] == "Order"
    assert result["aggregate_id"] == str(agg_id)
    assert result["payload"] == {"total": 99}
    assert result["trace_id"] == "trace-1"
    assert result["correlation_id"] == "corr-1"
    assert result["causation_id"] == "caus-1"
    assert result["timestamp"] == created.isoformat()


def test__envelope_event_converter__optional_none_fields__omits_them_from_result() -> None:
    # Arrange
    event = _make_event(trace_id=None, correlation_id=None, causation_id=None)
    converter = EnvelopeEventConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert "trace_id" not in result
    assert "correlation_id" not in result
    assert "causation_id" not in result
    assert "schema_version" in result
    assert "payload" in result
    assert "timestamp" in result
