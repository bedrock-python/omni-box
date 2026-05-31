"""Unit tests for ``omni_box.core.converters.event``."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from omni_box.core.converters.event import (
    EnvelopeEventConverter,
    EventConverter,
    RawEventConverter,
    SchemaVersionedConverter,
)
from omni_box.core.models.entities import OutboxEvent
from omni_box.utils.datetime import utc_now

pytestmark = pytest.mark.unit


def _build_event(**overrides: Any) -> OutboxEvent:
    base: dict[str, Any] = {
        "aggregate_type": "user",
        "aggregate_id": uuid4(),
        "event_type": "user.created",
        "topic": "users",
        "partition_key": "k1",
        "payload": {"id": "u1"},
        "created_at": utc_now(),
    }
    base.update(overrides)
    return OutboxEvent(**base)


# ---------- Protocol ----------


@pytest.mark.parametrize(
    "converter",
    [RawEventConverter(), SchemaVersionedConverter(), EnvelopeEventConverter()],
    ids=["raw", "schema-versioned", "envelope"],
)
def test__event_converter_protocol__concrete_implementations__are_recognized(converter: EventConverter) -> None:
    # Act / Assert
    assert isinstance(converter, EventConverter)


# ---------- RawEventConverter ----------


def test__raw_event_converter__any_event__returns_payload_unchanged() -> None:
    # Arrange
    event = _build_event(payload={"hello": "world"})
    converter = RawEventConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result == {"hello": "world"}
    assert result is event.payload


# ---------- SchemaVersionedConverter ----------


def test__schema_versioned_converter__with_schema_version__wraps_payload() -> None:
    # Arrange
    event = _build_event(schema_version="1.2.3")
    converter = SchemaVersionedConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result == {"schema_version": "1.2.3", "payload": event.payload}


def test__schema_versioned_converter__without_schema_version__keeps_none() -> None:
    # Arrange
    event = _build_event()
    converter = SchemaVersionedConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result["schema_version"] is None


# ---------- EnvelopeEventConverter ----------


def test__envelope_converter__full_event__includes_tracing_fields() -> None:
    # Arrange
    event = _build_event(
        schema_version="1.2.3",
        trace_id="t1",
        correlation_id="c1",
        causation_id="ca1",
    )
    converter = EnvelopeEventConverter(default_schema_version="1.0.0")

    # Act
    result = converter.convert(event)

    # Assert
    assert result["schema_version"] == "1.2.3"
    assert result["event_type"] == "user.created"
    assert result["aggregate_type"] == "user"
    assert result["aggregate_id"] == str(event.aggregate_id)
    assert result["payload"] == event.payload
    assert result["timestamp"] == event.created_at.isoformat()
    assert result["trace_id"] == "t1"
    assert result["correlation_id"] == "c1"
    assert result["causation_id"] == "ca1"


def test__envelope_converter__no_schema_version__uses_default() -> None:
    # Arrange
    default_version = "2.0.0"
    event = _build_event()
    converter = EnvelopeEventConverter(default_schema_version=default_version)

    # Act
    result = converter.convert(event)

    # Assert
    assert result["schema_version"] == default_version


def test__envelope_converter__no_tracing__omits_optional_keys() -> None:
    # Arrange
    event = _build_event()
    converter = EnvelopeEventConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert "trace_id" not in result
    assert "correlation_id" not in result
    assert "causation_id" not in result


def test__envelope_converter__default_default_schema_version__is_1_0_0() -> None:
    # Arrange
    event = _build_event()
    converter = EnvelopeEventConverter()

    # Act
    result = converter.convert(event)

    # Assert
    assert result["schema_version"] == "1.0.0"
