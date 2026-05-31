"""Event converters: OutboxEvent -> message payload dict."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import JsonValue

from ..models.entities import OutboxEvent


@runtime_checkable
class EventConverter(Protocol):
    """Protocol for converting OutboxEvent to message payload (body) dict."""

    def convert(self, event: OutboxEvent) -> dict[str, JsonValue]:
        """Convert OutboxEvent to message payload for broker.

        Returns:
            JSON-serializable dict to be sent as message body/value.
        """
        ...


class RawEventConverter:
    """Passthrough: returns event.payload as-is."""

    def convert(self, event: OutboxEvent) -> dict[str, JsonValue]:
        return event.payload


class SchemaVersionedConverter:
    """Minimal envelope: schema_version + payload. No default for schema_version."""

    def convert(self, event: OutboxEvent) -> dict[str, JsonValue]:
        return {
            "schema_version": event.schema_version,
            "payload": event.payload,
        }


class EnvelopeEventConverter:
    """Full envelope: schema_version, event_type, aggregate info, payload, tracing."""

    def __init__(self, default_schema_version: str = "1.0.0") -> None:
        self._default_schema_version = default_schema_version

    def convert(self, event: OutboxEvent) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "schema_version": event.schema_version or self._default_schema_version,
            "event_type": event.event_type,
            "aggregate_type": event.aggregate_type,
            "aggregate_id": str(event.aggregate_id),
            "payload": event.payload,
            "timestamp": event.created_at.isoformat(),
        }
        if event.trace_id is not None:
            result["trace_id"] = event.trace_id
        if event.correlation_id is not None:
            result["correlation_id"] = event.correlation_id
        if event.causation_id is not None:
            result["causation_id"] = event.causation_id
        return result
