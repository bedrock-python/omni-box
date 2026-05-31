"""Consumer protocols for Inbox integrations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol

from pydantic import JsonValue


@dataclass(frozen=True, slots=True)
class EnvelopeData:
    """Unwrapped message payload and extracted metadata from an envelope."""

    payload: dict[str, JsonValue]
    trace_id: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    schema_version: str | None = None


class EnvelopeParser(Protocol):
    """Protocol for unwrapping nested payloads and extracting metadata."""

    def parse(self, raw_payload: dict[str, JsonValue], headers: dict[str, str] | None) -> EnvelopeData:
        """Unwrap payload and extract metadata."""
        ...


class AckHandle(ABC):
    """Acknowledgement handle for broker commit/ack operations."""

    @abstractmethod
    async def commit(self) -> None:
        """Commit/acknowledge the consumed message."""
        ...


class NullAckHandle(AckHandle):
    """No-op ack handle."""

    async def commit(self) -> None:
        return


@dataclass(frozen=True, slots=True)
class ConsumedMessage:
    """Normalized consumed message passed to Inbox runner."""

    message_id: str
    source: str
    event_type: str
    payload: dict[str, JsonValue]
    headers: dict[str, str] | None = None
    trace_id: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    schema_version: str | None = None
    ack_handle: AckHandle | None = None
    raw_message: object = None


class EventConsumer(ABC):
    """Abstract event consumer."""

    @abstractmethod
    async def start(self) -> None:
        """Start consumer lifecycle resources."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop consumer lifecycle resources."""
        ...

    @abstractmethod
    async def getone(self) -> ConsumedMessage:
        """Fetch one message from broker."""
        ...
