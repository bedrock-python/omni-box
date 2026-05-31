"""Base repository protocols."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, TypedDict, Unpack, runtime_checkable
from uuid import UUID

from ..models.entities import BaseEvent, InboxEvent, OutboxEvent
from ..models.types import PositiveInt


class FetchFilters(TypedDict, total=False):
    """Common filters for event fetching."""

    source: str | list[str] | None
    topic: str | list[str] | None
    aggregate_type: str | list[str] | None
    aggregate_id: UUID | list[UUID] | None


@dataclass(frozen=True, slots=True)
class RepositoryCapabilities:
    """Flags for optional repository capabilities."""

    supports_bulk: bool = False
    supports_distributed_locking: bool = False
    supports_retention: bool = False


@runtime_checkable
class EventRepository[T: BaseEvent](Protocol):
    """Generic base repository protocol for both Outbox and Inbox."""

    @property
    def capabilities(self) -> RepositoryCapabilities:
        """Report optional features supported by this repository instance."""
        ...

    async def create(self, event: T) -> T:
        """Persist a new event."""
        ...

    async def get_by_id(self, event_id: UUID) -> T | None:
        """Fetch an event by its primary identifier."""
        ...

    async def fetch_pending(self, limit: PositiveInt, **filters: Unpack[FetchFilters]) -> list[T]:
        """Fetch pending events that are ready for processing."""
        ...

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        """Manually mark an event as being processed (locking)."""
        ...

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        """Mark an event as COMPLETED."""
        ...

    async def mark_failed(
        self, event_id: UUID, error: str, worker_id: str, next_retry_at: datetime | None, count_as_attempt: bool = True
    ) -> None:
        """Mark an event as FAILED or schedule a retry."""
        ...


@runtime_checkable
class OutboxEventRepository(EventRepository[OutboxEvent], Protocol):
    """Specific protocol for Outbox events."""


@runtime_checkable
class InboxEventRepository(EventRepository[InboxEvent], Protocol):
    """Specific protocol for Inbox events."""

    async def get_by_message_id(self, message_id: str, consumer_group: str) -> InboxEvent | None:
        """Get inbox event by external message identifier and consumer group."""
        ...

    async def exists(self, message_id: str, consumer_group: str) -> bool:
        """Check if message already exists in inbox."""
        ...

    async def has_completed_sibling_for_inbox_key(
        self, message_id: str, consumer_group: str, exclude_event_id: UUID
    ) -> bool:
        """True if another row with the same (message_id, consumer_group) is completed."""
        ...
