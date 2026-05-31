"""Feature-based protocols for storage capabilities."""

from __future__ import annotations

from typing import Protocol, Unpack, runtime_checkable
from uuid import UUID

from ..models.entities import BaseEvent
from ..models.types import EventFailureUpdate
from .repository import FetchFilters


@runtime_checkable
class SupportsBulkOperations[T: BaseEvent](Protocol):
    """Storage supports efficient bulk updates."""

    async def bulk_mark_completed(self, event_ids: list[UUID], worker_id: str) -> int:
        """Mark multiple events as completed in a single operation."""
        ...

    async def bulk_create(self, events: list[T]) -> list[T]:
        """Create multiple events in a single operation."""
        ...

    async def bulk_mark_failed(
        self, failures: list[EventFailureUpdate], worker_id: str, count_as_attempt: bool = True
    ) -> int:
        """Mark multiple events as failed in a single operation."""
        ...

    async def bulk_release_locks(self, event_ids: list[UUID], worker_id: str) -> int:
        """Release locks for multiple events in a single operation."""
        ...


@runtime_checkable
class SupportsDistributedLocking[T: BaseEvent](Protocol):
    """Storage supports distributed locking."""

    async def fetch_and_lock_pending(
        self, limit: int, worker_id: str, ttl: int | None = None, **filters: Unpack[FetchFilters]
    ) -> list[T]:
        """Atomically fetch and lock pending events."""
        ...

    async def refresh_lock(self, event_id: UUID, worker_id: str) -> bool:
        """Refresh the lock on a specific event."""
        ...

    async def release_lock(self, event_id: UUID, worker_id: str) -> bool:
        """Manually release the lock on a specific event."""
        ...

    async def force_unlock(self, event_id: UUID, reason: str) -> bool:
        """Forcefully release a lock regardless of owner, with audit reason."""
        ...


@runtime_checkable
class SupportsRetentionPolicies(Protocol):
    """Storage supports automatic cleanup."""

    async def delete_old_completed(self, retention_days: int, batch_size: int) -> int:
        """Delete events that were completed more than N days ago."""
        ...

    async def release_stale_locks(self, stale_timeout_seconds: int) -> int:
        """Release locks that have exceeded their TTL."""
        ...
