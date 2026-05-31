from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Unpack, cast

from ...protocols.features import SupportsDistributedLocking

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ...protocols.repository import EventRepository, FetchFilters

from ...constants import DEFAULT_LEASE_TIMEOUT_SECONDS


class FetchStrategy[T: BaseEvent](Protocol):
    """Protocol for fetching events from storage."""

    async def fetch(
        self,
        repo: EventRepository[T],
        batch_size: int,
        worker_id: str,
        **filters: Unpack[FetchFilters],
    ) -> list[T]:
        """Fetch pending events for processing."""
        ...


class OptimisticLockingFetchStrategy[T: BaseEvent]:
    """Fetch events and mark them as processing individually.

    Fallback strategy for repositories that do not implement
    ``SupportsDistributedLocking``. Under concurrent workers it can produce
    thundering-herd behaviour (every worker fetches the same pending batch and
    races on ``mark_processing``). Prefer ``DistributedLockingFetchStrategy``
    whenever the repository supports it.
    """

    async def fetch(
        self,
        repo: EventRepository[T],
        batch_size: int,
        worker_id: str,
        **filters: Unpack[FetchFilters],
    ) -> list[T]:
        """Fetch pending and mark individually."""
        pending = await repo.fetch_pending(limit=batch_size, **filters)
        locked = []
        for event in pending:
            if await repo.mark_processing(event.id, worker_id):
                locked.append(event)
        return locked


class DistributedLockingFetchStrategy[T: BaseEvent]:
    """Fetch events using atomic storage-level locking."""

    def __init__(self, ttl: int = DEFAULT_LEASE_TIMEOUT_SECONDS) -> None:
        self.ttl = ttl

    async def fetch(
        self,
        repo: EventRepository[T],
        batch_size: int,
        worker_id: str,
        **filters: Unpack[FetchFilters],
    ) -> list[T]:
        """Atomically fetch and lock a batch of events."""
        locking_repo = cast(SupportsDistributedLocking[T], repo)
        return await locking_repo.fetch_and_lock_pending(
            limit=batch_size,
            worker_id=worker_id,
            ttl=self.ttl,
            **filters,
        )


class FilteredFetchStrategy[T: BaseEvent]:
    """Strategy that applies fixed filters to all fetch operations."""

    def __init__(self, sources: list[str] | None = None, ttl: int = DEFAULT_LEASE_TIMEOUT_SECONDS) -> None:
        self._sources = sources
        self._ttl = ttl

    async def fetch(
        self,
        repo: EventRepository[T],
        batch_size: int,
        worker_id: str,
        **filters: Unpack[FetchFilters],
    ) -> list[T]:
        """Fetch events with additional sources filtering."""
        combined_filters = filters.copy()
        if self._sources:
            combined_filters["source"] = self._sources

        if isinstance(repo, SupportsDistributedLocking):
            return await repo.fetch_and_lock_pending(
                limit=batch_size,
                worker_id=worker_id,
                ttl=self._ttl,
                **combined_filters,
            )

        pending = await repo.fetch_pending(limit=batch_size, **combined_filters)
        locked = []
        for event in pending:
            if await repo.mark_processing(event.id, worker_id):
                locked.append(event)
        return locked
