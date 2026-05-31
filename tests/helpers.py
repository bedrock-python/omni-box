"""Test helpers and fakes for omni-box."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.types import EventFailureUpdate
from omni_box.core.protocols import (
    EventPublisher,
    OutboxEventRepository,
    RepositoryCapabilities,
    SupportsBulkOperations,
    SupportsDistributedLocking,
    SupportsRetentionPolicies,
)
from omni_box.core.protocols.repository import EventRepository


def create_fake_event(
    id: UUID | None = None,
    topic: str = "topic.ok",
    partition_key: str = "k1",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    event_type: str = "user.created",
) -> OutboxEvent:
    """Helper to create OutboxEvent for tests."""
    return OutboxEvent(
        id=id or uuid4(),
        aggregate_type="User",
        aggregate_id=uuid4(),
        event_type=event_type,
        topic=topic,
        partition_key=partition_key,
        payload=payload or {"a": 1},
        headers=headers,
    )


class FakeOutboxStore(
    OutboxEventRepository,
    SupportsBulkOperations[OutboxEvent],
    SupportsDistributedLocking[OutboxEvent],
    SupportsRetentionPolicies,
):
    """In-memory outbox store for job tests."""

    def __init__(
        self,
        pending_events: list[OutboxEvent] | None = None,
        stale_release_count: int = 0,
        cleanup_delete_count: int = 0,
    ) -> None:
        self.pending_events = pending_events or []
        self.stale_release_count = stale_release_count
        self.cleanup_delete_count = cleanup_delete_count
        self.fetch_calls: list[tuple[int, str, int | None]] = []
        self.published_ids: list[UUID] = []
        self.failed_calls: list[tuple[UUID, str, datetime | None]] = []
        self.release_calls: list[int] = []
        self.cleanup_calls: list[tuple[int, int]] = []
        self.lock_refresh_calls: list[tuple[UUID, str]] = []
        self.lock_release_calls: list[tuple[UUID, str]] = []

    @property
    def capabilities(self) -> RepositoryCapabilities:
        return RepositoryCapabilities(
            supports_bulk=True,
            supports_distributed_locking=True,
            supports_retention=True,
        )

    async def create(self, event: OutboxEvent) -> OutboxEvent:
        return event

    async def bulk_create(self, events: list[OutboxEvent]) -> list[OutboxEvent]:
        return events

    async def get_by_id(self, event_id: UUID) -> OutboxEvent | None:
        return next((e for e in self.pending_events if e.id == event_id), None)

    async def fetch_pending(self, limit: int, **filters: Any) -> list[OutboxEvent]:
        return self.pending_events[:limit]

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def fetch_and_lock_pending(
        self, limit: int, worker_id: str, ttl: int | None = None, **filters: Any
    ) -> list[OutboxEvent]:
        self.fetch_calls.append((limit, worker_id, ttl))
        batch = self.pending_events[:limit]
        return batch

    async def refresh_lock(self, event_id: UUID, worker_id: str) -> bool:
        self.lock_refresh_calls.append((event_id, worker_id))
        return True

    async def release_lock(self, event_id: UUID, worker_id: str) -> bool:
        self.lock_release_calls.append((event_id, worker_id))
        return True

    async def bulk_release_locks(self, event_ids: list[UUID], worker_id: str) -> int:
        count = 0
        for event_id in event_ids:
            self.lock_release_calls.append((event_id, worker_id))
            count += 1
        return count

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        self.published_ids.append(event_id)

    async def bulk_mark_completed(self, event_ids: list[UUID], worker_id: str) -> int:
        count = 0
        for event_id in event_ids:
            self.published_ids.append(event_id)
            count += 1
        return count

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None,
        count_as_attempt: bool = True,
    ) -> None:
        self.failed_calls.append((event_id, error, next_retry_at))

    async def bulk_mark_failed(
        self, failures: list[EventFailureUpdate], worker_id: str, count_as_attempt: bool = True
    ) -> int:
        count = 0
        for f in failures:
            self.failed_calls.append((f.event_id, f.error, f.next_retry_at))
            count += 1
        return count

    async def force_unlock(self, event_id: UUID, reason: str) -> bool:
        return True

    async def release_stale_locks(self, timeout_seconds: int) -> int:
        self.release_calls.append(timeout_seconds)
        return self.stale_release_count

    async def delete_old_completed(self, retention_days: int, batch_size: int) -> int:
        self.cleanup_calls.append((retention_days, batch_size))
        count = self.cleanup_delete_count
        self.cleanup_delete_count = 0
        return count


class FakeEventPublisher(EventPublisher):
    """Fake EventPublisher for testing the publish(event) path."""

    def __init__(self, fail_for_topic: str | None = None) -> None:
        self.fail_for_topic = fail_for_topic
        self.published_events: list[OutboxEvent] = []

    async def publish(self, event: OutboxEvent, repo: EventRepository[OutboxEvent]) -> None:
        self.published_events.append(event)
        if self.fail_for_topic and event.topic == self.fail_for_topic:
            raise RuntimeError("publish failed")


class FakeLogger:
    """Minimal structured logger stub for testing."""

    def __init__(self) -> None:
        self.debug_calls: list[tuple[str, dict[str, object]]] = []
        self.info_calls: list[tuple[str, dict[str, object]]] = []
        self.warning_calls: list[tuple[str, dict[str, object]]] = []
        self.error_calls: list[tuple[str, dict[str, object]]] = []
        self.exception_calls: list[tuple[str, dict[str, object]]] = []

    def debug(self, event: str, **kwargs: object) -> None:
        self.debug_calls.append((event, kwargs))

    def info(self, event: str, **kwargs: object) -> None:
        self.info_calls.append((event, kwargs))

    def warning(self, event: str, **kwargs: object) -> None:
        self.warning_calls.append((event, kwargs))

    def error(self, event: str, **kwargs: object) -> None:
        self.error_calls.append((event, kwargs))

    def exception(self, event: str, **kwargs: object) -> None:
        self.exception_calls.append((event, kwargs))
        self.error_calls.append((event, kwargs))
