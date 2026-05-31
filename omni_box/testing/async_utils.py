"""Outbox event testing utilities."""

from __future__ import annotations

from uuid import UUID

from ..core.models.entities import OutboxEvent
from ..core.models.enums import EventStatus
from ..core.protocols import OutboxEventRepository


async def assert_outbox_event_created(
    repository: OutboxEventRepository,
    aggregate_id: UUID,
    event_type: str,
    status: EventStatus = EventStatus.PENDING,
) -> OutboxEvent:
    """Assert that an outbox event was created and return it."""
    events = await repository.fetch_pending(limit=1000)

    event = next(
        (e for e in events if e.aggregate_id == aggregate_id and e.event_type == event_type),
        None,
    )

    assert event is not None, f"Outbox event '{event_type}' not found for aggregate {aggregate_id}"
    assert event.status == status, f"Expected status {status}, got {event.status}"

    return event
