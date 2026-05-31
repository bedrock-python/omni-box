from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

import structlog

from ..models.entities import BaseEvent
from ..models.types import EventFailureUpdate
from ..protocols.metrics import ProcessingMetrics
from ..protocols.repository import EventRepository

logger = structlog.get_logger(__name__)


@dataclass
class ProcessingContext[T: BaseEvent]:
    """Shared state and result tracker for event processing pipelines."""

    repo: EventRepository[T]
    """The repository being used for fetching and committing events."""
    worker_id: str
    """Unique identifier of the worker instance currently processing events."""
    metrics: ProcessingMetrics | None = None
    """Optional metrics collector for recording processing stats."""

    # Results of processing
    completed_ids: list[UUID] = field(default_factory=list)
    """List of event IDs that were successfully processed in this batch."""
    failed_counted: list[EventFailureUpdate] = field(default_factory=list)
    """Failures that increment the retry counter and count toward ``max_attempts``."""
    failed_noncounted: list[EventFailureUpdate] = field(default_factory=list)
    """Transient failures that trigger a retry but do not increment the counter."""
    skipped_ids: set[UUID] = field(default_factory=set)
    """Set of event IDs that were filtered out or skipped by any step."""
    statuses: dict[UUID, str] = field(default_factory=dict)
    """Semantic status of event processing result (e.g. 'completed', 'stale', 'failed')."""

    # Additional data for steps
    extra: dict[str, object] = field(default_factory=dict)

    def mark_completed(self, event_id: UUID, status: str | None = None) -> None:
        """Mark an event as successfully completed in this processing batch."""
        self.completed_ids.append(event_id)
        if status:
            self.statuses[event_id] = status

    def mark_failed(
        self,
        event_id: UUID,
        error: str | None,
        count_as_attempt: bool = True,
        next_retry_at: datetime | None = None,
        status: str | None = None,
    ) -> None:
        """Mark an event as failed and schedule a retry."""
        failure = EventFailureUpdate(event_id, error or "unknown error", next_retry_at)
        if count_as_attempt:
            self.failed_counted.append(failure)
        else:
            self.failed_noncounted.append(failure)
        if status:
            self.statuses[event_id] = status

    def mark_skipped(self, event_id: UUID, reason: str, status: str | None = None) -> None:
        """Mark an event as skipped by a processing step."""
        self.skipped_ids.add(event_id)
        if status:
            self.statuses[event_id] = status
        logger.info("Event skipped", event_id=str(event_id), reason=reason)

    @property
    def failed_ids(self) -> set[UUID]:
        """All failed event IDs in this batch."""
        return {f.event_id for f in self.failed_counted} | {f.event_id for f in self.failed_noncounted}

    def get_failure(self, event_id: UUID) -> EventFailureUpdate | None:
        """Find failure information for a specific event ID."""
        for f in self.failed_counted:
            if f.event_id == event_id:
                return f
        for f in self.failed_noncounted:
            if f.event_id == event_id:
                return f
        return None
