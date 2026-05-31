from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from ..models.entities import BaseEvent
    from ..models.types import EventFailureUpdate


class EventHandlerStatus(StrEnum):
    """Semantic status of event processing result."""

    COMPLETED = "completed"
    """Handler successfully processed the event."""

    STALE = "stale"
    """Handler ignored the event because it is outdated (revision check)."""

    SKIPPED = "skipped"
    """Handler explicitly chose to skip this event (e.g. business logic)."""

    FAILED = "failed"
    """Handler failed with an exception or explicit error."""

    RETRY = "retry"
    """Handler failed but requested a retry."""


@dataclass(frozen=True, slots=True)
class EventHandlerResult:
    """Explicit outcome of a single event handler."""

    success: bool
    processed: bool = True
    status: EventHandlerStatus | str | None = None
    error_message: str | None = None
    count_as_attempt: bool = True
    next_retry_at: datetime | None = None


def coerce_handler_outcome(raw: EventHandlerResult | None) -> EventHandlerResult:
    """Treat ``None`` as successful completion."""
    if raw is None:
        return EventHandlerResult(success=True, status=EventHandlerStatus.COMPLETED)
    return raw


def handler_completed(status: EventHandlerStatus | str = EventHandlerStatus.COMPLETED) -> EventHandlerResult:
    """Explicit successful completion."""
    return EventHandlerResult(success=True, status=status)


def handler_skipped(status: EventHandlerStatus | str = EventHandlerStatus.SKIPPED) -> EventHandlerResult:
    """Signal that the handler chose not to process this event."""
    return EventHandlerResult(success=False, processed=False, count_as_attempt=False, status=status)


def handler_retry(
    message: str,
    *,
    count_as_attempt: bool = True,
    next_retry_at: datetime | None = None,
    status: EventHandlerStatus | str = EventHandlerStatus.RETRY,
) -> EventHandlerResult:
    """Schedule retry with an error message."""
    return EventHandlerResult(
        success=False,
        error_message=message,
        count_as_attempt=count_as_attempt,
        next_retry_at=next_retry_at,
        status=status,
    )


@dataclass(frozen=True, slots=True)
class BatchProcessingResult[T: BaseEvent]:
    """Results of batch event processing."""

    processed_event_ids: list[UUID] = field(default_factory=list)
    failed_counted: list[EventFailureUpdate] = field(default_factory=list)
    failed_noncounted: list[EventFailureUpdate] = field(default_factory=list)
    remaining_event_ids: set[UUID] = field(default_factory=set)
    commit_failed: bool = False
