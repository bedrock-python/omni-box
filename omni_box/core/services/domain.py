"""Omni-box domain services."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic_core import to_jsonable_python

from ..constants import (
    DEFAULT_HEADER_KEY_MAX_LENGTH,
    DEFAULT_HEADER_VALUE_MAX_LENGTH,
    DEFAULT_HEADERS_MAX_COUNT,
    DEFAULT_LEASE_TIMEOUT_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_PAYLOAD_MAX_BYTES,
    DEFAULT_SCHEDULE_AT_MAX_FUTURE_SECONDS,
    DEFAULT_SCHEDULE_AT_SKEW_SECONDS,
    DEFAULT_TRUNCATION_SUFFIX,
    FORCE_UNLOCK_REASON_MAX_LENGTH,
    LAST_ERROR_MAX_LENGTH,
)
from ..exceptions import (
    EventAlreadyLockedError,
    EventLockedByAnotherWorkerError,
    EventNotLockedError,
    InvalidEventStateError,
)
from ..models.entities import BaseEvent, InboxEvent, OutboxEvent
from ..models.enums import EventStatus

logger = logging.getLogger(__name__)


class OmniBoxDomainService:
    """Standardized way to create and manage events with sensible defaults."""

    def __init__(
        self,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        scheduled_at_skew_seconds: int = DEFAULT_SCHEDULE_AT_SKEW_SECONDS,
        payload_max_bytes: int = DEFAULT_PAYLOAD_MAX_BYTES,
        headers_max_count: int = DEFAULT_HEADERS_MAX_COUNT,
        header_key_max_length: int = DEFAULT_HEADER_KEY_MAX_LENGTH,
        header_value_max_length: int = DEFAULT_HEADER_VALUE_MAX_LENGTH,
        scheduled_at_max_future_seconds: int = DEFAULT_SCHEDULE_AT_MAX_FUTURE_SECONDS,
        last_error_max_length: int = LAST_ERROR_MAX_LENGTH,
        truncation_suffix: str = DEFAULT_TRUNCATION_SUFFIX,
    ) -> None:
        self.max_attempts = max_attempts
        self.scheduled_at_skew_seconds = scheduled_at_skew_seconds
        self.payload_max_bytes = payload_max_bytes
        self.headers_max_count = headers_max_count
        self.header_key_max_length = header_key_max_length
        self.header_value_max_length = header_value_max_length
        self.scheduled_at_max_future_seconds = scheduled_at_max_future_seconds
        self.last_error_max_length = last_error_max_length
        self.truncation_suffix = truncation_suffix

    @property
    def validation_context(self) -> dict[str, int]:
        return {
            "scheduled_at_skew_seconds": self.scheduled_at_skew_seconds,
            "scheduled_at_max_future_seconds": self.scheduled_at_max_future_seconds,
            "payload_max_bytes": self.payload_max_bytes,
            "headers_max_count": self.headers_max_count,
            "header_key_max_length": self.header_key_max_length,
            "header_value_max_length": self.header_value_max_length,
        }

    def create_outbox_event(
        self,
        aggregate_type: str,
        aggregate_id: UUID,
        event_type: str,
        topic: str,
        partition_key: str,
        payload: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
        max_attempts: int | None = None,
        trace_id: str | None = None,
        idempotency_key: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        schema_version: str | None = None,
        scheduled_at: datetime | None = None,
    ) -> OutboxEvent:
        event_data = {
            "id": uuid4(),
            "aggregate_type": aggregate_type.strip(),
            "aggregate_id": aggregate_id,
            "event_type": event_type.strip(),
            "topic": topic.strip(),
            "partition_key": partition_key.strip(),
            "payload": to_jsonable_python(payload),
            "headers": headers,
            "attempts_made": 0,
            "max_attempts": max_attempts or self.max_attempts,
            "trace_id": trace_id,
            "idempotency_key": idempotency_key,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            "schema_version": schema_version,
            "status": EventStatus.PENDING,
        }
        if scheduled_at:
            event_data["scheduled_at"] = scheduled_at

        return OutboxEvent.model_validate(event_data, context=self.validation_context)

    def create_inbox_event(
        self,
        message_id: str,
        consumer_group: str,
        source: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        schema_version: str | None = None,
    ) -> InboxEvent:
        event_data = {
            "id": uuid4(),
            "message_id": message_id.strip(),
            "consumer_group": consumer_group.strip(),
            "source": source.strip(),
            "event_type": event_type.strip(),
            "payload": to_jsonable_python(payload),
            "headers": headers,
            "attempts_made": 0,
            "max_attempts": self.max_attempts,
            "trace_id": trace_id,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            "schema_version": schema_version,
            "status": EventStatus.PENDING,
        }
        return InboxEvent.model_validate(event_data, context=self.validation_context)

    def is_lock_stale(self, event: BaseEvent, now: datetime, stale_timeout_seconds: float | None = None) -> bool:
        """Check if the current lock is stale."""
        if not event.locked_at:
            return False

        timeout = stale_timeout_seconds if stale_timeout_seconds is not None else float(DEFAULT_LEASE_TIMEOUT_SECONDS)
        if timeout <= 0:
            raise ValueError(f"stale_timeout_seconds must be positive, got {timeout}")

        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        delta = now.astimezone(UTC) - event.locked_at
        return delta.total_seconds() > timeout

    def assert_locked_by(self, event: BaseEvent, worker_id: str) -> None:
        """Assert that the event is locked by the specified worker."""
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("worker_id cannot be empty or whitespace")

        if not event.locked_at:
            raise EventNotLockedError(event.id)
        if event.locked_by != normalized_worker_id:
            raise EventLockedByAnotherWorkerError(event.id, event.locked_by, normalized_worker_id)

    def lock_event[TE: BaseEvent](self, event: TE, worker_id: str, locked_at: datetime) -> TE:
        """Lock the event for processing."""
        if event.status != EventStatus.PENDING:
            raise InvalidEventStateError(
                event.id,
                event.status,
                [EventStatus.PENDING],
                "Only PENDING events can be locked",
            )

        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("worker_id cannot be empty or whitespace")

        if event.locked_at:
            raise EventAlreadyLockedError(event.id, event.locked_by)

        if locked_at.tzinfo is None:
            raise ValueError("locked_at must be timezone-aware")

        return self._copy_event_with_update(
            event,
            {
                "locked_at": locked_at.astimezone(UTC),
                "locked_by": normalized_worker_id,
            },
        )

    def refresh_event_lock[TE: BaseEvent](self, event: TE, worker_id: str, now: datetime) -> TE:
        """Extend the current lock time."""
        if event.status != EventStatus.PENDING:
            raise InvalidEventStateError(
                event.id,
                event.status,
                [EventStatus.PENDING],
                "Only PENDING events can have their lock refreshed",
            )

        self.assert_locked_by(event, worker_id)
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        return self._copy_event_with_update(event, {"locked_at": now.astimezone(UTC)})

    def unlock_event[TE: BaseEvent](self, event: TE, worker_id: str) -> TE:
        """Release the lock on the event."""
        if event.status != EventStatus.PENDING:
            raise InvalidEventStateError(
                event.id,
                event.status,
                [EventStatus.PENDING],
                "Only PENDING events can be unlocked",
            )

        self.assert_locked_by(event, worker_id)

        return self._copy_event_with_update(event, {"locked_at": None, "locked_by": None})

    def force_unlock_event[TE: BaseEvent](self, event: TE, reason: str) -> TE:
        """Forcefully release the lock without owner verification."""
        if not event.locked_at:
            raise EventNotLockedError(event.id)

        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("Reason for force unlock cannot be empty or whitespace")

        if len(normalized_reason) > FORCE_UNLOCK_REASON_MAX_LENGTH:
            raise ValueError(
                f"Reason for force unlock is too long: {len(normalized_reason)} "
                f"(max {FORCE_UNLOCK_REASON_MAX_LENGTH} chars)"
            )

        return self._copy_event_with_update(
            event,
            {
                "locked_at": None,
                "locked_by": None,
                "last_error": f"Administrative force unlock: {normalized_reason}",
            },
        )

    def mark_event_completed[TE: BaseEvent](self, event: TE, completed_at: datetime, worker_id: str) -> TE:
        """Mark event as successfully completed."""
        self.assert_locked_by(event, worker_id)

        if event.status == EventStatus.COMPLETED:  # pragma: no cover
            raise InvalidEventStateError(event.id, event.status, [EventStatus.PENDING], "Event is already completed")
        if event.status == EventStatus.FAILED:  # pragma: no cover
            raise InvalidEventStateError(event.id, event.status, [EventStatus.PENDING])

        if completed_at.tzinfo is None:
            raise ValueError("completed_at must be timezone-aware")

        comp_at_utc = completed_at.astimezone(UTC)
        if comp_at_utc < event.created_at:
            raise ValueError(f"completed_at {comp_at_utc} cannot be before created_at {event.created_at}")

        if comp_at_utc < event.scheduled_at:
            raise ValueError(f"completed_at {comp_at_utc} cannot be before scheduled_at {event.scheduled_at}")

        return self._copy_event_with_update(
            event,
            {
                "status": EventStatus.COMPLETED,
                "completed_at": comp_at_utc,
                "locked_at": None,
                "locked_by": None,
            },
        )

    def mark_event_failed[TE: BaseEvent](
        self,
        event: TE,
        error: str,
        worker_id: str,
        count_as_attempt: bool = True,
        next_retry_at: datetime | None = None,
    ) -> TE:
        """Mark event as failed with error details."""
        if not error.strip():
            raise ValueError("Error message cannot be empty or whitespace")

        if not count_as_attempt and next_retry_at is None:
            raise ValueError("next_retry_at must be provided if not counting as attempt")

        self.assert_locked_by(event, worker_id)

        if event.status == EventStatus.COMPLETED:  # pragma: no cover
            raise InvalidEventStateError(event.id, event.status, [EventStatus.PENDING])
        if event.status == EventStatus.FAILED:  # pragma: no cover
            raise InvalidEventStateError(
                event.id, event.status, [EventStatus.PENDING], "Event is already in FAILED state"
            )

        new_attempts_made = event.attempts_made + (1 if count_as_attempt else 0)
        if new_attempts_made > event.max_attempts:  # pragma: no cover
            raise ValueError(f"Cannot increment attempts_made beyond max_attempts ({event.max_attempts})")

        truncated_error = BaseEvent.truncate_error(
            error, max_bytes=self.last_error_max_length, suffix=self.truncation_suffix
        )
        new_status = EventStatus.FAILED if new_attempts_made == event.max_attempts else EventStatus.PENDING

        update_data: dict[str, object] = {
            "status": new_status,
            "attempts_made": new_attempts_made,
            "last_error": truncated_error,
            "locked_at": None,
            "locked_by": None,
        }
        if next_retry_at is not None:
            if next_retry_at.tzinfo is None:
                raise ValueError("next_retry_at must be timezone-aware")
            next_retry_at_utc = next_retry_at.astimezone(UTC)

            if next_retry_at_utc < event.created_at:
                raise ValueError(f"next_retry_at {next_retry_at_utc} cannot be before created_at {event.created_at}")

            if (next_retry_at_utc - event.created_at).total_seconds() > self.scheduled_at_max_future_seconds:
                raise ValueError(f"next_retry_at {next_retry_at_utc} is too far in the future")
            update_data["scheduled_at"] = next_retry_at_utc

        return self._copy_event_with_update(event, update_data)

    def _copy_event_with_update[TE: BaseEvent](self, event: TE, update: dict[str, object]) -> TE:
        """Create a copy of the event with updated fields."""
        new_data = event.model_dump(mode="python")
        new_data.update(update)
        return type(event).model_validate(new_data, context=self.validation_context)
