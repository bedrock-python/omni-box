"""Omni-box domain entities."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Self, cast
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationInfo, field_validator, model_validator

from ...utils import utc_now
from ..constants import (
    AGGREGATE_TYPE_MAX_LENGTH,
    CAUSATION_ID_MAX_LENGTH,
    CONSUMER_GROUP_MAX_LENGTH,
    CORRELATION_ID_MAX_LENGTH,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_SCHEDULE_AT_MAX_FUTURE_SECONDS,
    DEFAULT_SCHEDULE_AT_SKEW_SECONDS,
    EVENT_TYPE_MAX_LENGTH,
    IDEMPOTENCY_KEY_MAX_LENGTH,
    LAST_ERROR_MAX_LENGTH,
    MESSAGE_ID_MAX_LENGTH,
    PARTITION_KEY_MAX_LENGTH,
    SCHEMA_VERSION_MAX_LENGTH,
    SOURCE_MAX_LENGTH,
    TOPIC_MAX_LENGTH,
    TRACE_ID_MAX_LENGTH,
    WORKER_ID_MAX_LENGTH,
)
from ..exceptions import (
    EventAlreadyLockedError,
    EventLockedByAnotherWorkerError,
    EventNotLockedError,
    InvalidEventStateError,
)
from .enums import EventStatus
from .schemas import BaseEventSchema
from .types import StrippedNonEmptyStr
from .validators import validate_headers, validate_payload

__all__ = [
    "BaseEvent",
    "EventAlreadyLockedError",
    "EventLockedByAnotherWorkerError",
    "EventNotLockedError",
    "InboxEvent",
    "InvalidEventStateError",
    "OutboxEvent",
]


class BaseEvent(BaseModel):
    """Base generic event domain entity for Transactional Outbox and Inbox.

    Provides core fields for event identification, status tracking, retry
    orchestration, and metadata. All events are frozen (immutable) to
    ensure data integrity during processing.
    """

    model_config = ConfigDict(frozen=True)

    # Identifiers
    id: UUID = Field(default_factory=uuid4)
    event_type: StrippedNonEmptyStr = Field(max_length=EVENT_TYPE_MAX_LENGTH)

    # Payload & Metadata
    payload: dict[str, JsonValue] = Field(..., description="Event payload as a JSON-compatible dictionary.")
    headers: dict[str, str] | None = Field(default=None, description="Optional message headers.")
    trace_id: StrippedNonEmptyStr | None = Field(default=None, max_length=TRACE_ID_MAX_LENGTH)
    idempotency_key: StrippedNonEmptyStr | None = Field(default=None, max_length=IDEMPOTENCY_KEY_MAX_LENGTH)
    correlation_id: StrippedNonEmptyStr | None = Field(default=None, max_length=CORRELATION_ID_MAX_LENGTH)
    causation_id: StrippedNonEmptyStr | None = Field(default=None, max_length=CAUSATION_ID_MAX_LENGTH)
    schema_version: StrippedNonEmptyStr | None = Field(default=None, max_length=SCHEMA_VERSION_MAX_LENGTH)

    # Status & Retries
    status: EventStatus = EventStatus.PENDING
    attempts_made: int = Field(
        default=0,
        ge=0,
        description="Number of failed processing attempts recorded (does not include a successful attempt).",
    )
    max_attempts: int = Field(
        default=DEFAULT_MAX_ATTEMPTS,
        ge=1,
        description=(
            "Maximum number of processing failures allowed. "
            "Event transitions to FAILED when attempts_made == max_attempts."
        ),
    )
    last_error: str | None = Field(default=None, max_length=LAST_ERROR_MAX_LENGTH)

    # Timing
    created_at: datetime = Field(default_factory=utc_now)
    scheduled_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = Field(default=None)
    locked_at: datetime | None = Field(default=None)
    locked_by: StrippedNonEmptyStr | None = Field(default=None, max_length=WORKER_ID_MAX_LENGTH)

    @field_validator("created_at", "scheduled_at", "completed_at", "locked_at", mode="after")
    @classmethod
    def validate_timezone(cls, v: datetime | None) -> datetime | None:
        """Ensure all datetimes are timezone-aware and normalize to UTC."""
        if v is None:
            return v

        if v.tzinfo is None:
            raise ValueError("Datetime must be timezone-aware")

        # Normalize any timezone to UTC
        return v.astimezone(UTC)

    @field_validator("scheduled_at")
    @classmethod
    def validate_scheduled_at(cls, v: datetime, info: ValidationInfo) -> datetime:
        """Ensure scheduled_at is within reasonable range."""
        created_at = info.data.get("created_at")
        if created_at is not None:
            skew_limit = DEFAULT_SCHEDULE_AT_SKEW_SECONDS
            max_future_seconds = DEFAULT_SCHEDULE_AT_MAX_FUTURE_SECONDS
            if info.context:
                skew_limit = info.context.get("scheduled_at_skew_seconds", DEFAULT_SCHEDULE_AT_SKEW_SECONDS)
                max_future_seconds = info.context.get(
                    "scheduled_at_max_future_seconds", DEFAULT_SCHEDULE_AT_MAX_FUTURE_SECONDS
                )

            if skew_limit < 0:
                raise ValueError(f"scheduled_at_skew_seconds must be >= 0, got {skew_limit}")
            if max_future_seconds < 1:
                raise ValueError(f"scheduled_at_max_future_seconds must be >= 1, got {max_future_seconds}")

            if v < created_at and (created_at - v).total_seconds() > skew_limit:
                raise ValueError(f"scheduled_at {v} cannot be significantly before created_at {created_at}")

            if (v - created_at).total_seconds() > max_future_seconds:
                raise ValueError(
                    f"scheduled_at {v} is too far in the future (max {max_future_seconds} seconds from creation)"
                )

        return v

    @field_validator("payload")
    @classmethod
    def validate_payload_content(cls, v: dict[str, JsonValue], info: ValidationInfo) -> dict[str, JsonValue]:
        return validate_payload(v, info)

    @field_validator("headers")
    @classmethod
    def validate_headers_content(cls, v: dict[str, str] | None, info: ValidationInfo) -> dict[str, str] | None:
        return validate_headers(v, info)

    @model_validator(mode="after")
    def _pydantic_validate_invariants(self) -> Self:
        """Internal Pydantic model validator hook."""
        return self.validate_invariants()

    def validate_invariants(self) -> Self:
        """Enforce business invariants across all fields."""
        self._validate_status_timing()
        self._validate_attempts()
        self._validate_lock()
        return self

    def _validate_status_timing(self) -> None:
        """Validate status vs completed timing."""
        if self.status == EventStatus.COMPLETED:
            if self.completed_at is None:
                raise ValueError("completed_at must be set when status is COMPLETED")

            # Allow small clock skew (e.g. 1 second)
            skew_limit = 1.0
            if (
                self.completed_at < self.created_at
                and (self.created_at - self.completed_at).total_seconds() > skew_limit
            ):
                raise ValueError(f"completed_at {self.completed_at} cannot be before created_at {self.created_at}")
            if (
                self.completed_at < self.scheduled_at
                and (self.scheduled_at - self.completed_at).total_seconds() > skew_limit
            ):
                raise ValueError(f"completed_at {self.completed_at} cannot be before scheduled_at {self.scheduled_at}")
        elif self.completed_at is not None:
            raise ValueError(f"completed_at must be None when status is {self.status}")

    def _validate_attempts(self) -> None:
        """Validate attempts consistency."""
        if self.attempts_made > self.max_attempts:
            raise ValueError(f"attempts_made ({self.attempts_made}) cannot exceed max_attempts ({self.max_attempts})")

        if self.status == EventStatus.FAILED and self.attempts_made != self.max_attempts:
            raise ValueError(
                f"status is FAILED, but attempts_made ({self.attempts_made}) "
                f"must equal max_attempts ({self.max_attempts})"
            )
        if self.status == EventStatus.PENDING and self.attempts_made >= self.max_attempts:
            raise ValueError(
                f"status is PENDING, but attempts_made ({self.attempts_made}) "
                f"has reached max_attempts ({self.max_attempts})"
            )

    def _validate_lock(self) -> None:
        """Validate lock consistency."""
        if (self.locked_at is not None) != (self.locked_by is not None):
            raise ValueError("locked_at and locked_by must be both set or both None")

        # Locked events must always be PENDING.
        if self.is_locked and self.status != EventStatus.PENDING:
            raise ValueError(f"locked event must be in PENDING status, got {self.status}")

    @property
    def is_locked(self) -> bool:
        """Check if the event is currently locked."""
        return self.locked_at is not None

    @property
    def can_retry(self) -> bool:
        """Check if the event can be retried."""
        return self.status == EventStatus.PENDING and self.attempts_made < self.max_attempts

    @property
    def attempts_left(self) -> int:
        """Number of attempts remaining before FAILED status."""
        if self.status != EventStatus.PENDING:
            return 0
        return self.max_attempts - self.attempts_made

    @property
    def failure_count(self) -> int:
        """Number of attempts made when in FAILED status."""
        return self.attempts_made if self.status == EventStatus.FAILED else 0

    @staticmethod
    def truncate_error(error: str, max_bytes: int, suffix: str) -> str:
        """Truncate error message to database limit (in bytes)."""
        if max_bytes < 1:
            raise ValueError(f"max_bytes must be >= 1, got {max_bytes}")

        stripped = error.strip()
        if not stripped:
            raise ValueError("Error message cannot be empty or whitespace")

        encoded = stripped.encode("utf-8")
        if len(encoded) <= max_bytes:
            return stripped

        suffix_encoded = suffix.encode("utf-8")
        if len(suffix_encoded) >= max_bytes:
            return encoded[:max_bytes].decode("utf-8", errors="ignore")

        keep_bytes = max_bytes - len(suffix_encoded)
        return encoded[:keep_bytes].decode("utf-8", errors="ignore") + suffix


class OutboxEvent(BaseEvent):
    """Domain entity for events being sent from the application (Outbox).

    Adds aggregate context and routing information required for publishing
    events to external brokers.
    """

    # Outbox-specific Identifiers
    aggregate_type: StrippedNonEmptyStr = Field(max_length=AGGREGATE_TYPE_MAX_LENGTH)
    aggregate_id: UUID = Field(...)

    # Routing
    topic: StrippedNonEmptyStr = Field(max_length=TOPIC_MAX_LENGTH)
    partition_key: StrippedNonEmptyStr = Field(max_length=PARTITION_KEY_MAX_LENGTH)


class InboxEvent(BaseEvent):
    """Domain entity for events received from external brokers (Inbox).

    Ensures exactly-once processing by tracking external message identifiers
    and consumer groups. Facilitates payload parsing into typed schemas
    using the global schema registry.
    """

    # Inbox-specific Identifiers
    message_id: StrippedNonEmptyStr = Field(max_length=MESSAGE_ID_MAX_LENGTH)
    consumer_group: StrippedNonEmptyStr = Field(max_length=CONSUMER_GROUP_MAX_LENGTH)

    # Source info
    source: StrippedNonEmptyStr = Field(max_length=SOURCE_MAX_LENGTH)

    @property
    def processed_at(self) -> datetime | None:
        """Alias for completed_at."""
        return self.completed_at

    def get_context_value(self, key: str) -> str | None:
        """Get a value from the event headers (context).

        Args:
            key: The header key to retrieve.

        Returns:
            The header value if present, None otherwise.
        """
        return self.headers.get(key) if self.headers else None

    def get_payload_as[S: BaseEventSchema](self, schema_cls: type[S] | None = None) -> S:
        """Resolve and parse the payload into a typed schema.

        If schema_cls is provided, it parses with it. Otherwise, it uses the global
        discovery mechanism via BaseEventSchema.resolve() based on event_type and schema_version.
        """
        final_schema_cls: type[S]
        if schema_cls is None:
            resolved_cls = BaseEventSchema.resolve(
                event_type=self.event_type,
                version=self.schema_version,
            )
            final_schema_cls = cast(type[S], resolved_cls)
        else:
            final_schema_cls = schema_cls

        return final_schema_cls.from_payload(self.payload)
