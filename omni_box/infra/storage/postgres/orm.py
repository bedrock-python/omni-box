"""Omni-box event ORM models. No dependency on sqlalchemy-postgres-kit."""

from __future__ import annotations

import datetime
from functools import partial
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from ....core.constants import DEFAULT_MAX_ATTEMPTS
from ....core.models.enums import EventStatus

# Enum without DB-level constraint (string column);
UnConstrainedEnum = partial(
    Enum,
    native_enum=False,
    create_constraint=False,
    validate_strings=True,
    values_callable=lambda obj: [getattr(item, "value", item) for item in obj] if hasattr(obj, "__members__") else obj,
)


class EventMixin:
    """Mixin for common transactional event database columns."""

    __abstract__ = True

    # Identifiers
    id: Mapped[UUID] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)

    # Payload & Metadata
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict[str, str] | None] = mapped_column(JSONB)
    trace_id: Mapped[str | None] = mapped_column(String(64))
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    causation_id: Mapped[str | None] = mapped_column(String(64))
    schema_version: Mapped[str | None] = mapped_column(String(50))

    # Status & Retries
    status: Mapped[EventStatus] = mapped_column(
        UnConstrainedEnum(
            EventStatus,
            length=20,
        ),
        default=EventStatus.PENDING,
        nullable=False,
    )
    attempts_made: Mapped[int] = mapped_column(default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(default=DEFAULT_MAX_ATTEMPTS, nullable=False)
    last_error: Mapped[str | None] = mapped_column(String(2000))

    # Timing - all datetime fields MUST be timezone-aware (UTC)
    scheduled_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))

    # Locking
    locked_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    locked_by: Mapped[str | None] = mapped_column(String(255))

    # Timestamps
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.timezone("UTC", func.now()),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        server_default=func.timezone("UTC", func.now()),
        onupdate=func.timezone("UTC", func.now()),
    )


class OutboxColumnsMixin:
    """Outbox-specific database columns."""

    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(255), nullable=False)


class InboxColumnsMixin:
    """Inbox-specific database columns."""

    message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(255), nullable=False)


def get_event_constraints(table_name: str, include_created_at_in_unique: bool = False) -> tuple:
    """Generate generic constraints and indexes for event table."""
    idempotency_key_cols = ["idempotency_key"]
    if include_created_at_in_unique:
        idempotency_key_cols.append("created_at")

    return (
        # Ensure attempts_made never exceeds max_attempts at database level
        CheckConstraint("attempts_made <= max_attempts", name=f"ck_{table_name}_attempts_valid"),
        # Status consistency constraints
        CheckConstraint(
            f"(status = '{EventStatus.COMPLETED.value}' AND completed_at IS NOT NULL) OR "
            f"(status != '{EventStatus.COMPLETED.value}' AND completed_at IS NULL)",
            name=f"ck_{table_name}_completed_status_consistency",
        ),
        # Locking consistency: locked_at and locked_by must be both NULL or both NOT NULL
        CheckConstraint(
            "(locked_at IS NULL AND locked_by IS NULL) OR (locked_at IS NOT NULL AND locked_by IS NOT NULL)",
            name=f"ck_{table_name}_lock_consistency",
        ),
        Index(
            f"idx_{table_name}_pending_fetch",
            "scheduled_at",
            postgresql_where=text(
                f"status = '{EventStatus.PENDING.value}' AND locked_at IS NULL AND attempts_made < max_attempts"
            ),
        ),
        Index(
            f"idx_{table_name}_locked_at",
            "locked_at",
            postgresql_where=text("locked_at IS NOT NULL"),
        ),
        Index(
            f"idx_{table_name}_completed_cleanup",
            "completed_at",
            postgresql_where=text(f"status = '{EventStatus.COMPLETED.value}' AND completed_at IS NOT NULL"),
        ),
        Index(
            f"idx_{table_name}_idempotency_key",
            *idempotency_key_cols,
            postgresql_where=text("idempotency_key IS NOT NULL"),
            unique=True,
        ),
        Index(f"idx_{table_name}_created_at", "created_at"),
        Index(f"idx_{table_name}_updated_at", "updated_at"),
    )


class OutboxEventDBBase(EventMixin, OutboxColumnsMixin):
    """Abstract base for standard outbox table."""

    __abstract__ = True
    __tablename__ = "outbox_events"
    __table_args__ = get_event_constraints("outbox_events")


class InboxEventDBBase(EventMixin, InboxColumnsMixin):
    """Abstract base for standard inbox table."""

    __abstract__ = True
    __tablename__ = "inbox_events"

    # Columns for INSERT ... ON CONFLICT DO NOTHING (PostgresInboxRepository)
    __inbox_dedup_index_columns__: tuple[str, ...] = ("message_id", "consumer_group")

    __table_args__ = (
        *get_event_constraints("inbox_events"),
        # Inbox specific unique constraint for deduplication
        Index(
            "idx_inbox_deduplication",
            "message_id",
            "consumer_group",
            unique=True,
        ),
    )


class OutboxEventPartitionedDBBase(EventMixin, OutboxColumnsMixin):
    """Abstract base for partitioned outbox table."""

    __abstract__ = True
    __tablename__ = "outbox_events_partitioned"

    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.timezone("UTC", func.now()),
        nullable=False,
        primary_key=True,
    )

    __outbox_conflict_index_id__ = ("id", "created_at")
    __outbox_conflict_index_idempotency__ = ("idempotency_key", "created_at")

    __table_args__ = (
        *get_event_constraints("outbox_events_p", include_created_at_in_unique=True),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )


class InboxEventPartitionedDBBase(EventMixin, InboxColumnsMixin):
    """Abstract base for partitioned inbox table (RANGE by created_at).

    PostgreSQL requires unique indexes on partitioned tables to include the partition key.
    Deduplication uses (message_id, consumer_group, created_at); ON CONFLICT must use the same columns.
    """

    __abstract__ = True
    __tablename__ = "inbox_events_partitioned"

    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.timezone("UTC", func.now()),
        nullable=False,
        primary_key=True,
    )

    __inbox_dedup_index_columns__: tuple[str, ...] = ("message_id", "consumer_group", "created_at")

    __table_args__ = (
        *get_event_constraints("inbox_events_p", include_created_at_in_unique=True),
        Index(
            "idx_inbox_events_p_deduplication",
            "message_id",
            "consumer_group",
            "created_at",
            unique=True,
        ),
        Index(
            "idx_inbox_events_p_message_consumer",
            "message_id",
            "consumer_group",
            unique=False,
        ),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )


# Type aliases
type EventModelType = type[EventMixin]
type OutboxModelType = type[OutboxEventDBBase] | type[OutboxEventPartitionedDBBase]
type InboxModelType = type[InboxEventDBBase] | type[InboxEventPartitionedDBBase]
