# Database Migrations and Schema

`omni-box` ships abstract SQLAlchemy bases that describe the outbox/inbox schema. The consuming service owns its own `DeclarativeBase`, registers the concrete tables, and generates migrations.

> The DDL below mirrors `omni_box/infra/storage/postgres/orm.py`. If you derive a custom schema, keep the column names and types identical — `PostgresOutboxRepository` / `PostgresInboxRepository` rely on them via `mapped_column` defaults.

## Status values

`EventStatus` is defined as a Python `StrEnum` with **lowercase** values:

```python
class EventStatus(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
```

All DDL, constraints, and partial indexes use the lowercase form.

## Outbox table

```sql
CREATE TABLE outbox_events (
    id                UUID PRIMARY KEY,
    event_type        VARCHAR(100) NOT NULL,

    aggregate_type    VARCHAR(50)  NOT NULL,
    aggregate_id      UUID         NOT NULL,
    topic             VARCHAR(255) NOT NULL,
    partition_key     VARCHAR(255) NOT NULL,

    payload           JSONB        NOT NULL,
    headers           JSONB,

    trace_id          VARCHAR(64),
    idempotency_key   VARCHAR(128),
    correlation_id    VARCHAR(64),
    causation_id      VARCHAR(64),
    schema_version    VARCHAR(50),

    status            VARCHAR(20)  NOT NULL DEFAULT 'pending',
    attempts_made     INTEGER      NOT NULL DEFAULT 0,
    max_attempts      INTEGER      NOT NULL DEFAULT 6,
    last_error        VARCHAR(2000),

    scheduled_at      TIMESTAMPTZ  NOT NULL,
    completed_at      TIMESTAMPTZ,

    locked_at         TIMESTAMPTZ,
    locked_by         VARCHAR(255),

    created_at        TIMESTAMPTZ  NOT NULL DEFAULT timezone('UTC', now()),
    updated_at        TIMESTAMPTZ           DEFAULT timezone('UTC', now()),

    CONSTRAINT ck_outbox_events_attempts_valid CHECK (attempts_made <= max_attempts),
    CONSTRAINT ck_outbox_events_completed_status_consistency CHECK (
        (status = 'completed' AND completed_at IS NOT NULL) OR
        (status <> 'completed' AND completed_at IS NULL)
    ),
    CONSTRAINT ck_outbox_events_lock_consistency CHECK (
        (locked_at IS NULL AND locked_by IS NULL) OR
        (locked_at IS NOT NULL AND locked_by IS NOT NULL)
    )
);

-- Hot path for the publisher: PENDING, not locked, has budget.
CREATE INDEX idx_outbox_events_pending_fetch
    ON outbox_events (scheduled_at)
    WHERE status = 'pending' AND locked_at IS NULL AND attempts_made < max_attempts;

CREATE INDEX idx_outbox_events_locked_at
    ON outbox_events (locked_at)
    WHERE locked_at IS NOT NULL;

CREATE INDEX idx_outbox_events_completed_cleanup
    ON outbox_events (completed_at)
    WHERE status = 'completed' AND completed_at IS NOT NULL;

CREATE UNIQUE INDEX idx_outbox_events_idempotency_key
    ON outbox_events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_outbox_events_created_at ON outbox_events (created_at);
CREATE INDEX idx_outbox_events_updated_at ON outbox_events (updated_at);
```

## Inbox table

The inbox uses an extra **unique** index over `(message_id, consumer_group)` so that `PostgresInboxRepository` can use `INSERT ... ON CONFLICT DO NOTHING` for cheap deduplication.

```sql
CREATE TABLE inbox_events (
    id                UUID PRIMARY KEY,
    event_type        VARCHAR(100) NOT NULL,

    message_id        VARCHAR(255) NOT NULL,
    consumer_group    VARCHAR(255) NOT NULL,
    source            VARCHAR(255) NOT NULL,

    payload           JSONB        NOT NULL,
    headers           JSONB,

    trace_id          VARCHAR(64),
    idempotency_key   VARCHAR(128),
    correlation_id    VARCHAR(64),
    causation_id      VARCHAR(64),
    schema_version    VARCHAR(50),

    status            VARCHAR(20)  NOT NULL DEFAULT 'pending',
    attempts_made     INTEGER      NOT NULL DEFAULT 0,
    max_attempts      INTEGER      NOT NULL DEFAULT 6,
    last_error        VARCHAR(2000),

    scheduled_at      TIMESTAMPTZ  NOT NULL,
    completed_at      TIMESTAMPTZ,

    locked_at         TIMESTAMPTZ,
    locked_by         VARCHAR(255),

    created_at        TIMESTAMPTZ  NOT NULL DEFAULT timezone('UTC', now()),
    updated_at        TIMESTAMPTZ           DEFAULT timezone('UTC', now()),

    CONSTRAINT ck_inbox_events_attempts_valid CHECK (attempts_made <= max_attempts),
    CONSTRAINT ck_inbox_events_completed_status_consistency CHECK (
        (status = 'completed' AND completed_at IS NOT NULL) OR
        (status <> 'completed' AND completed_at IS NULL)
    ),
    CONSTRAINT ck_inbox_events_lock_consistency CHECK (
        (locked_at IS NULL AND locked_by IS NULL) OR
        (locked_at IS NOT NULL AND locked_by IS NOT NULL)
    )
);

-- Inbox deduplication key.
CREATE UNIQUE INDEX idx_inbox_deduplication
    ON inbox_events (message_id, consumer_group);

CREATE INDEX idx_inbox_events_pending_fetch
    ON inbox_events (scheduled_at)
    WHERE status = 'pending' AND locked_at IS NULL AND attempts_made < max_attempts;

CREATE INDEX idx_inbox_events_locked_at
    ON inbox_events (locked_at)
    WHERE locked_at IS NOT NULL;

CREATE INDEX idx_inbox_events_completed_cleanup
    ON inbox_events (completed_at)
    WHERE status = 'completed' AND completed_at IS NOT NULL;

CREATE UNIQUE INDEX idx_inbox_events_idempotency_key
    ON inbox_events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_inbox_events_created_at ON inbox_events (created_at);
CREATE INDEX idx_inbox_events_updated_at ON inbox_events (updated_at);
```

## Partitioned variants

When you inherit from `OutboxEventPartitionedDBBase` / `InboxEventPartitionedDBBase`, `created_at` becomes part of the **primary key** (PostgreSQL requires unique indexes on partitioned tables to include the partition key). Every unique index — including the inbox deduplication index — must be widened accordingly.

For the partitioned inbox:

```sql
CREATE UNIQUE INDEX idx_inbox_events_p_deduplication
    ON inbox_events_partitioned (message_id, consumer_group, created_at);
```

`PostgresInboxRepository` reads the column tuple from `__inbox_dedup_index_columns__` on the model class, so the partitioned base sets `("message_id", "consumer_group", "created_at")` automatically.

## SQLAlchemy registration

`omni_box.infra.storage.postgres.orm` does **not** export a shared `Base`. Define your own `DeclarativeBase` and bind the abstract models to it:

```python
from sqlalchemy.orm import DeclarativeBase

from omni_box.infra.storage.postgres import (
    InboxEventDBBase,
    OutboxEventDBBase,
)


class Base(DeclarativeBase):
    """Your service-owned declarative base."""


class OutboxEventDB(Base, OutboxEventDBBase):
    """Concrete outbox table; inherits __tablename__ and __table_args__."""


class InboxEventDB(Base, InboxEventDBBase):
    """Concrete inbox table; inherits __tablename__, dedup index, etc."""
```

You can override `__tablename__`, add service-specific columns, or change `__inbox_dedup_index_columns__` if you partition.

## Alembic example

```python
# alembic/env.py
from my_service.infra.db.models import Base
target_metadata = Base.metadata
```

An auto-generated migration for the outbox table will look like this (fragment):

```python
def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("aggregate_type", sa.String(50), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("partition_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("headers", postgresql.JSONB),
        sa.Column("trace_id", sa.String(64)),
        sa.Column("idempotency_key", sa.String(128)),
        sa.Column("correlation_id", sa.String(64)),
        sa.Column("causation_id", sa.String(64)),
        sa.Column("schema_version", sa.String(50)),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("attempts_made", sa.Integer, nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer, nullable=False, server_default="6"),
        sa.Column("last_error", sa.String(2000)),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("locked_at", sa.DateTime(timezone=True)),
        sa.Column("locked_by", sa.String(255)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("timezone('UTC', now())"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("timezone('UTC', now())"),
        ),
        sa.CheckConstraint("attempts_made <= max_attempts", name="ck_outbox_events_attempts_valid"),
        # ...other CHECKs and indexes from get_event_constraints("outbox_events")
    )
```

If you use SQLAlchemy autogenerate, Alembic picks up the `Index` / `CheckConstraint` declarations from `__table_args__` automatically — you do not need to write them out manually.
