"""Concrete ORM models for testing."""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase

from omni_box.infra.storage.postgres.orm import (
    InboxEventDBBase,
    InboxEventPartitionedDBBase,
    OutboxEventDBBase,
    OutboxEventPartitionedDBBase,
)


class Base(DeclarativeBase):
    """Test database base class."""

    pass


class ConcreteOutboxEvent(Base, OutboxEventDBBase):
    """Concrete outbox event model for testing."""

    pass


class ConcreteInboxEvent(Base, InboxEventDBBase):
    """Concrete inbox event model for testing."""

    pass


class ConcreteInboxEventPartitioned(Base, InboxEventPartitionedDBBase):
    """Concrete inbox event model with partitioning support for testing."""

    pass


class ConcreteOutboxEventPartitioned(Base, OutboxEventPartitionedDBBase):
    """Concrete outbox event model with partitioning support for testing."""

    pass
