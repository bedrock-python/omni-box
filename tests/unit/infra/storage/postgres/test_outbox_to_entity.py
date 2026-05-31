"""Unit tests for PostgresOutboxRepository._to_entity.

Tests the direct conversion from an ORM mock to an OutboxEvent domain entity,
covering all fields that ``_to_entity`` reads from the db_event.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.storage.postgres.repositories.outbox import PostgresOutboxRepository
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.unit

_NOW = datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC)


def _make_repo() -> PostgresOutboxRepository:
    from unittest.mock import AsyncMock

    from sqlalchemy.ext.asyncio import AsyncSession

    session = AsyncMock(spec=AsyncSession)
    return PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEvent)


def test__to_entity__all_fields__maps_to_outbox_event() -> None:
    # Arrange
    repo = _make_repo()

    aggregate_id = uuid4()
    event_id = uuid4()

    db = MagicMock()
    db.id = event_id
    db.event_type = "order.placed"
    db.payload = {"order_id": "abc"}
    db.headers = {"x-trace": "123"}
    db.status = "pending"
    db.attempts_made = 0
    db.max_attempts = 5
    db.last_error = None
    db.trace_id = "trace-xyz"
    db.idempotency_key = "idem-1"
    db.correlation_id = "corr-1"
    db.causation_id = "cause-1"
    db.schema_version = "v1"
    db.created_at = _NOW
    db.scheduled_at = _NOW
    db.completed_at = None
    db.locked_at = None
    db.locked_by = None
    # Outbox fields
    db.aggregate_type = "order"
    db.aggregate_id = aggregate_id
    db.topic = "orders"
    db.partition_key = str(aggregate_id)

    # Act
    entity = repo._to_entity(db)  # type: ignore[arg-type]

    # Assert
    assert isinstance(entity, OutboxEvent)
    assert entity.id == event_id
    assert entity.event_type == "order.placed"
    assert entity.payload == {"order_id": "abc"}
    assert entity.headers == {"x-trace": "123"}
    assert entity.trace_id == "trace-xyz"
    assert entity.idempotency_key == "idem-1"
    assert entity.correlation_id == "corr-1"
    assert entity.causation_id == "cause-1"
    assert entity.schema_version == "v1"
    assert entity.aggregate_type == "order"
    assert entity.aggregate_id == aggregate_id
    assert entity.topic == "orders"
    assert entity.partition_key == str(aggregate_id)
    assert entity.attempts_made == 0
    assert entity.max_attempts == 5
    assert entity.created_at == _NOW
    assert entity.scheduled_at == _NOW
