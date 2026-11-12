"""created_at and updated_at in a session whose time zone is not UTC."""

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.storage.postgres import PostgresOutboxRepository
from omni_box.utils import utc_now
from tests.models import (
    Base,
    ConcreteInboxEvent,
    ConcreteInboxEventPartitioned,
    ConcreteOutboxEvent,
    ConcreteOutboxEventPartitioned,
)

pytestmark = pytest.mark.integration

# Not UTC, and no daylight saving time since 2014, so the offset is the same all year.
SESSION_TIME_ZONE = "Europe/Moscow"

OUTBOX_COLUMNS: dict[str, Any] = {"aggregate_type": "order", "topic": "orders", "partition_key": "k"}
INBOX_COLUMNS: dict[str, Any] = {"message_id": "msg-1", "consumer_group": "cg-1", "source": "orders"}


@pytest.mark.parametrize(
    ("model", "kind_columns"),
    [
        (ConcreteOutboxEvent, OUTBOX_COLUMNS),
        (ConcreteOutboxEventPartitioned, OUTBOX_COLUMNS),
        (ConcreteInboxEvent, INBOX_COLUMNS),
        (ConcreteInboxEventPartitioned, INBOX_COLUMNS),
    ],
)
async def test__event_tables__non_utc_session__server_defaults_are_insert_instant(
    async_session: AsyncSession, model: type[Base], kind_columns: dict[str, Any]
) -> None:
    # Arrange
    await async_session.execute(text(f"SET LOCAL TIME ZONE '{SESSION_TIME_ZONE}'"))
    values = {"id": uuid4(), "event_type": "e", "payload": {}, "scheduled_at": utc_now(), **kind_columns}
    if model in (ConcreteOutboxEvent, ConcreteOutboxEventPartitioned):
        values["aggregate_id"] = uuid4()
    table = model.__table__

    # Act
    row = (
        await async_session.execute(
            insert(table)
            .values(**values)
            .returning(table.c.created_at, table.c.updated_at, func.now().label("transaction_start"))
        )
    ).one()

    # Assert
    assert row.created_at == row.transaction_start
    assert row.updated_at == row.transaction_start


async def test__outbox_repository__non_utc_session__update_sets_updated_at_to_update_instant(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    await repo.create(
        OutboxEvent(
            id=event_id,
            aggregate_type="t",
            aggregate_id=uuid4(),
            event_type="e",
            topic="tp",
            partition_key="k",
            payload={"p": 1},
        )
    )
    await async_session.commit()
    await async_session.execute(text(f"SET LOCAL TIME ZONE '{SESSION_TIME_ZONE}'"))

    # Act
    await repo.mark_processing(event_id, worker_id="worker-1")

    # Assert
    row = (
        await async_session.execute(
            select(ConcreteOutboxEvent.updated_at, func.now().label("transaction_start")).where(
                ConcreteOutboxEvent.id == event_id
            )
        )
    ).one()
    assert row.updated_at == row.transaction_start
