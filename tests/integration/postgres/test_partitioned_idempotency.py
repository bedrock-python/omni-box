"""Idempotent inserts on a partitioned outbox table, against a real Postgres.

The unique index on a partitioned table has to carry ``created_at``, so on its
own it only rejects an identical timestamp — never a retry. The repository
serializes ``idempotency_key`` with an advisory lock and a lookup instead;
these tests are the reproduction from issue #7 and its neighbours.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.storage.postgres import PostgresOutboxRepository
from omni_box.utils import utc_now
from tests.models import ConcreteOutboxEvent, ConcreteOutboxEventPartitioned

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _event(idempotency_key: str | None, **overrides: Any) -> OutboxEvent:
    now = utc_now()
    data: dict[str, Any] = {
        "id": uuid4(),
        "aggregate_type": "order",
        "aggregate_id": uuid4(),
        "event_type": "order.created",
        "topic": "orders",
        "partition_key": "key",
        "payload": {"a": 1},
        "idempotency_key": idempotency_key,
        "created_at": now,
        "scheduled_at": now,
    }
    data.update(overrides)
    return OutboxEvent.model_validate(data)


def _partitioned_repo(session: AsyncSession) -> PostgresOutboxRepository:
    return PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEventPartitioned)


async def _rows_with_key(session: AsyncSession, model: type, key: str) -> int:
    stmt = select(func.count()).select_from(model).where(model.idempotency_key == key)  # type: ignore[attr-defined]
    return int((await session.execute(stmt)).scalar_one())


async def test__outbox_repository__partitioned_same_key_a_moment_later__returns_the_first_event_and_keeps_one_row(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = _partitioned_repo(async_session)
    key = f"idem-{uuid4()}"
    first = await repo.create(_event(key))
    await async_session.commit()
    await asyncio.sleep(0.05)  # a retry arriving later: a different created_at

    # Act
    second = await repo.create(_event(key))
    await async_session.commit()

    # Assert
    assert second.id == first.id
    assert await _rows_with_key(async_session, ConcreteOutboxEventPartitioned, key) == 1


async def test__outbox_repository__partitioned_concurrent_writers__second_waits_for_the_first_and_gets_its_row(
    db_engine: AsyncEngine, async_session: AsyncSession
) -> None:
    # Arrange
    key = f"idem-{uuid4()}"
    first = await _partitioned_repo(async_session).create(_event(key))  # holds the advisory lock until commit
    make_session = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)

    # Act
    async with make_session() as session_b:
        writer_b = asyncio.create_task(_partitioned_repo(session_b).create(_event(key)))
        await asyncio.sleep(0.2)
        still_waiting = not writer_b.done()
        await async_session.commit()
        second = await writer_b
        await session_b.commit()

    # Assert
    assert still_waiting
    assert second.id == first.id
    assert await _rows_with_key(async_session, ConcreteOutboxEventPartitioned, key) == 1


async def test__outbox_repository__partitioned_bulk_create__collapses_duplicates_within_and_across_batches(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = _partitioned_repo(async_session)
    k1, k2, k3 = (f"idem-{uuid4()}" for _ in range(3))

    # Act
    first_batch = await repo.bulk_create([_event(k1), _event(k2), _event(k1)])
    await async_session.commit()
    second_batch = await repo.bulk_create([_event(k2), _event(k3)])
    await async_session.commit()

    # Assert
    assert [e.idempotency_key for e in first_batch] == [k1, k2, k1]
    assert first_batch[2].id == first_batch[0].id
    assert [e.idempotency_key for e in second_batch] == [k2, k3]
    assert second_batch[0].id == first_batch[1].id
    for key in (k1, k2, k3):
        assert await _rows_with_key(async_session, ConcreteOutboxEventPartitioned, key) == 1


async def test__outbox_repository__partitioned_table_holding_old_duplicates__returns_the_earliest_instead_of_raising(
    async_session: AsyncSession,
) -> None:
    # Arrange: two rows for one key, written before the repository serialized the key
    repo = _partitioned_repo(async_session)
    key = f"idem-{uuid4()}"
    earlier = utc_now() - timedelta(seconds=5)
    older = _event(key, created_at=earlier, scheduled_at=earlier)
    newer = _event(key)
    for duplicate in (older, newer):
        await async_session.execute(
            insert(ConcreteOutboxEventPartitioned).values(**repo._prepare_insert_values(duplicate))
        )
    await async_session.commit()

    # Act
    result = await repo.create(_event(key))
    await async_session.commit()

    # Assert
    assert result.id == older.id
    assert await _rows_with_key(async_session, ConcreteOutboxEventPartitioned, key) == 2


async def test__outbox_repository__plain_table_same_key__still_deduplicates_through_the_unique_index(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    key = f"idem-{uuid4()}"
    first = await repo.create(_event(key))
    await async_session.commit()

    # Act
    second = await repo.create(_event(key))
    await async_session.commit()

    # Assert
    assert second.id == first.id
    assert await _rows_with_key(async_session, ConcreteOutboxEvent, key) == 1
