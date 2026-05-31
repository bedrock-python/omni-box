"""Integration tests for outbox atomicity guarantees."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.storage.postgres import PostgresOutboxRepository
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.integration


# Define test business metadata globally
test_metadata = MetaData()
business_table = Table(
    "test_business_data",
    test_metadata,
    Column("id", Integer, primary_key=True),
    Column("data", String(255)),
)


@pytest.fixture(autouse=True)
async def setup_business_table(db_engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """Ensure business table exists for each test using a separate connection."""
    async with db_engine.begin() as conn:
        await conn.run_sync(test_metadata.create_all)
    yield
    async with db_engine.begin() as conn:
        await conn.run_sync(test_metadata.drop_all)


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__rollback_mid_transaction__reverts_both_business_and_outbox_data(
    db_engine: AsyncEngine,
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(async_session, model_class=ConcreteOutboxEvent)
    event = OutboxEvent(
        id=uuid4(),
        aggregate_type="TestAggregate",
        aggregate_id=uuid4(),
        event_type="TestEvent",
        topic="test-topic",
        partition_key="test-key",
        payload={"data": "test"},
    )

    def simulate_crash() -> None:
        raise RuntimeError("Simulated crash")

    # Act
    try:
        async with async_session.begin():
            await async_session.execute(insert(business_table).values(id=1, data="test_data"))
            await repo.create(event)
            simulate_crash()
    except RuntimeError:
        pass

    # Assert: both rows must be absent after the rollback
    async_session_maker = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session_maker() as verify_session:
        business_result = await verify_session.execute(select(business_table))
        assert business_result.scalar_one_or_none() is None

        outbox_result = await verify_session.execute(
            select(ConcreteOutboxEvent).where(ConcreteOutboxEvent.id == event.id)
        )
        assert outbox_result.scalar_one_or_none() is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__commit_transaction__persists_both_business_and_outbox_data(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(async_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_type="TestAggregate",
        aggregate_id=uuid4(),
        event_type="TestEventCommit",
        topic="test-topic",
        partition_key="test-key",
        payload={"data": "committed"},
    )

    # Act
    async with async_session.begin():
        await async_session.execute(insert(business_table).values(id=2, data="committed_data"))
        await repo.create(event)

    # Assert
    async_session.expire_all()
    business_result = await async_session.execute(select(business_table).where(business_table.c.id == 2))
    assert business_result.scalar_one_or_none() is not None

    outbox_result = await async_session.execute(select(ConcreteOutboxEvent).where(ConcreteOutboxEvent.id == event_id))
    outbox_row = outbox_result.scalar_one_or_none()
    assert outbox_row is not None
    assert outbox_row.event_type == "TestEventCommit"


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__upsert_duplicate_idempotency_key__does_not_abort_transaction(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(async_session, model_class=ConcreteOutboxEvent)
    key = f"upsert-key-{uuid4()}"
    event1 = OutboxEvent(
        id=uuid4(),
        aggregate_type="TestAggregate",
        aggregate_id=uuid4(),
        event_type="TestUpsert",
        topic="test-topic",
        partition_key="test-key",
        payload={"attempt": 1},
        idempotency_key=key,
    )

    # Act
    async with async_session.begin():
        created1 = await repo.create(event1)
        assert created1.id == event1.id

        event2 = OutboxEvent(
            id=uuid4(),
            aggregate_type="TestAggregate",
            aggregate_id=uuid4(),
            event_type="TestUpsert",
            topic="test-topic",
            partition_key="test-key",
            payload={"attempt": 2},
            idempotency_key=key,
        )
        existing = await repo.create(event2)

        # Assert: duplicate resolves to first event; transaction remains open
        assert existing.id == created1.id
        count_result = await async_session.execute(
            select(ConcreteOutboxEvent).where(ConcreteOutboxEvent.idempotency_key == key)
        )
        assert len(count_result.scalars().all()) == 1
