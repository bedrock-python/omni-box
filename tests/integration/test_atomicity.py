"""Integration tests for outbox and inbox atomicity guarantees."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, func, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from omni_box import InboxConsumerRunner, InboxEvent, InboxEventRepository
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.protocols import ConsumedMessage, NullAckHandle
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol
from omni_box.infra.storage.postgres import PostgresInboxRepository, PostgresOutboxRepository
from tests.models import ConcreteInboxEvent, ConcreteOutboxEvent

pytestmark = pytest.mark.integration


# Define test business metadata globally
test_metadata = MetaData()
business_table = Table(
    "test_business_data",
    test_metadata,
    Column("id", Integer, primary_key=True),
    Column("data", String(255)),
)


class OneMessageConsumer:
    """EventConsumer that hands out the same message on every ``getone``."""

    def __init__(self, message: ConsumedMessage) -> None:
        self._message = message

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def getone(self) -> ConsumedMessage:
        return self._message


class SessionPerTransactionProvider(InboxTransactionProviderProtocol):
    """Opens a fresh session and transaction per call, the way a service would."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[InboxEventRepository]:
        async with self._session_factory() as session, session.begin():
            yield PostgresInboxRepository(session, model_class=ConcreteInboxEvent)


def _make_message() -> ConsumedMessage:
    return ConsumedMessage(
        message_id=f"msg-{uuid4()}",
        source="orders",
        event_type="order.created",
        payload={"order_id": "42"},
        ack_handle=NullAckHandle(),
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test__inbox_runner__handler_writes_through_repo_session__business_and_inbox_rows_commit_together(
    db_engine: AsyncEngine,
) -> None:
    # Arrange
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    message = _make_message()

    async def handler(event: InboxEvent, repo: InboxEventRepository) -> None:
        assert isinstance(repo, PostgresInboxRepository)
        await repo.session.execute(insert(business_table).values(id=1, data=event.payload["order_id"]))

    runner = InboxConsumerRunner(
        consumer=OneMessageConsumer(message),
        transaction_provider=SessionPerTransactionProvider(session_factory),
        handler=handler,
        worker_id="inbox-1",
        consumer_group="billing",
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert: the business row and the inbox row landed in one commit
    assert result.processed is True
    assert result.committed is True
    async with session_factory() as verify_session:
        business_rows = (await verify_session.execute(select(business_table))).all()
        inbox_row = (
            await verify_session.execute(
                select(ConcreteInboxEvent).where(ConcreteInboxEvent.message_id == message.message_id)
            )
        ).scalar_one_or_none()
    assert [row.data for row in business_rows] == ["42"]
    assert inbox_row is not None
    assert inbox_row.status == EventStatus.COMPLETED


@pytest.mark.integration
@pytest.mark.asyncio
async def test__inbox_runner__handler_raises_after_writing_through_repo_session__both_rows_roll_back_together(
    db_engine: AsyncEngine,
) -> None:
    # Arrange
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    message = _make_message()
    rows_seen_inside_transaction: list[int] = []

    async def handler(event: InboxEvent, repo: InboxEventRepository) -> None:
        assert isinstance(repo, PostgresInboxRepository)
        await repo.session.execute(insert(business_table).values(id=1, data=event.payload["order_id"]))
        count = (await repo.session.execute(select(func.count()).select_from(business_table))).scalar_one()
        rows_seen_inside_transaction.append(count)
        raise RuntimeError("Simulated crash")

    runner = InboxConsumerRunner(
        consumer=OneMessageConsumer(message),
        transaction_provider=SessionPerTransactionProvider(session_factory),
        handler=handler,
        worker_id="inbox-1",
        consumer_group="billing",
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert: the handler saw its own row, and the rollback took it away with the inbox row
    assert rows_seen_inside_transaction == [1]
    assert result.processed is False
    assert result.committed is False
    async with session_factory() as verify_session:
        business_rows = (await verify_session.execute(select(business_table))).all()
        inbox_row = (
            await verify_session.execute(
                select(ConcreteInboxEvent).where(ConcreteInboxEvent.message_id == message.message_id)
            )
        ).scalar_one_or_none()
    assert business_rows == []
    assert inbox_row is None
