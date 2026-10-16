"""``InboxConsumerRunner`` against a real Kafka broker and PostgreSQL: what gets redelivered."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

import orjson
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from sqlalchemy import Column, Integer, MetaData, String, Table, func, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from omni_box import AckStrategy, InboxConsumerRunner, InboxEvent, InboxEventRepository
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol
from omni_box.infra.brokers.kafka import KafkaEventConsumer
from omni_box.infra.storage.postgres import PostgresInboxRepository
from tests.models import ConcreteInboxEvent

pytestmark = pytest.mark.integration

_metadata = MetaData()
_invoices = Table(
    "test_runner_invoices",
    _metadata,
    Column("id", Integer, primary_key=True),
    Column("order_id", String(32), nullable=False),
)

Handler = Callable[[InboxEvent, InboxEventRepository], Awaitable[None]]


@pytest.fixture
async def sessions(db_engine: AsyncEngine) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    async with db_engine.begin() as conn:
        await conn.run_sync(_metadata.create_all)
    yield async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    async with db_engine.begin() as conn:
        await conn.run_sync(_metadata.drop_all)


class _Transactions(InboxTransactionProviderProtocol):
    """One session per message; the transaction numbered ``fail_on`` cannot open, as in a database blip."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], fail_on: int | None = None) -> None:
        self._sessions = sessions
        self._fail_on = fail_on
        self.opened = 0

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[InboxEventRepository]:
        self.opened += 1
        if self.opened == self._fail_on:
            raise ConnectionResetError("connection to the database was lost")
        async with self._sessions.begin() as session:
            yield PostgresInboxRepository(session, model_class=ConcreteInboxEvent)


async def _write_invoice(event: InboxEvent, repo: InboxEventRepository) -> None:
    assert isinstance(repo, PostgresInboxRepository)
    await repo.session.execute(insert(_invoices).values(order_id=event.payload["order_id"]))


async def _invoices_by_order(sessions: async_sessionmaker[AsyncSession]) -> dict[str, int]:
    async with sessions() as session:
        rows = await session.execute(select(_invoices.c.order_id, func.count()).group_by(_invoices.c.order_id))
        return dict(rows.tuples().all())


async def _produce(producer: AIOKafkaProducer, topic: str, orders: list[str]) -> None:
    for order in orders:
        await producer.send_and_wait(topic, orjson.dumps({"order_id": order}))


def _runner(
    bootstrap: str,
    topic: str,
    group: str,
    handler: Handler,
    transactions: _Transactions,
) -> InboxConsumerRunner:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    return InboxConsumerRunner(
        KafkaEventConsumer(consumer),
        transactions,
        handler,
        worker_id="billing-1",
        consumer_group=group,
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )


async def _committed_offset(bootstrap: str, topic: str, group: str) -> int | None:
    check = AIOKafkaConsumer(bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False)
    await check.start()
    try:
        return await check.committed(TopicPartition(topic, 0))
    finally:
        await check.stop()


async def test__inbox_runner__run_forever_after_handler_and_persist_failures__redelivers_both_and_loses_nothing(
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_producer: AIOKafkaProducer,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange: A's handler fails once, B's transaction fails once, C goes through
    group = f"billing-{uuid4().hex[:6]}"
    await _produce(kafka_producer, kafka_topic, ["A", "B", "C"])
    failed_once: set[str] = set()

    async def flaky(event: InboxEvent, repo: InboxEventRepository) -> None:
        await _write_invoice(event, repo)
        order = str(event.payload["order_id"])
        if order == "A" and order not in failed_once:
            failed_once.add(order)
            raise RuntimeError("transient handler failure")

    runner = _runner(kafka_bootstrap, kafka_topic, group, flaky, _Transactions(sessions, fail_on=3))
    await runner.start()
    task = asyncio.create_task(runner.run_forever())

    # Act
    try:

        async def all_invoiced() -> None:
            while len(await _invoices_by_order(sessions)) < 3:
                await asyncio.sleep(0.1)

        await asyncio.wait_for(all_invoiced(), timeout=20)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.stop()

    # Assert: every order invoiced exactly once, and the offset only covers what was committed
    assert await _invoices_by_order(sessions) == {"A": 1, "B": 1, "C": 1}
    assert await _committed_offset(kafka_bootstrap, kafka_topic, group) == 3


async def test__inbox_runner__failing_record_then_restart__the_failing_record_comes_back_first(
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_producer: AIOKafkaProducer,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange: A fails for as long as the first process runs, B behind it is fine
    group = f"billing-{uuid4().hex[:6]}"
    await _produce(kafka_producer, kafka_topic, ["A", "B"])
    attempts: list[str] = []

    async def poisoned_by_a(event: InboxEvent, repo: InboxEventRepository) -> None:
        attempts.append(str(event.payload["order_id"]))
        if event.payload["order_id"] == "A":
            raise RuntimeError("handler bug for A")
        await _write_invoice(event, repo)

    first = _runner(kafka_bootstrap, kafka_topic, group, poisoned_by_a, _Transactions(sessions))
    await first.start()
    task = asyncio.create_task(first.run_forever())
    try:

        async def tried_twice() -> None:
            while len(attempts) < 2:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(tried_twice(), timeout=20)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await first.stop()

    # Act: the handler is fixed and the process restarts
    restarted = _runner(kafka_bootstrap, kafka_topic, group, _write_invoice, _Transactions(sessions))
    await restarted.start()
    try:
        first_after_restart = await asyncio.wait_for(restarted.process_one(), timeout=20)
        second_after_restart = await asyncio.wait_for(restarted.process_one(), timeout=20)
    finally:
        await restarted.stop()

    # Assert: A held its partition instead of being read past, and nothing was committed over it
    assert set(attempts) == {"A"}
    assert first_after_restart.committed is True
    assert second_after_restart.committed is True
    assert await _invoices_by_order(sessions) == {"A": 1, "B": 1}
    assert await _committed_offset(kafka_bootstrap, kafka_topic, group) == 2
