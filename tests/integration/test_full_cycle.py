"""End-to-end integration test for Transactional Outbox and Inbox."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box import (
    AckStrategy,
    EventHandlerResult,
    EventRouter,
    InboxConsumerRunner,
    InboxEvent,
    InboxEventRepository,
    OmniBoxDomainService,
    OutboxEvent,
    OutboxPublisher,
)
from omni_box.core.models.enums import EventStatus
from omni_box.core.protocols import ConsumedMessage, NullAckHandle
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol
from omni_box.infra.storage.postgres import (
    PostgresInboxRepository,
    PostgresOutboxRepository,
)
from tests.models import ConcreteInboxEvent, ConcreteOutboxEvent

pytestmark = pytest.mark.integration


class FakeBroker:
    """In-memory broker to bridge Outbox and Inbox in tests."""

    def __init__(self):
        self.queue: asyncio.Queue[Any] = asyncio.Queue()

    async def publish(self, event: OutboxEvent, repo: Any):
        await self.queue.put(
            {
                "id": str(event.id),
                "event_type": event.event_type,
                "topic": event.topic,
                "payload": event.payload,
                "message_id": str(event.id),
                "source": "test-source",
            }
        )

    async def getone(self):
        return await self.queue.get()

    async def start(self):
        pass

    async def stop(self):
        pass

    async def commit(self, handle):
        pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_inbox_full_cycle__event_created_published_and_consumed__inbox_row_completed(
    async_session: AsyncSession,
) -> None:
    # Arrange
    domain = OmniBoxDomainService()
    broker = FakeBroker()
    outbox_repo = PostgresOutboxRepository(async_session, model_class=ConcreteOutboxEvent)
    inbox_repo = PostgresInboxRepository(async_session, model_class=ConcreteInboxEvent)
    router = EventRouter()
    handler_called = asyncio.Event()
    received_payload: dict[str, Any] = {}

    async def test_handler(event: InboxEvent, repo: InboxEventRepository, **dependencies: Any) -> EventHandlerResult:
        nonlocal received_payload
        received_payload = event.payload
        handler_called.set()
        return EventHandlerResult(processed=True, success=True)

    router.register_handler(event_type="order.created", topic="orders", handler=test_handler)

    event_id = uuid4()
    outbox_event = domain.create_outbox_event(
        aggregate_type="order",
        aggregate_id=uuid4(),
        event_type="order.created",
        topic="orders",
        partition_key="key-123",
        payload={"order_id": "123", "amount": 100},
    ).model_copy(update={"id": event_id})

    async with async_session.begin():
        await outbox_repo.create(outbox_event)

    publisher = OutboxPublisher(repo=outbox_repo, broker=broker)  # type: ignore

    class RealBrokerAdapter:
        async def publish(self, event: Any, repo: Any) -> None:
            await broker.publish(event, repo)

    publisher._broker = RealBrokerAdapter()  # type: ignore

    # Act: publish outbox batch
    res = await publisher.publish_batch(worker_id="worker-out", batch_size=10)
    assert event_id in res.processed_event_ids

    class RealConsumerAdapter:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def getone(self) -> ConsumedMessage:
            raw = await broker.getone()
            return ConsumedMessage(
                message_id=raw["message_id"],
                source=raw["topic"],
                event_type=raw["event_type"],
                payload=raw["payload"],
                ack_handle=NullAckHandle(),
            )

    class SimpleTransactionProvider(InboxTransactionProviderProtocol):
        @asynccontextmanager
        async def transaction(self) -> Any:
            yield inbox_repo

    runner = InboxConsumerRunner(
        consumer=RealConsumerAdapter(),  # type: ignore
        transaction_provider=SimpleTransactionProvider(),
        handler=lambda event, repo, **deps: router.dispatch(event, "orders", repo, **deps),
        worker_id="worker-in",
        consumer_group="test-group",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )
    await runner.start()
    consume_res = await runner.process_one()
    await runner.stop()

    # Assert
    assert consume_res.processed is True
    assert handler_called.is_set()
    assert received_payload["order_id"] == "123"
    assert consume_res.event_id is not None
    inbox_row = await inbox_repo.get_by_id(consume_res.event_id)
    assert inbox_row is not None
    assert inbox_row.status == EventStatus.COMPLETED
