"""A broker outage against the real outbox table: the attempt budget is not spent on it.

The relay here is the documented one -- ``OutboxPublisher`` over
``PostgresOutboxRepository`` and ``KafkaEventPublisher`` -- and the broker is a
producer that behaves the way aiokafka does while the container is paused: it
neither answers nor refuses. Cycles run back to back rather than on a timer, so
the outage from the report takes milliseconds instead of four minutes.
"""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
from aiokafka.errors import MessageSizeTooLargeError, NodeNotReadyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from omni_box import OmniBoxDomainService, OutboxEvent, OutboxPublisher
from omni_box.core.converters import EnvelopeEventConverter
from omni_box.core.models.enums import EventStatus
from omni_box.core.services.results import BatchProcessingResult
from omni_box.infra.brokers.kafka import KafkaEventPublisher
from omni_box.infra.storage.postgres import PostgresOutboxRepository
from omni_box.utils import utc_now
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.integration

EVENTS = 20
"""The size of the reporter's backlog."""

MAX_ATTEMPTS = 6
"""The default budget, and the number of cycles that used to burn all of it."""

CYCLES = 7
"""One cycle more than the budget: on 0.2.0 the seventh already found nothing left to publish."""

TOPIC = "orders.events"


class _StubClient:
    """The producer's client, asked for a metadata refresh when a topic is unknown."""

    def __init__(self, answers: bool) -> None:
        self.answers = answers

    async def force_metadata_update(self) -> bool:
        return self.answers


class _StubProducer:
    """An ``AIOKafkaProducer`` that raises what the real one raises, or records the send.

    ``failure`` is what the broker gives back; setting it to ``None`` is the
    broker coming back.
    """

    def __init__(self, failure: BaseException | None = None, *, answers_metadata: bool = False) -> None:
        self.failure = failure
        self.client = _StubClient(answers_metadata)
        self.sent: list[bytes | None] = []

    async def send_and_wait(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        if self.failure is not None:
            raise self.failure
        self.sent.append(key)


def _broker(producer: _StubProducer) -> KafkaEventPublisher:
    """The reporter's wiring, minus the adapter's own retry loop.

    ``max_infra_retries=0`` keeps a cycle to a single probe and to no sleeping;
    the retry loop itself is covered by the adapter's unit tests.
    """
    return KafkaEventPublisher(producer, EnvelopeEventConverter(), max_infra_retries=0)


async def _create_events(session_factory: async_sessionmaker[AsyncSession]) -> list[UUID]:
    """Fill the outbox the way a service would, in one transaction."""
    domain = OmniBoxDomainService(max_attempts=MAX_ATTEMPTS)
    ids: list[UUID] = []
    async with session_factory() as session, session.begin():
        repo = PostgresOutboxRepository(session, model_class=ConcreteOutboxEvent)
        for i in range(EVENTS):
            event = domain.create_outbox_event(
                aggregate_type="order",
                aggregate_id=uuid4(),
                event_type="order.created",
                topic=TOPIC,
                partition_key=f"order-{i}",
                payload={"n": i},
            )
            await repo.create(event)
            ids.append(event.id)
    return ids


async def _relay_cycle(
    session_factory: async_sessionmaker[AsyncSession],
    producer: _StubProducer,
) -> BatchProcessingResult[OutboxEvent]:
    """One tick of a relay loop: fetch, publish, commit."""
    async with session_factory() as session, session.begin():
        publisher = OutboxPublisher(
            PostgresOutboxRepository(session, model_class=ConcreteOutboxEvent),
            _broker(producer),
            publish_timeout=5.0,
        )
        return await publisher.publish_batch(worker_id="relay-1", batch_size=100)


async def _rows(session_factory: async_sessionmaker[AsyncSession], ids: list[UUID]) -> list[OutboxEvent]:
    """Read the rows back as domain entities, in the order they were created."""
    async with session_factory() as session:
        repo = PostgresOutboxRepository(session, model_class=ConcreteOutboxEvent)
        rows = [await repo.get_by_id(event_id) for event_id in ids]
    return [row for row in rows if row is not None]


@pytest.fixture
def session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """A session per relay cycle, the way a long-running relay works."""
    return async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)


async def test__relay__broker_unreachable_for_longer_than_the_budget__rows_stay_pending(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The reporter's scenario: an outage that outlasts ``max_attempts`` cycles."""
    # Arrange
    ids = await _create_events(session_factory)
    producer = _StubProducer(NodeNotReadyError("node 1 is not ready"))
    created_before = utc_now()

    # Act
    per_cycle = []
    for _ in range(CYCLES):
        await _relay_cycle(session_factory, producer)
        per_cycle.append({(row.status, row.attempts_made) for row in await _rows(session_factory, ids)})

    # Assert
    assert per_cycle == [{(EventStatus.PENDING, 0)}] * CYCLES
    rows = await _rows(session_factory, ids)
    assert all(row.locked_at is None for row in rows)
    assert all(row.last_error == "Kafka broker unreachable: NodeNotReadyError: node 1 is not ready" for row in rows)
    # One probe per cycle, not one per row: the rest of the batch is deferred as
    # soon as the broker turns out to be unreachable, and keeps its schedule.
    assert producer.sent == []
    rescheduled = [row.id for row in rows if row.scheduled_at > created_before]
    assert len(rescheduled) == CYCLES


async def test__relay__broker_back_after_an_outage__next_cycle_publishes_everything(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """No ``requeue_failed`` and no operator: the backlog drains on the first cycle after the outage."""
    # Arrange
    ids = await _create_events(session_factory)
    producer = _StubProducer(NodeNotReadyError("node 1 is not ready"))
    for _ in range(CYCLES):
        await _relay_cycle(session_factory, producer)

    # Act
    producer.failure = None  # Kafka is back
    await asyncio.sleep(1.5)  # the probed rows were put a second ahead
    result = await _relay_cycle(session_factory, producer)

    # Assert
    assert len(producer.sent) == EVENTS
    assert set(result.processed_event_ids) == set(ids)
    rows = await _rows(session_factory, ids)
    assert [row.status for row in rows] == [EventStatus.COMPLETED] * EVENTS
    assert [row.attempts_made for row in rows] == [0] * EVENTS


async def test__relay__broker_answers_and_rejects_the_record__every_row_spends_an_attempt(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A broker that is there and says no is about the row: the budget is still the right tool."""
    # Arrange
    ids = await _create_events(session_factory)
    producer = _StubProducer(MessageSizeTooLargeError("The message is 2000000 bytes when serialized"))

    # Act
    result = await _relay_cycle(session_factory, producer)

    # Assert
    assert {failure.event_id for failure in result.failed_counted} == set(ids)
    assert result.failed_noncounted == []
    rows = await _rows(session_factory, ids)
    assert [row.status for row in rows] == [EventStatus.PENDING] * EVENTS
    assert [row.attempts_made for row in rows] == [1] * EVENTS
    assert all("MessageSizeTooLargeError" in (row.last_error or "") for row in rows)
