"""Per-key order and retry pacing of the outbox relay, against the real table.

The relay is ``OutboxPublisher`` over ``PostgresOutboxRepository``, one session and
transaction per cycle, the way the user guide runs it. The broker records what it
sends, in order, and fails the sends it is told to fail.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from omni_box import OmniBoxDomainService, OutboxEvent, OutboxPublisher, TransientError
from omni_box.core.models.enums import EventStatus
from omni_box.core.services.results import BatchProcessingResult
from omni_box.infra.storage.postgres import PostgresOutboxRepository
from omni_box.utils import utc_now
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.integration

ORDER_EVENTS = ("order.created", "order.paid", "order.cancelled")


class _Broker:
    """Records what it sends; ``fail`` decides, per event, what to raise instead."""

    def __init__(self, fail: Callable[[OutboxEvent], BaseException | None] | None = None) -> None:
        self.fail = fail or (lambda event: None)
        self.sent: list[str] = []

    async def publish(self, event: OutboxEvent, repo: object) -> None:
        error = self.fail(event)
        if error is not None:
            raise error
        self.sent.append(event.event_type)


def _fail_first_send(error: BaseException) -> Callable[[OutboxEvent], BaseException | None]:
    pending = [error]

    def fail(event: OutboxEvent) -> BaseException | None:
        return pending.pop() if pending else None

    return fail


@pytest.fixture
def session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)


async def _write(
    session_factory: async_sessionmaker[AsyncSession],
    event_type: str,
    *,
    partition_key: str,
    topic: str = "orders",
    max_attempts: int | None = None,
    scheduled_in: timedelta | None = None,
) -> UUID:
    """One event in one transaction, the way a service writes them."""
    event = OmniBoxDomainService().create_outbox_event(
        aggregate_type="order",
        aggregate_id=uuid4(),
        event_type=event_type,
        topic=topic,
        partition_key=partition_key,
        payload={"key": partition_key},
        max_attempts=max_attempts,
        scheduled_at=utc_now() + scheduled_in if scheduled_in else None,
    )
    async with session_factory() as session, session.begin():
        await PostgresOutboxRepository(session, model_class=ConcreteOutboxEvent).create(event)
    return event.id


async def _write_order(session_factory: async_sessionmaker[AsyncSession], partition_key: str = "order-1") -> None:
    for event_type in ORDER_EVENTS:
        await _write(session_factory, event_type, partition_key=partition_key)


async def _cycle(
    session_factory: async_sessionmaker[AsyncSession],
    broker: _Broker,
    *,
    worker_id: str = "relay-1",
    batch_size: int = 20,
    retry_backoff_base_seconds: float | None = None,
) -> BatchProcessingResult[OutboxEvent]:
    """One relay cycle; the backoff is the default unless a test sets it."""
    options = {} if retry_backoff_base_seconds is None else {"retry_backoff_base_seconds": retry_backoff_base_seconds}
    async with session_factory() as session, session.begin():
        publisher = OutboxPublisher(
            PostgresOutboxRepository(session, model_class=ConcreteOutboxEvent), broker, **options
        )
        return await publisher.publish_batch(worker_id=worker_id, batch_size=batch_size)


async def _drain(session_factory: async_sessionmaker[AsyncSession], broker: _Broker, **cycle_options: object) -> None:
    """Run cycles until one publishes nothing: a key goes out one event per cycle."""
    for _ in range(20):
        result = await _cycle(session_factory, broker, **cycle_options)  # type: ignore[arg-type]
        if not result.processed_event_ids:
            return
    raise AssertionError("the relay did not drain in 20 cycles")


async def _row(session_factory: async_sessionmaker[AsyncSession], event_id: UUID) -> ConcreteOutboxEvent:
    async with session_factory() as session:
        query = select(ConcreteOutboxEvent).where(ConcreteOutboxEvent.id == event_id)
        return (await session.execute(query)).scalar_one()


# ---------- #60: one relay, one failed publish ----------


async def test__relay__transient_failure_on_the_first_event__the_key_still_goes_out_in_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write_order(session_factory)
    broker = _Broker(_fail_first_send(TransientError("broker unreachable")))

    # Act
    await _cycle(session_factory, broker)
    await asyncio.sleep(1.1)  # the probed row is put a second ahead
    await _drain(session_factory, broker)

    # Assert
    assert broker.sent == list(ORDER_EVENTS)


async def test__relay__counted_failure_on_the_first_event__its_successors_wait_for_its_retry(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write_order(session_factory)
    broker = _Broker(_fail_first_send(ValueError("record rejected")))

    # Act
    await _cycle(session_factory, broker, retry_backoff_base_seconds=0.4)
    await _cycle(session_factory, broker, retry_backoff_base_seconds=0.4)
    sent_while_backing_off = list(broker.sent)
    await asyncio.sleep(0.5)
    await _drain(session_factory, broker, retry_backoff_base_seconds=0.4)

    # Assert
    assert sent_while_backing_off == []
    assert broker.sent == list(ORDER_EVENTS)


async def test__relay__counted_failure_with_backoff_off__the_key_still_goes_out_in_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write_order(session_factory)
    broker = _Broker(_fail_first_send(ValueError("record rejected")))

    # Act: the failing cycle publishes nothing, the next ones the whole key
    await _cycle(session_factory, broker, retry_backoff_base_seconds=0)
    await _drain(session_factory, broker, retry_backoff_base_seconds=0)

    # Assert
    assert broker.sent == list(ORDER_EVENTS)


async def test__relay__failure_on_one_key__other_keys_are_not_held_back(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write_order(session_factory, "order-1")
    await _write(session_factory, "order.created", partition_key="order-2")
    broker = _Broker(lambda event: ValueError("rejected") if event.partition_key == "order-1" else None)

    # Act
    await _cycle(session_factory, broker)

    # Assert
    assert broker.sent == ["order.created"]


async def test__relay__several_events_of_one_key__one_per_cycle_in_write_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write_order(session_factory, "order-1")
    await _write_order(session_factory, "order-2")
    broker = _Broker()

    # Act
    per_cycle = []
    for _ in range(3):
        result = await _cycle(session_factory, broker)
        per_cycle.append(len(result.processed_event_ids))

    # Assert: each cycle takes the head of both keys
    assert per_cycle == [2, 2, 2]
    assert broker.sent == [event for event in ORDER_EVENTS for _ in range(2)]


async def test__relay__delayed_event__does_not_hold_back_the_events_written_after_it(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write(session_factory, "order.reminder", partition_key="order-1", scheduled_in=timedelta(hours=1))
    await _write(session_factory, "order.paid", partition_key="order-1")
    broker = _Broker()

    # Act
    await _cycle(session_factory, broker)

    # Assert
    assert broker.sent == ["order.paid"]


async def test__relay__terminally_failed_event__its_successors_go_out_without_it(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    failed_id = await _write(session_factory, "order.created", partition_key="order-1", max_attempts=1)
    await _write(session_factory, "order.paid", partition_key="order-1")
    broker = _Broker(lambda event: ValueError("rejected") if event.event_type == "order.created" else None)

    # Act
    await _cycle(session_factory, broker)
    await _drain(session_factory, broker)

    # Assert
    assert (await _row(session_factory, failed_id)).status == EventStatus.FAILED
    assert broker.sent == ["order.paid"]


# ---------- #60: several relays ----------


async def test__two_relays__the_head_of_a_key_is_held__the_other_relay_does_not_take_its_successors(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write_order(session_factory, "order-1")
    other = await _write(session_factory, "order.created", partition_key="order-2")

    async with session_factory() as relay_1, relay_1.begin():
        held = await PostgresOutboxRepository(relay_1, model_class=ConcreteOutboxEvent).fetch_and_lock_pending(
            limit=1, worker_id="relay-1"
        )

        # Act: relay 2 runs while relay 1 is still publishing its head of order-1
        async with session_factory() as relay_2, relay_2.begin():
            taken = await PostgresOutboxRepository(relay_2, model_class=ConcreteOutboxEvent).fetch_and_lock_pending(
                limit=20, worker_id="relay-2"
            )

    # Assert
    assert [event.event_type for event in held] == ["order.created"]
    assert [event.id for event in taken] == [other]


async def test__two_relays__head_committed_by_one__the_other_takes_the_rest_in_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    await _write_order(session_factory)
    broker = _Broker()

    # Act
    await _cycle(session_factory, broker, worker_id="relay-1", batch_size=1)
    await _drain(session_factory, broker, worker_id="relay-2", batch_size=20)

    # Assert
    assert broker.sent == list(ORDER_EVENTS)


# ---------- #61: counted failures are spaced out ----------


async def test__busy_relay__counted_failure__is_not_retried_until_its_backoff_has_passed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange: the reporter's busy relay -- other traffic keeps every cycle non-empty
    rejected = await _write(session_factory, "x", partition_key="k", topic="invoices")
    broker = _Broker(lambda event: PermissionError("not authorized") if event.topic == "invoices" else None)

    # Act
    started = utc_now()
    for _ in range(6):
        await _write(session_factory, "x", partition_key=str(uuid4()), topic="orders")
        await _cycle(session_factory, broker)

    # Assert: one attempt spent, and the next one at least half the base (10 s) away
    row = await _row(session_factory, rejected)
    assert row.status == EventStatus.PENDING
    assert row.attempts_made == 1
    assert row.scheduled_at >= started + timedelta(seconds=5)
    assert broker.sent == ["x"] * 6


async def test__busy_relay__backoff_off__spends_the_budget_in_six_cycles(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    rejected = await _write(session_factory, "x", partition_key="k", topic="invoices")
    broker = _Broker(lambda event: PermissionError("not authorized") if event.topic == "invoices" else None)

    # Act
    for _ in range(6):
        await _cycle(session_factory, broker, retry_backoff_base_seconds=0)

    # Assert: retry_backoff_base_seconds=0 is the behaviour from before the backoff
    row = await _row(session_factory, rejected)
    assert row.status == EventStatus.FAILED
    assert row.attempts_made == 6


async def test__mark_failed_without_a_time__keeps_the_schedule_like_bulk_mark_failed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Arrange
    event_id = await _write(session_factory, "x", partition_key="k")
    scheduled_at = (await _row(session_factory, event_id)).scheduled_at
    await asyncio.sleep(0.05)

    # Act
    async with session_factory() as session, session.begin():
        repo = PostgresOutboxRepository(session, model_class=ConcreteOutboxEvent)
        await repo.fetch_and_lock_pending(limit=1, worker_id="relay-1")
        await repo.mark_failed(event_id, "deferred", "relay-1", next_retry_at=None, count_as_attempt=False)

    # Assert
    assert (await _row(session_factory, event_id)).scheduled_at == scheduled_at


async def test__outbox_tables__carry_the_index_behind_the_per_key_order(db_engine: AsyncEngine) -> None:
    # Arrange / Act
    async with db_engine.connect() as conn:
        names = await conn.run_sync(
            lambda sync: {
                table: {index["name"] for index in inspect(sync).get_indexes(table)}
                for table in ("outbox_events", "outbox_events_partitioned")
            }
        )

    # Assert
    assert "idx_outbox_events_pending_key_order" in names["outbox_events"]
    assert "idx_outbox_events_p_pending_key_order" in names["outbox_events_partitioned"]
