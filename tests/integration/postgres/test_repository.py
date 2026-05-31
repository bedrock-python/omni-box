from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.exceptions import EventConcurrentUpdateError
from omni_box.core.models.entities import InboxEvent, OutboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.models.types import EventFailureUpdate
from omni_box.infra.storage.postgres import PostgresInboxRepository, PostgresOutboxRepository
from omni_box.infra.storage.postgres.repositories.base import _as_list
from omni_box.utils import utc_now
from tests.models import ConcreteInboxEvent, ConcreteInboxEventPartitioned, ConcreteOutboxEvent

pytestmark = pytest.mark.integration


# ---------- helpers ----------


def _make_inbox_domain_event(**kwargs: Any) -> InboxEvent:
    now = utc_now()
    data: dict[str, Any] = {
        "message_id": "msg-1",
        "consumer_group": "cg-1",
        "source": "orders",
        "event_type": "order.created",
        "payload": {"a": 1},
        "created_at": now,
        "scheduled_at": now,
    }
    data.update(kwargs)
    return InboxEvent.model_validate(data)


def _mock_inbox_db_row(event_id: UUID, message_id: str, consumer_group: str, now: datetime) -> MagicMock:
    m = MagicMock()
    m.id = event_id
    m.message_id = message_id
    m.consumer_group = consumer_group
    m.source = "orders"
    m.event_type = "order.created"
    m.payload = {"a": 1}
    m.headers = None
    m.status = EventStatus.PENDING
    m.attempts_made = 0
    m.max_attempts = 6
    m.last_error = None
    m.trace_id = None
    m.idempotency_key = None
    m.correlation_id = None
    m.causation_id = None
    m.schema_version = "1"
    m.created_at = now
    m.scheduled_at = now
    m.completed_at = None
    m.locked_at = None
    m.locked_by = None
    return m


# ---------- create ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__create_with_full_fields__persists_and_returns_event(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    now = utc_now()
    event_id = uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_type="order",
        aggregate_id=uuid4(),
        event_type="order.created",
        topic="orders",
        partition_key="key",
        payload={"foo": "bar"},
        idempotency_key="idem-1",
        correlation_id="corr-1",
        causation_id="caus-1",
        schema_version="2",
        created_at=now,
        scheduled_at=now,
    )

    # Act
    created = await repo.create(event)

    # Assert
    assert created.id == event_id
    assert created.idempotency_key == "idem-1"
    fetched = await repo.get_by_id(event_id)
    assert fetched is not None
    assert fetched.payload == {"foo": "bar"}


@pytest.mark.asyncio
async def test__outbox_repository__create_without_idempotency_key__inserts_then_selects() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_session.execute.return_value = mock_result
    mock_session.begin_nested = MagicMock()
    mock_session.begin_nested.return_value.__aenter__ = AsyncMock()
    mock_session.begin_nested.return_value.__aexit__ = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_type="order",
        aggregate_id=uuid4(),
        event_type="order.created",
        topic="orders",
        partition_key="key",
        payload={"foo": "bar"},
        idempotency_key=None,
    )
    mock_db_event = MagicMock()
    mock_result.scalar_one_or_none.side_effect = [None, mock_db_event]
    mock_db_event.id = event_id
    mock_db_event.aggregate_type = "order"
    mock_db_event.aggregate_id = event.aggregate_id
    mock_db_event.event_type = "order.created"
    mock_db_event.topic = "orders"
    mock_db_event.partition_key = "key"
    mock_db_event.payload = {"foo": "bar"}
    mock_db_event.headers = None
    mock_db_event.status = EventStatus.PENDING
    mock_db_event.attempts_made = 0
    mock_db_event.max_attempts = 6
    mock_db_event.last_error = None
    mock_db_event.trace_id = None
    mock_db_event.idempotency_key = None
    mock_db_event.correlation_id = None
    mock_db_event.causation_id = None
    mock_db_event.schema_version = "1"
    mock_db_event.created_at = utc_now()
    mock_db_event.scheduled_at = utc_now()
    mock_db_event.completed_at = None
    mock_db_event.locked_at = None
    mock_db_event.locked_by = None

    # Act
    await repo.create(event)

    # Assert
    assert mock_session.execute.call_count == 2
    assert mock_session.begin_nested.call_count == 0


@pytest.mark.asyncio
async def test__outbox_repository__create_both_insert_and_select_return_none__raises_concurrent_update_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_session.execute.return_value = mock_result
    mock_session.begin_nested = MagicMock()
    mock_session.begin_nested.return_value.__aenter__ = AsyncMock()
    mock_session.begin_nested.return_value.__aexit__ = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event = OutboxEvent(
        id=uuid4(),
        aggregate_type="order",
        aggregate_id=uuid4(),
        event_type="t",
        topic="tp",
        partition_key="k",
        payload={"_": 1},
    )
    mock_result.scalar_one_or_none.return_value = None

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError, match="Failed to create or retrieve"):
        await repo.create(event)


# ---------- validation ----------


@pytest.mark.asyncio
async def test__outbox_repository__invalid_fetch_args__raises_value_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act / Assert
    with pytest.raises(ValueError, match="limit must be greater than 0"):
        await repo.fetch_and_lock_pending(limit=0, worker_id="w")

    with pytest.raises(ValueError, match="cannot be empty"):
        await repo.fetch_and_lock_pending(limit=1, worker_id="  ")

    with pytest.raises(ValueError, match="stale_timeout_seconds must be greater than 0"):
        await repo.release_stale_locks(stale_timeout_seconds=0)

    with pytest.raises(ValueError, match="retention_days must be greater than 0"):
        await repo.delete_old_completed(retention_days=0)


@pytest.mark.asyncio
async def test__outbox_repository__session_execute_raises__propagates_exception() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_session.execute.side_effect = Exception("flush failed")
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act / Assert
    with pytest.raises(Exception, match="flush failed"):
        await repo.fetch_and_lock_pending(1, "w")
    with pytest.raises(Exception, match="flush failed"):
        await repo.refresh_lock(uuid4(), "w")
    with pytest.raises(Exception, match="flush failed"):
        await repo.bulk_mark_completed([uuid4()], "w")
    with pytest.raises(Exception, match="flush failed"):
        await repo.bulk_mark_failed([EventFailureUpdate(uuid4(), "e", None)], "w")
    with pytest.raises(Exception, match="flush failed"):
        await repo.release_stale_locks(60)
    with pytest.raises(Exception, match="flush failed"):
        await repo.force_unlock(uuid4(), "reason")
    with pytest.raises(Exception, match="flush failed"):
        await repo.bulk_release_locks([uuid4()], "w")
    with pytest.raises(Exception, match="flush failed"):
        await repo.requeue_failed(uuid4())
    with pytest.raises(Exception, match="flush failed"):
        await repo.delete_old_completed(7)


# ---------- force_unlock ----------


@pytest.mark.asyncio
async def test__outbox_repository__force_unlock_valid_reason__succeeds_or_raises_on_not_found() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()

    mock_result.scalar_one_or_none.return_value = event_id
    mock_session.execute.return_value = mock_result

    # Act
    await repo.force_unlock(event_id, "test reason")

    # Assert: no exception raised; now test not-found path
    mock_result.scalar_one_or_none.return_value = None
    with pytest.raises(EventConcurrentUpdateError):
        await repo.force_unlock(event_id, "test reason")

    with pytest.raises(ValueError, match="Reason for force unlock is too long"):
        await repo.force_unlock(event_id, "a" * 256)


# ---------- bulk_mark_failed ----------


@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_failed_no_retry_at__returns_updated_count() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [uuid4()]
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    count = await repo.bulk_mark_failed(
        [EventFailureUpdate(uuid4(), "err", None)], worker_id="w1", count_as_attempt=False
    )

    # Assert
    assert count == 1


@pytest.mark.asyncio
async def test__outbox_repository__misc_truncate_error_and_normalize_worker() -> None:
    # Arrange
    repo = PostgresOutboxRepository(
        session=AsyncMock(),
        model_class=ConcreteOutboxEvent,
        error_max_length=10,
        truncation_suffix="...",
    )

    # Act
    truncated = repo._truncate_error("a" * 20)
    normalized = repo._normalize_worker_id("  worker-1  ")

    # Assert
    assert truncated == "aaaaaaa..."
    assert normalized == "worker-1"
    with pytest.raises(ValueError):
        repo._normalize_worker_id("   ")


# ---------- get_by_id ----------


@pytest.mark.asyncio
async def test__outbox_repository__get_by_id_found_and_not_found__returns_entity_or_none() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    mock_db_event = MagicMock()
    mock_db_event.id = event_id
    mock_db_event.aggregate_type = "a"
    mock_db_event.aggregate_id = uuid4()
    mock_db_event.event_type = "e"
    mock_db_event.topic = "t"
    mock_db_event.partition_key = "k"
    mock_db_event.payload = {"_": 1}
    mock_db_event.headers = None
    mock_db_event.status = EventStatus.PENDING
    mock_db_event.attempts_made = 0
    mock_db_event.max_attempts = 6
    mock_db_event.last_error = None
    mock_db_event.trace_id = None
    mock_db_event.idempotency_key = None
    mock_db_event.correlation_id = None
    mock_db_event.causation_id = None
    mock_db_event.schema_version = "1"
    mock_db_event.created_at = utc_now()
    mock_db_event.scheduled_at = utc_now()
    mock_db_event.completed_at = None
    mock_db_event.locked_at = None
    mock_db_event.locked_by = None
    mock_result.scalar_one_or_none.return_value = mock_db_event
    mock_session.execute.return_value = mock_result

    # Act
    res = await repo.get_by_id(event_id)

    # Assert
    assert res is not None
    assert res.id == event_id

    mock_result.scalar_one_or_none.return_value = None
    res = await repo.get_by_id(event_id)
    assert res is None


# ---------- fetch_and_lock ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__fetch_and_lock_pending__returns_locked_event(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    now = utc_now()
    event_id = uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_type="order",
        aggregate_id=uuid4(),
        event_type="order.created",
        topic="orders",
        partition_key="key",
        payload={"foo": "bar"},
        idempotency_key="idem-1",
        correlation_id="corr-1",
        causation_id="caus-1",
        schema_version="2",
        created_at=now,
        scheduled_at=now,
    )
    await repo.create(event)

    # Act
    events = await repo.fetch_and_lock_pending(limit=10, worker_id="worker-1")

    # Assert
    assert len(events) == 1
    fetched_event = events[0]
    assert fetched_event.id == event_id
    assert fetched_event.idempotency_key == "idem-1"
    assert fetched_event.status == EventStatus.PENDING
    assert fetched_event.locked_by == "worker-1"


@pytest.mark.asyncio
async def test__outbox_repository__fetch_and_lock_no_events__returns_empty_list() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    # Act
    events = await repo.fetch_and_lock_pending(limit=10, worker_id="worker-1")

    # Assert
    assert len(events) == 0
    assert mock_session.execute.call_count == 1


@pytest.mark.asyncio
async def test__outbox_repository__fetch_and_lock_with_ttl__calls_session_once() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    # Act
    await repo.fetch_and_lock_pending(limit=10, worker_id="w1", ttl=300)

    # Assert
    assert mock_session.execute.call_count == 1


# ---------- refresh_lock ----------


@pytest.mark.asyncio
async def test__outbox_repository__refresh_lock_returns_id__returns_true() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = uuid4()
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    success = await repo.refresh_lock(uuid4(), worker_id="worker-1")

    # Assert
    assert success is True
    assert mock_session.execute.call_count == 1


@pytest.mark.asyncio
async def test__outbox_repository__refresh_lock_returns_none__returns_false() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    success = await repo.refresh_lock(uuid4(), "w1")

    # Assert
    assert success is False


# ---------- mark_completed ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__mark_completed__sets_status_completed(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_type="t",
        aggregate_id=uuid4(),
        event_type="e",
        topic="tp",
        partition_key="k",
        payload={"p": 1},
    )
    await repo.create(event)
    await repo.mark_processing(event_id, worker_id="worker-1")

    # Act
    await repo.mark_completed(event_id, worker_id="worker-1")

    # Assert
    fetched = await repo.get_by_id(event_id)
    assert fetched is not None
    assert fetched.status == EventStatus.COMPLETED
    assert fetched.completed_at is not None


@pytest.mark.asyncio
async def test__outbox_repository__mark_completed_row_not_found__raises_concurrent_update_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.mark_completed(uuid4(), worker_id="worker-1")


# ---------- bulk_mark_completed ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_completed__marks_all_events_completed(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    event_id1, event_id2 = uuid4(), uuid4()
    for eid, pk in [(event_id1, "k1"), (event_id2, "k2")]:
        await repo.create(
            OutboxEvent(
                aggregate_type="t",
                aggregate_id=uuid4(),
                event_type="e",
                topic="tp",
                payload={"p": 1},
                id=eid,
                partition_key=pk,
            )
        )
        await repo.mark_processing(eid, worker_id="w1")

    # Act
    count = await repo.bulk_mark_completed([event_id1, event_id2], worker_id="w1")

    # Assert
    assert count == 2
    for eid in [event_id1, event_id2]:
        f = await repo.get_by_id(eid)
        assert f is not None
        assert f.status == EventStatus.COMPLETED


@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_completed_empty_list__skips_execute() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    await repo.bulk_mark_completed([], worker_id="w1")

    # Assert
    assert mock_session.execute.call_count == 0


@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_completed_with_worker_id__calls_execute_once() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    event_id = uuid4()
    mock_result.scalars.return_value.all.return_value = [event_id]
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    await repo.bulk_mark_completed([event_id], worker_id="worker-123")

    # Assert
    assert mock_session.execute.call_count == 1


@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_completed_duplicate_ids__raises_value_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()

    # Act / Assert
    with pytest.raises(ValueError, match="must be unique"):
        await repo.bulk_mark_completed([event_id, event_id], worker_id="worker-1")


# ---------- mark_failed ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__mark_failed__sets_pending_with_incremented_attempt(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_type="t",
        aggregate_id=uuid4(),
        event_type="e",
        topic="tp",
        partition_key="k",
        payload={"p": 1},
    )
    await repo.create(event)
    await repo.mark_processing(event_id, worker_id="worker-1")

    # Act
    await repo.mark_failed(
        event_id, error="test error", worker_id="worker-1", next_retry_at=None, count_as_attempt=True
    )

    # Assert
    fetched = await repo.get_by_id(event_id)
    assert fetched is not None
    assert fetched.status == EventStatus.PENDING
    assert fetched.attempts_made == 1
    assert fetched.last_error is not None
    assert "test error" in fetched.last_error


@pytest.mark.asyncio
async def test__outbox_repository__mark_failed_row_not_found__raises_concurrent_update_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.mark_failed(uuid4(), error="err", worker_id="worker-1", next_retry_at=None, count_as_attempt=True)


# ---------- bulk_mark_failed ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_failed__increments_attempts_for_all_events(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    event_id1, event_id2 = uuid4(), uuid4()
    for eid, pk in [(event_id1, "k1"), (event_id2, "k2")]:
        await repo.create(
            OutboxEvent(
                aggregate_type="t",
                aggregate_id=uuid4(),
                event_type="e",
                topic="tp",
                payload={"p": 1},
                id=eid,
                partition_key=pk,
            )
        )
        await repo.mark_processing(eid, worker_id="w1")

    # Act
    count = await repo.bulk_mark_failed(
        [EventFailureUpdate(event_id1, "err 1", None), EventFailureUpdate(event_id2, "err 2", None)],
        count_as_attempt=True,
        worker_id="w1",
    )

    # Assert
    assert count == 2
    for eid in [event_id1, event_id2]:
        f = await repo.get_by_id(eid)
        assert f is not None
        assert f.status == EventStatus.PENDING
        assert f.attempts_made == 1


@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_failed_partial_update__raises_concurrent_update_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    event_id1, event_id2 = uuid4(), uuid4()
    mock_result.scalars.return_value.all.return_value = [event_id1]
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError) as exc_info:
        await repo.bulk_mark_failed(
            [EventFailureUpdate(event_id1, "err", None), EventFailureUpdate(event_id2, "err", None)],
            worker_id="worker-1",
        )

    assert exc_info.value.expected == 2
    assert exc_info.value.actual == 1
    assert event_id2 in exc_info.value.missing_ids


@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_failed_duplicate_ids__raises_value_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()

    # Act / Assert
    with pytest.raises(ValueError, match="must be unique"):
        await repo.bulk_mark_failed(
            [EventFailureUpdate(event_id, "err", None), EventFailureUpdate(event_id, "err2", None)],
            worker_id="worker-1",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__bulk_mark_failed_with_next_retry_at__persists_scheduled_at(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    event = OutboxEvent(
        aggregate_type="t",
        aggregate_id=uuid4(),
        event_type="e",
        topic="tp",
        payload={"p": 1},
        id=event_id,
        partition_key="k1",
    )
    await repo.create(event)
    await repo.mark_processing(event_id, worker_id="w1")
    next_retry = utc_now()

    # Act
    await repo.bulk_mark_failed(
        [EventFailureUpdate(event_id, "error", next_retry)],
        count_as_attempt=True,
        worker_id="w1",
    )

    # Assert
    fetched = await repo.get_by_id(event_id)
    assert fetched is not None
    assert fetched.scheduled_at.timestamp() == pytest.approx(next_retry.timestamp(), abs=0.001)


# ---------- cleanup / retention ----------


@pytest.mark.asyncio
async def test__outbox_repository__release_stale_locks__returns_count_and_calls_execute_once() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [uuid4()] * 5
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    count = await repo.release_stale_locks(stale_timeout_seconds=60)

    # Assert
    assert count == 5
    assert mock_session.execute.call_count == 1


@pytest.mark.asyncio
async def test__outbox_repository__delete_old_completed__returns_count_from_first_batch() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_del_result = MagicMock()
    mock_del_result.scalars.return_value.all.side_effect = [[uuid4()] * 5, []]
    mock_session.execute.return_value = mock_del_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    count = await repo.delete_old_completed(retention_days=7)

    # Assert
    assert count == 5


# ---------- bulk_release_locks ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__bulk_release_locks__clears_lock_fields(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    id1, id2 = uuid4(), uuid4()
    for eid, pk in [(id1, "k1"), (id2, "k2")]:
        await repo.create(
            OutboxEvent(
                aggregate_type="t",
                aggregate_id=uuid4(),
                event_type="e",
                topic="tp",
                payload={"p": 1},
                id=eid,
                partition_key=pk,
            )
        )
        await repo.mark_processing(eid, worker_id="w1")

    # Act
    count = await repo.bulk_release_locks([id1, id2], worker_id="w1")

    # Assert
    assert count == 2
    f1 = await repo.get_by_id(id1)
    assert f1 is not None
    assert f1.locked_at is None
    assert f1.locked_by is None


@pytest.mark.asyncio
async def test__outbox_repository__bulk_release_locks_partial_update__raises_concurrent_update_error() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [uuid4()]
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event_ids = [uuid4(), uuid4()]

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.bulk_release_locks(event_ids, worker_id="worker-1")


# ---------- release_lock ----------


@pytest.mark.asyncio
async def test__outbox_repository__release_lock_found_and_not_found__returns_bool() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    event_id = uuid4()
    mock_result.scalar_one_or_none.return_value = event_id
    mock_session.execute.return_value = mock_result
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)

    # Act
    success = await repo.release_lock(event_id, "w1")

    # Assert
    assert success is True

    mock_result.scalar_one_or_none.return_value = None
    success = await repo.release_lock(event_id, "w1")
    assert success is False


# ---------- requeue_failed ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__outbox_repository__requeue_failed__resets_status_and_attempts(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(session=async_session, model_class=ConcreteOutboxEvent)
    event_id = uuid4()
    event = OutboxEvent(
        aggregate_type="t",
        aggregate_id=uuid4(),
        event_type="e",
        topic="tp",
        payload={"p": 1},
        id=event_id,
        status=EventStatus.PENDING,
        attempts_made=0,
        max_attempts=6,
        partition_key="k1",
    )
    await repo.create(event)
    for _ in range(6):
        await repo.mark_processing(event_id, worker_id="w1")
        await repo.mark_failed(event_id, error="err", worker_id="w1")
    fetched = await repo.get_by_id(event_id)
    assert fetched is not None
    assert fetched.status == EventStatus.FAILED

    # Act
    success = await repo.requeue_failed(event_id)

    # Assert
    assert success is True
    requeued = await repo.get_by_id(event_id)
    assert requeued is not None
    assert requeued.status == EventStatus.PENDING
    assert requeued.attempts_made == 0


# ---------- fetch_pending ----------


@pytest.mark.asyncio
async def test__outbox_repository__fetch_pending__returns_empty_list_when_no_events() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    # Act
    events = await repo.fetch_pending(limit=10, topic="tp")

    # Assert
    assert len(events) == 0
    assert mock_session.execute.call_count == 1


# ---------- _prepare_insert_values ----------


@pytest.mark.asyncio
async def test__outbox_repository__prepare_insert_values__includes_aggregate_type_and_topic() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresOutboxRepository(session=mock_session, model_class=ConcreteOutboxEvent)
    event = OutboxEvent(
        aggregate_type="t", aggregate_id=uuid4(), event_type="e", topic="tp", partition_key="k", payload={"p": 1}
    )

    # Act
    vals = repo._prepare_insert_values(event)

    # Assert
    assert vals["aggregate_type"] == "t"
    assert vals["topic"] == "tp"


# ---------- inbox repository ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test__inbox_repository__non_partitioned_create__persists_and_returns_event(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresInboxRepository(session=async_session, model_class=ConcreteInboxEvent)
    new_id = uuid4()
    event = _make_inbox_domain_event(id=new_id)

    # Act
    out = await repo.create(event)

    # Assert
    assert out.id == new_id
    assert out.message_id == "msg-1"
    fetched = await repo.get_by_id(new_id)
    assert fetched is not None
    assert fetched.message_id == "msg-1"


@pytest.mark.integration
@pytest.mark.asyncio
async def test__inbox_repository__partitioned_create_duplicate__returns_existing_event(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresInboxRepository(session=async_session, model_class=ConcreteInboxEventPartitioned)
    new_id = uuid4()
    event = _make_inbox_domain_event(id=new_id)

    # Act
    out = await repo.create(event)

    # Assert
    assert out.id == new_id
    conflict_event = _make_inbox_domain_event(id=uuid4(), message_id="msg-1", consumer_group="cg-1")
    out_conflict = await repo.create(conflict_event)
    assert out_conflict.id == new_id


@pytest.mark.asyncio
async def test__inbox_repository__partitioned_lock_then_select__returns_existing_under_lock() -> None:
    # Arrange
    mock_session = AsyncMock()
    r_lock = MagicMock()
    r_select = MagicMock()
    mock_session.execute.side_effect = [r_lock, r_select]
    repo = PostgresInboxRepository(session=mock_session, model_class=ConcreteInboxEventPartitioned)
    now = utc_now()
    existing_id = uuid4()
    new_id = uuid4()
    event = _make_inbox_domain_event(id=new_id)
    existing_db = _mock_inbox_db_row(existing_id, "msg-1", "cg-1", now)
    r_select.scalar_one_or_none.return_value = existing_db

    # Act
    out = await repo.create(event)

    # Assert
    assert mock_session.execute.call_count == 2
    assert out.id == existing_id


@pytest.mark.asyncio
async def test__inbox_repository__has_completed_sibling_non_partitioned__returns_false_without_query() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresInboxRepository(session=mock_session, model_class=ConcreteInboxEvent)

    # Act
    result = await repo.has_completed_sibling_for_inbox_key("a", "b", uuid4())

    # Assert
    assert result is False
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test__inbox_repository__has_completed_sibling_partitioned__queries_and_returns_scalar() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar.return_value = True
    mock_session.execute.return_value = mock_result
    repo = PostgresInboxRepository(session=mock_session, model_class=ConcreteInboxEventPartitioned)
    exclude_id = uuid4()

    # Act
    result = await repo.has_completed_sibling_for_inbox_key("x", "y", exclude_id)

    # Assert
    assert result is True
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test__inbox_repository__exists__returns_true_when_found() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar.return_value = True
    mock_session.execute.return_value = mock_result
    repo = PostgresInboxRepository(session=mock_session, model_class=ConcreteInboxEvent)

    # Act
    result = await repo.exists("m1", "cg1")

    # Assert
    assert result is True
    assert mock_session.execute.call_count == 1


@pytest.mark.asyncio
async def test__inbox_repository__get_by_message_id__returns_entity_when_found() -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_result = MagicMock()
    now = utc_now()
    event_id = uuid4()
    mock_db = _mock_inbox_db_row(event_id, "m1", "cg1", now)
    mock_result.scalar_one_or_none.return_value = mock_db
    mock_session.execute.return_value = mock_result
    repo = PostgresInboxRepository(session=mock_session, model_class=ConcreteInboxEvent)

    # Act
    res = await repo.get_by_message_id("m1", "cg1")

    # Assert
    assert res is not None
    assert res.id == event_id


@pytest.mark.asyncio
async def test__inbox_repository__partitioned_create_conflict_after_insert__returns_existing_from_select() -> None:
    # Arrange
    mock_session = AsyncMock()
    r_lock = MagicMock()
    r_select_before = MagicMock()
    r_insert = MagicMock()
    r_select_after = MagicMock()
    mock_session.execute.side_effect = [r_lock, r_select_before, r_insert, r_select_after]
    repo = PostgresInboxRepository(session=mock_session, model_class=ConcreteInboxEventPartitioned)
    now = utc_now()
    existing_id = uuid4()
    event = _make_inbox_domain_event(message_id="m1", consumer_group="cg1")
    r_select_before.scalar_one_or_none.return_value = None
    r_insert.scalar_one_or_none.return_value = None
    r_select_after.scalar_one_or_none.return_value = _mock_inbox_db_row(existing_id, "m1", "cg1", now)

    # Act
    out = await repo.create(event)

    # Assert
    assert out.id == existing_id
    assert mock_session.execute.call_count == 4


@pytest.mark.asyncio
async def test__inbox_repository__prepare_insert_values__includes_source_and_message_id() -> None:
    # Arrange
    mock_session = AsyncMock()
    repo = PostgresInboxRepository(session=mock_session, model_class=ConcreteInboxEvent)
    event = _make_inbox_domain_event(source="src1")

    # Act
    vals = repo._prepare_insert_values(event)

    # Assert
    assert vals["source"] == "src1"
    assert vals["message_id"] == "msg-1"


# ---------- _as_list helper ----------


@pytest.mark.asyncio
async def test__as_list_helper__various_inputs__returns_expected_list_or_none() -> None:
    # Arrange / Act / Assert
    assert _as_list(None) is None
    assert _as_list([]) is None
    assert _as_list(["a"]) == ["a"]
    assert _as_list(("b",)) == ["b"]
