"""Unit tests for PostgresOutboxRepository happy paths.

Covers all branches that return data (not error paths), exercising the full
chain from session mock through ``_to_entity`` back to the caller.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.exceptions import EventConcurrentUpdateError
from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.storage.postgres.repositories.base import _as_list
from omni_box.infra.storage.postgres.repositories.outbox import PostgresOutboxRepository
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _make_event(**kwargs: object) -> OutboxEvent:
    defaults: dict[str, object] = dict(
        id=uuid4(),
        aggregate_type="user",
        aggregate_id=uuid4(),
        event_type="user.created",
        topic="t",
        partition_key="p",
        payload={"k": "v"},
    )
    defaults.update(kwargs)
    return OutboxEvent(**defaults)  # type: ignore[arg-type]


def _make_db_event(event_id: UUID | None = None) -> MagicMock:
    """Return a mock ORM object with all required attributes."""
    db = MagicMock()
    db.id = event_id or uuid4()
    db.event_type = "user.created"
    db.payload = {"k": "v"}
    db.headers = None
    db.status = "pending"
    db.attempts_made = 0
    db.max_attempts = 3
    db.last_error = None
    db.trace_id = None
    db.idempotency_key = None
    db.correlation_id = None
    db.causation_id = None
    db.schema_version = None
    db.created_at = _NOW
    db.scheduled_at = _NOW
    db.completed_at = None
    db.locked_at = None
    db.locked_by = None
    # Outbox-specific
    db.aggregate_type = "user"
    db.aggregate_id = uuid4()
    db.topic = "t"
    db.partition_key = "p"
    return db


def _make_session_returning(scalar_value: object = None, scalars_list: list | None = None) -> AsyncSession:
    """Session whose execute always returns a successful result."""
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = scalar_value
    mock_result.scalar.return_value = scalar_value
    mock_result.scalars.return_value.all.return_value = scalars_list or []
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _make_repo(session: AsyncSession) -> PostgresOutboxRepository:
    return PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEvent)


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


async def test__create__session_returns_db_event__returns_entity() -> None:
    # Arrange
    event = _make_event()
    db_event = _make_db_event(event.id)
    session = _make_session_returning(scalar_value=db_event)
    repo = _make_repo(session)

    # Act
    result = await repo.create(event)

    # Assert
    assert isinstance(result, OutboxEvent)
    assert result.event_type == "user.created"


async def test__create__first_execute_none_second_returns_existing__returns_entity() -> None:
    # Arrange
    event = _make_event()
    db_event = _make_db_event(event.id)

    result1 = MagicMock()
    result1.scalar_one_or_none.return_value = None  # INSERT returns nothing (conflict)

    result2 = MagicMock()
    result2.scalar_one_or_none.return_value = db_event  # SELECT fetches existing

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[result1, result2])
    repo = _make_repo(session)

    # Act
    result = await repo.create(event)

    # Assert
    assert isinstance(result, OutboxEvent)
    assert result.id == db_event.id


async def test__create__both_executes_return_none__raises_concurrent_update_error() -> None:
    # Arrange
    event = _make_event()

    result_none = MagicMock()
    result_none.scalar_one_or_none.return_value = None

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[result_none, result_none])
    repo = _make_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.create(event)


# ---------------------------------------------------------------------------
# get_by_id
# ---------------------------------------------------------------------------


async def test__get_by_id__db_event_found__returns_entity() -> None:
    # Arrange
    db_event = _make_db_event()
    session = _make_session_returning(scalar_value=db_event)
    repo = _make_repo(session)

    # Act
    result = await repo.get_by_id(db_event.id)

    # Assert
    assert isinstance(result, OutboxEvent)


async def test__get_by_id__db_event_not_found__returns_none() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act
    result = await repo.get_by_id(uuid4())

    # Assert
    assert result is None


# ---------------------------------------------------------------------------
# fetch_pending
# ---------------------------------------------------------------------------


async def test__fetch_pending__returns_list_of_db_events__returns_list_of_entities() -> None:
    # Arrange
    db_events = [_make_db_event(), _make_db_event()]
    session = _make_session_returning(scalars_list=db_events)
    repo = _make_repo(session)

    # Act
    result = await repo.fetch_pending(limit=10)

    # Assert
    assert len(result) == 2
    assert all(isinstance(e, OutboxEvent) for e in result)


# ---------------------------------------------------------------------------
# mark_processing
# ---------------------------------------------------------------------------


async def test__mark_processing__session_returns_id__returns_true() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act
    result = await repo.mark_processing(event_id, "worker-1")

    # Assert
    assert result is True


async def test__mark_processing__session_returns_none__returns_false() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act
    result = await repo.mark_processing(uuid4(), "worker-1")

    # Assert
    assert result is False


# ---------------------------------------------------------------------------
# mark_completed
# ---------------------------------------------------------------------------


async def test__mark_completed__session_returns_id__succeeds_without_exception() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act – must not raise
    await repo.mark_completed(event_id, "worker-1")


async def test__mark_completed__session_returns_none__raises_concurrent_update_error() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.mark_completed(uuid4(), "worker-1")


# ---------------------------------------------------------------------------
# mark_failed
# ---------------------------------------------------------------------------


async def test__mark_failed__count_as_attempt_true__succeeds() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act – must not raise
    await repo.mark_failed(event_id, "some error", "worker-1", count_as_attempt=True)


async def test__mark_failed__count_as_attempt_false__succeeds() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act – must not raise
    await repo.mark_failed(event_id, "some error", "worker-1", count_as_attempt=False)


# ---------------------------------------------------------------------------
# bulk_mark_completed
# ---------------------------------------------------------------------------


async def test__bulk_mark_completed__all_ids_updated__returns_count() -> None:
    # Arrange
    ids = [uuid4(), uuid4()]

    result_ok = MagicMock()
    result_ok.scalars.return_value.all.return_value = ids  # all updated

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result_ok)
    repo = _make_repo(session)

    # Act
    count = await repo.bulk_mark_completed(ids, "worker-1")

    # Assert
    assert count == 2


async def test__bulk_mark_completed__partial_update__raises_concurrent_update_error() -> None:
    # Arrange
    id1, id2 = uuid4(), uuid4()
    ids = [id1, id2]

    # First execute: UPDATE returns only 1 id (partial success)
    result_update = MagicMock()
    result_update.scalars.return_value.all.return_value = [id1]

    # Second execute (_get_existing_ids): SELECT returns both ids (both exist)
    result_existing = MagicMock()
    result_existing.scalars.return_value.all.return_value = [id1, id2]

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[result_update, result_existing])
    repo = _make_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.bulk_mark_completed(ids, "worker-1")


# ---------------------------------------------------------------------------
# force_unlock
# ---------------------------------------------------------------------------


async def test__force_unlock__session_returns_id__returns_true() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act
    result = await repo.force_unlock(event_id, "stale lock reason")

    # Assert
    assert result is True


async def test__force_unlock__session_returns_none__raises_concurrent_update_error() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.force_unlock(uuid4(), "stale lock reason")


# ---------------------------------------------------------------------------
# fetch_and_lock_pending
# ---------------------------------------------------------------------------


async def test__fetch_and_lock_pending__with_ttl__returns_entities() -> None:
    # Arrange
    db_events = [_make_db_event()]
    session = _make_session_returning(scalars_list=db_events)
    repo = _make_repo(session)

    # Act
    result = await repo.fetch_and_lock_pending(limit=5, worker_id="worker-1", ttl=60)

    # Assert
    assert len(result) == 1
    assert isinstance(result[0], OutboxEvent)


async def test__fetch_and_lock_pending__without_ttl__returns_entities() -> None:
    # Arrange
    db_events = [_make_db_event(), _make_db_event()]
    session = _make_session_returning(scalars_list=db_events)
    repo = _make_repo(session)

    # Act
    result = await repo.fetch_and_lock_pending(limit=5, worker_id="worker-1", ttl=None)

    # Assert
    assert len(result) == 2


# ---------------------------------------------------------------------------
# refresh_lock
# ---------------------------------------------------------------------------


async def test__refresh_lock__session_returns_id__returns_true() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act
    result = await repo.refresh_lock(event_id, "worker-1")

    # Assert
    assert result is True


async def test__refresh_lock__session_returns_none__returns_false() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act
    result = await repo.refresh_lock(uuid4(), "worker-1")

    # Assert
    assert result is False


# ---------------------------------------------------------------------------
# release_lock
# ---------------------------------------------------------------------------


async def test__release_lock__session_returns_id__returns_true() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act
    result = await repo.release_lock(event_id, "worker-1")

    # Assert
    assert result is True


async def test__release_lock__session_returns_none__returns_false() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act
    result = await repo.release_lock(uuid4(), "worker-1")

    # Assert
    assert result is False


# ---------------------------------------------------------------------------
# bulk_release_locks
# ---------------------------------------------------------------------------


async def test__bulk_release_locks__all_updated__returns_count() -> None:
    # Arrange
    ids = [uuid4(), uuid4()]

    result_ok = MagicMock()
    result_ok.scalars.return_value.all.return_value = ids

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result_ok)
    repo = _make_repo(session)

    # Act
    count = await repo.bulk_release_locks(ids, "worker-1")

    # Assert
    assert count == 2


async def test__bulk_release_locks__partial_update__raises_concurrent_update_error() -> None:
    # Arrange
    ids = [uuid4(), uuid4()]

    result_partial = MagicMock()
    result_partial.scalars.return_value.all.return_value = ids[:1]  # only 1 released

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result_partial)
    repo = _make_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.bulk_release_locks(ids, "worker-1")


# ---------------------------------------------------------------------------
# delete_old_completed
# ---------------------------------------------------------------------------


async def test__delete_old_completed__returns_count_of_deleted_rows() -> None:
    # Arrange
    deleted_ids = [uuid4(), uuid4(), uuid4()]

    result_ok = MagicMock()
    result_ok.scalars.return_value.all.return_value = deleted_ids

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result_ok)
    repo = _make_repo(session)

    # Act
    count = await repo.delete_old_completed(retention_days=30)

    # Assert
    assert count == 3


# ---------------------------------------------------------------------------
# release_stale_locks
# ---------------------------------------------------------------------------


async def test__release_stale_locks__loop_exits_when_count_below_batch_size__returns_total() -> None:
    # Arrange
    # First call: batch_size results (forces another iteration)
    # Second call: 0 results (loop exits)
    result_full = MagicMock()
    result_full.scalars.return_value.all.return_value = [uuid4()] * 5

    result_empty = MagicMock()
    result_empty.scalars.return_value.all.return_value = []

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[result_full, result_empty])

    # batch_size=5 so first iteration is "full" and second is empty → exit
    repo = PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEvent, batch_size=5)

    # Act
    total = await repo.release_stale_locks(stale_timeout_seconds=300)

    # Assert
    assert total == 5


# ---------------------------------------------------------------------------
# requeue_failed
# ---------------------------------------------------------------------------


async def test__requeue_failed__session_returns_id__returns_true() -> None:
    # Arrange
    event_id = uuid4()
    session = _make_session_returning(scalar_value=event_id)
    repo = _make_repo(session)

    # Act
    result = await repo.requeue_failed(event_id)

    # Assert
    assert result is True


async def test__requeue_failed__session_returns_none__returns_false() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act
    result = await repo.requeue_failed(uuid4())

    # Assert
    assert result is False


# ---------------------------------------------------------------------------
# _as_list (module-level utility)
# ---------------------------------------------------------------------------


def test__as_list__none__returns_none() -> None:
    # Act / Assert
    assert _as_list(None) is None


def test__as_list__empty_tuple__returns_none() -> None:
    # Act / Assert
    assert _as_list(()) is None


def test__as_list__non_empty_tuple__returns_list() -> None:
    # Act
    result = _as_list(("a", "b"))

    # Assert
    assert result == ["a", "b"]


# ---------------------------------------------------------------------------
# capabilities property
# ---------------------------------------------------------------------------


def test__capabilities__always__returns_all_supported() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))

    # Act
    caps = repo.capabilities

    # Assert
    assert caps.supports_bulk is True
    assert caps.supports_distributed_locking is True
    assert caps.supports_retention is True


# ---------------------------------------------------------------------------
# bulk_create — without-idempotency (no-idem) branch
# ---------------------------------------------------------------------------


async def test__bulk_create__events_without_idempotency_key__returns_created_entities() -> None:
    # Arrange
    event = _make_event()  # idempotency_key is None by default
    db_event = _make_db_event(event.id)

    result_ok = MagicMock()
    result_ok.scalars.return_value.all.return_value = [db_event]

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result_ok)
    repo = _make_repo(session)

    # Act
    created = await repo.bulk_create([event])

    # Assert
    assert len(created) == 1
    assert isinstance(created[0], OutboxEvent)


async def test__bulk_create__partial_skipped_without_idem__fetches_existing_and_appends() -> None:
    # Arrange: batch of 2 events; insert returns only 1 (1 was skipped by conflict)
    e1, e2 = _make_event(), _make_event()
    db1 = _make_db_event(e1.id)
    db2 = _make_db_event(e2.id)

    # First execute: bulk insert returns only db1
    insert_result = MagicMock()
    insert_result.scalars.return_value.all.return_value = [db1]

    # Second execute: fetch_existing_event for skipped e2 returns db2
    fetch_result = MagicMock()
    fetch_result.scalar_one_or_none.return_value = db2

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[insert_result, fetch_result])
    repo = _make_repo(session)

    # Act
    created = await repo.bulk_create([e1, e2])

    # Assert
    assert len(created) == 2


# ---------------------------------------------------------------------------
# mark_failed — result None raises EventConcurrentUpdateError
# ---------------------------------------------------------------------------


async def test__mark_failed__session_returns_none__raises_concurrent_update_error() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.mark_failed(uuid4(), "oops", "worker-1")


# ---------------------------------------------------------------------------
# bulk_mark_failed — partial update raises; success returns count
# ---------------------------------------------------------------------------


async def test__bulk_mark_failed__all_updated__returns_count() -> None:
    # Arrange
    from omni_box.core.models.types import EventFailureUpdate

    ids = [uuid4(), uuid4()]
    failures = [EventFailureUpdate(event_id=i, error="e", next_retry_at=None) for i in ids]

    result_ok = MagicMock()
    result_ok.scalars.return_value.all.return_value = ids

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result_ok)
    repo = _make_repo(session)

    # Act
    count = await repo.bulk_mark_failed(failures, "worker-1")

    # Assert
    assert count == 2


async def test__bulk_mark_failed__partial_update__raises_concurrent_update_error() -> None:
    # Arrange
    from omni_box.core.models.types import EventFailureUpdate

    ids = [uuid4(), uuid4()]
    failures = [EventFailureUpdate(event_id=i, error="e", next_retry_at=None) for i in ids]

    # bulk update returns only 1 of 2 updated
    update_result = MagicMock()
    update_result.scalars.return_value.all.return_value = [ids[0]]

    # _get_existing_ids call returns both IDs (both exist, just not updated)
    existing_result = MagicMock()
    existing_result.scalars.return_value.all.return_value = ids

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[update_result, existing_result])
    repo = _make_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.bulk_mark_failed(failures, "worker-1")
