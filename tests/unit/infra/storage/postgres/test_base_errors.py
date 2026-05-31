"""Unit tests for SQLAlchemy error wrapping in ``PostgresEventRepository``.

Every public method that catches ``SQLAlchemyError`` and re-raises it as
``StorageError`` (or ``StorageIntegrityError``) is exercised here with a fake
session whose ``execute`` raises a controlled error. The goal is 100% coverage of
the ``except SQLAlchemyError`` branches without requiring a live database.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.constants import FORCE_UNLOCK_REASON_MAX_LENGTH
from omni_box.core.exceptions import StorageError, StorageIntegrityError
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.types import EventFailureUpdate
from omni_box.infra.storage.postgres.repositories.outbox import PostgresOutboxRepository
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.unit


# ---------- fixtures ----------


def _make_event() -> OutboxEvent:
    return OutboxEvent(
        id=uuid4(),
        aggregate_type="user",
        aggregate_id=uuid4(),
        event_type="user.created",
        topic="t",
        partition_key="p",
        payload={"k": "v"},
    )


def _session_raising(exc: BaseException) -> AsyncSession:
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=exc)
    return session


def _make_repo(session: AsyncSession) -> PostgresOutboxRepository:
    return PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEvent)


def _sqla_error() -> SQLAlchemyError:
    return SQLAlchemyError("boom")


def _integrity_error() -> IntegrityError:
    return IntegrityError("stmt", {}, Exception("constraint"))


# ---------- create ----------


async def test__create__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="database error"):
        await repo.create(_make_event())


async def test__create__integrity_error__wrapped_as_storage_integrity_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_integrity_error()))

    # Act / Assert
    with pytest.raises(StorageIntegrityError, match="constraint violation"):
        await repo.create(_make_event())


# ---------- bulk_create ----------


async def test__bulk_create__empty_input__returns_empty_list_without_session_call() -> None:
    # Arrange
    session = AsyncMock(spec=AsyncSession)
    repo = _make_repo(session)

    # Act
    result = await repo.bulk_create([])

    # Assert
    assert result == []
    session.execute.assert_not_called()


async def test__bulk_create__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="bulk creation database error"):
        await repo.bulk_create([_make_event()])


async def test__bulk_create__integrity_error__wrapped_as_storage_integrity_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_integrity_error()))

    # Act / Assert
    with pytest.raises(StorageIntegrityError, match="bulk creation constraint violation"):
        await repo.bulk_create([_make_event()])


# ---------- read paths ----------


async def test__get_by_id__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="error fetching event"):
        await repo.get_by_id(uuid4())


async def test__fetch_pending__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="error fetching pending"):
        await repo.fetch_pending(limit=10)


# ---------- state-transition paths ----------


async def test__mark_processing__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="marking processing"):
        await repo.mark_processing(uuid4(), "worker-1")


async def test__mark_completed__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="marking completed"):
        await repo.mark_completed(uuid4(), "worker-1")


async def test__mark_failed__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="marking failed"):
        await repo.mark_failed(uuid4(), "err", "worker-1")


# ---------- bulk-state-transition paths ----------


async def test__bulk_mark_completed__empty_list__returns_zero() -> None:
    # Arrange
    session = AsyncMock(spec=AsyncSession)
    repo = _make_repo(session)

    # Act
    result = await repo.bulk_mark_completed([], "worker")

    # Assert
    assert result == 0
    session.execute.assert_not_called()


async def test__bulk_mark_completed__duplicate_event_ids__raises_value_error() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))
    dup_id = uuid4()

    # Act / Assert
    with pytest.raises(ValueError, match="event_ids must be unique"):
        await repo.bulk_mark_completed([dup_id, dup_id], "worker-1")


async def test__bulk_mark_completed__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="bulk mark completed"):
        await repo.bulk_mark_completed([uuid4()], "worker-1")


async def test__bulk_mark_failed__empty_list__returns_zero() -> None:
    # Arrange
    session = AsyncMock(spec=AsyncSession)
    repo = _make_repo(session)

    # Act
    result = await repo.bulk_mark_failed([], "worker")

    # Assert
    assert result == 0
    session.execute.assert_not_called()


async def test__bulk_mark_failed__duplicate_event_ids__raises_value_error() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))
    dup_id = uuid4()
    f1 = EventFailureUpdate(event_id=dup_id, error="e", next_retry_at=None)
    f2 = EventFailureUpdate(event_id=dup_id, error="e", next_retry_at=None)

    # Act / Assert
    with pytest.raises(ValueError, match="event_ids in failures must be unique"):
        await repo.bulk_mark_failed([f1, f2], "worker")


async def test__bulk_mark_failed__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))
    failures = [EventFailureUpdate(event_id=uuid4(), error="e", next_retry_at=None)]

    # Act / Assert
    with pytest.raises(StorageError, match="bulk mark failed error"):
        await repo.bulk_mark_failed(failures, "worker-1")


async def test__bulk_mark_failed__count_as_attempt_false__uses_no_attempt_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: trigger SQLAlchemyError through the no-attempt branch (count_as_attempt=False)
    repo = _make_repo(_session_raising(_sqla_error()))
    failures = [EventFailureUpdate(event_id=uuid4(), error="e", next_retry_at=None)]

    # Act / Assert
    with pytest.raises(StorageError, match="bulk mark failed error"):
        await repo.bulk_mark_failed(failures, "worker-1", count_as_attempt=False)


# ---------- force_unlock ----------


@pytest.mark.parametrize("reason", ["", "   ", "\t\n"], ids=["empty", "spaces", "tabs"])
async def test__force_unlock__blank_reason__raises_value_error(reason: str) -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))

    # Act / Assert
    with pytest.raises(ValueError, match="Reason for force unlock cannot be empty"):
        await repo.force_unlock(uuid4(), reason)


async def test__force_unlock__reason_too_long__raises_value_error() -> None:
    # Arrange
    long_reason = "x" * (FORCE_UNLOCK_REASON_MAX_LENGTH + 1)
    repo = _make_repo(AsyncMock(spec=AsyncSession))

    # Act / Assert
    with pytest.raises(ValueError, match="too long"):
        await repo.force_unlock(uuid4(), long_reason)


async def test__force_unlock__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="force unlock error"):
        await repo.force_unlock(uuid4(), "valid reason")


# ---------- fetch_and_lock_pending ----------


async def test__fetch_and_lock_pending__limit_less_than_one__raises_value_error() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))

    # Act / Assert
    with pytest.raises(ValueError, match="limit must be greater than 0"):
        await repo.fetch_and_lock_pending(limit=0, worker_id="w-1")


async def test__fetch_and_lock_pending__empty_worker_id_after_strip__raises_value_error() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))

    # Act / Assert
    with pytest.raises(ValueError, match="worker_id cannot be empty"):
        await repo.fetch_and_lock_pending(limit=1, worker_id="   ")


async def test__fetch_and_lock_pending__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="fetch-and-lock error"):
        await repo.fetch_and_lock_pending(limit=1, worker_id="worker-1")


# ---------- refresh / release / bulk_release ----------


async def test__refresh_lock__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="lock refresh error"):
        await repo.refresh_lock(uuid4(), "worker-1")


async def test__release_lock__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="lock release error"):
        await repo.release_lock(uuid4(), "worker-1")


async def test__bulk_release_locks__empty_list__returns_zero() -> None:
    # Arrange
    session = AsyncMock(spec=AsyncSession)
    repo = _make_repo(session)

    # Act
    result = await repo.bulk_release_locks([], "worker")

    # Assert
    assert result == 0
    session.execute.assert_not_called()


async def test__bulk_release_locks__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="bulk lock release error"):
        await repo.bulk_release_locks([uuid4()], "worker-1")


# ---------- retention / housekeeping ----------


async def test__delete_old_completed__retention_below_one__raises_value_error() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))

    # Act / Assert
    with pytest.raises(ValueError, match="retention_days must be greater than 0"):
        await repo.delete_old_completed(retention_days=0)


async def test__delete_old_completed__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="cleanup error"):
        await repo.delete_old_completed(retention_days=7)


async def test__release_stale_locks__timeout_below_one__raises_value_error() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))

    # Act / Assert
    with pytest.raises(ValueError, match="stale_timeout_seconds must be greater than 0"):
        await repo.release_stale_locks(stale_timeout_seconds=0)


async def test__release_stale_locks__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="stale lock release error"):
        await repo.release_stale_locks(stale_timeout_seconds=60)


# ---------- requeue ----------


async def test__requeue_failed__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(_sqla_error()))

    # Act / Assert
    with pytest.raises(StorageError, match="requeue failed error"):
        await repo.requeue_failed(uuid4())


# ---------- _apply_filters with None value (skip branch) ----------


def test__apply_filters__filter_value_is_none__filter_is_skipped() -> None:
    # Arrange
    repo = _make_repo(AsyncMock(spec=AsyncSession))
    base_stmt = sa.select(ConcreteOutboxEvent)
    filters: dict[str, Any] = {"event_type": None, "aggregate_type": "user"}

    # Act
    out = repo._apply_filters(base_stmt, filters)  # type: ignore[arg-type]

    # Assert
    rendered = str(out.compile(compile_kwargs={"literal_binds": True}))
    assert "event_type" not in rendered or "IS NULL" not in rendered
    assert "aggregate_type" in rendered
