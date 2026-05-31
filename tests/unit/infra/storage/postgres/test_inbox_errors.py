"""Unit tests for SQLAlchemy error wrapping and edge branches in ``PostgresInboxRepository``."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.exceptions import StorageError
from omni_box.core.models.entities import InboxEvent
from omni_box.infra.storage.postgres.repositories.inbox import PostgresInboxRepository
from tests.models import ConcreteInboxEvent, ConcreteInboxEventPartitioned

pytestmark = pytest.mark.unit


def _make_event() -> InboxEvent:
    return InboxEvent(
        id=uuid4(),
        event_type="user.created",
        payload={"k": "v"},
        message_id="m-1",
        consumer_group="cg-1",
        source="kafka",
    )


def _session_raising(exc: BaseException) -> AsyncSession:
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=exc)
    return session


def _make_repo(
    session: AsyncSession,
    *,
    partitioned: bool = False,
) -> PostgresInboxRepository:
    cls = ConcreteInboxEventPartitioned if partitioned else ConcreteInboxEvent
    return PostgresInboxRepository(session=session, model_class=cls)


# ---------- get_by_message_id ----------


async def test__inbox_get_by_message_id__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(SQLAlchemyError("boom")))

    # Act / Assert
    with pytest.raises(StorageError, match="error fetching inbox event"):
        await repo.get_by_message_id("m-1", "cg-1")


# ---------- exists ----------


async def test__inbox_exists__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(SQLAlchemyError("boom")))

    # Act / Assert
    with pytest.raises(StorageError, match="error checking inbox event"):
        await repo.exists("m-1", "cg-1")


# ---------- has_completed_sibling_for_inbox_key ----------


async def test__inbox_has_completed_sibling__not_partitioned_model__returns_false_without_query() -> None:
    # Arrange
    session = AsyncMock(spec=AsyncSession)
    repo = _make_repo(session, partitioned=False)

    # Act
    result = await repo.has_completed_sibling_for_inbox_key("m-1", "cg-1", uuid4())

    # Assert
    assert result is False
    session.execute.assert_not_called()


async def test__inbox_has_completed_sibling__sqlalchemy_error_on_partitioned__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(SQLAlchemyError("boom")), partitioned=True)

    # Act / Assert
    with pytest.raises(StorageError, match="error checking sibling for inbox"):
        await repo.has_completed_sibling_for_inbox_key("m-1", "cg-1", uuid4())


# ---------- _fetch_existing_event when created_at NOT in dedup tuple ----------


async def test__inbox_fetch_existing_event__not_partitioned__skips_created_at_where_clause() -> None:
    # Arrange
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=AsyncMock(scalar_one_or_none=lambda: None))
    repo = _make_repo(session, partitioned=False)
    event = _make_event()

    # Act
    result = await repo._fetch_existing_event(event)

    # Assert
    assert result is None
    # WHERE clause of the rendered statement must not contain a created_at predicate.
    args, _ = session.execute.call_args
    rendered = str(args[0].compile(compile_kwargs={"literal_binds": True}))
    where_clause = rendered.split("WHERE", 1)[1]
    assert "created_at" not in where_clause


# ---------- create (partitioned path) — IntegrityError and SQLAlchemyError ----------


async def test__inbox_create_partitioned__integrity_error__wrapped_as_storage_integrity_error() -> None:
    # Arrange
    from sqlalchemy.exc import IntegrityError

    from omni_box.core.exceptions import StorageIntegrityError

    repo = _make_repo(_session_raising(IntegrityError("stmt", {}, Exception("dup"))), partitioned=True)

    # Act / Assert
    with pytest.raises(StorageIntegrityError, match="constraint violation"):
        await repo.create(_make_event())


async def test__inbox_create_partitioned__sqlalchemy_error__wrapped_as_storage_error() -> None:
    # Arrange
    repo = _make_repo(_session_raising(SQLAlchemyError("boom")), partitioned=True)

    # Act / Assert
    with pytest.raises(StorageError, match="database error"):
        await repo.create(_make_event())
