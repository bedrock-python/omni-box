"""Unit tests for PostgresInboxRepository happy paths.

Covers all method branches for the inbox repository, including both non-partitioned
(no created_at dedup) and partitioned (created_at in dedup) model paths.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.exceptions import EventConcurrentUpdateError
from omni_box.core.models.entities import InboxEvent
from omni_box.infra.storage.postgres.repositories.inbox import PostgresInboxRepository
from tests.models import ConcreteInboxEvent, ConcreteInboxEventPartitioned

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _make_inbox_event(**kwargs: object) -> InboxEvent:
    defaults: dict[str, object] = dict(
        id=uuid4(),
        event_type="msg.received",
        payload={"a": 1},
        message_id="m1",
        consumer_group="cg1",
        source="svc",
        created_at=_NOW,
        scheduled_at=_NOW,
    )
    defaults.update(kwargs)
    return InboxEvent(**defaults)  # type: ignore[arg-type]


def _make_db_inbox_event(message_id: str = "m1", consumer_group: str = "cg1") -> MagicMock:
    """Return a mock ORM inbox object with all required attributes."""
    db = MagicMock()
    db.id = uuid4()
    db.event_type = "msg.received"
    db.payload = {"a": 1}
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
    # Inbox-specific
    db.message_id = message_id
    db.consumer_group = consumer_group
    db.source = "svc"
    return db


def _make_session_returning(scalar_value: object = None, scalars_list: list | None = None) -> AsyncSession:
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = scalar_value
    mock_result.scalar.return_value = scalar_value
    mock_result.scalars.return_value.all.return_value = scalars_list or []
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _make_non_partitioned_repo(session: AsyncSession) -> PostgresInboxRepository:
    """Repo using ConcreteInboxEvent (no created_at in dedup cols)."""
    return PostgresInboxRepository(session=session, model_class=ConcreteInboxEvent)


def _make_partitioned_repo(session: AsyncSession) -> PostgresInboxRepository:
    """Repo using ConcreteInboxEventPartitioned (created_at in dedup cols)."""
    return PostgresInboxRepository(session=session, model_class=ConcreteInboxEventPartitioned)


# ---------------------------------------------------------------------------
# create — non-partitioned path (delegates to super().create)
# ---------------------------------------------------------------------------


async def test__create__non_partitioned_model__delegates_to_base_create_and_returns_entity() -> None:
    # Arrange
    event = _make_inbox_event()
    db_event = _make_db_inbox_event()

    result_ok = MagicMock()
    result_ok.scalar_one_or_none.return_value = db_event

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=result_ok)

    repo = _make_non_partitioned_repo(session)

    # Act
    entity = await repo.create(event)

    # Assert
    assert isinstance(entity, InboxEvent)
    assert entity.event_type == "msg.received"


# ---------------------------------------------------------------------------
# create — partitioned path (created_at in dedup cols)
# ---------------------------------------------------------------------------


async def test__create__partitioned_existing_found__returns_existing_entity() -> None:
    # Arrange
    event = _make_inbox_event()
    existing_db = _make_db_inbox_event()

    # Advisory lock execute returns nothing; SELECT returns existing
    lock_result = MagicMock()
    lock_result.scalar_one_or_none.return_value = None

    existing_result = MagicMock()
    existing_result.scalar_one_or_none.return_value = existing_db

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[lock_result, existing_result])
    repo = _make_partitioned_repo(session)

    # Act
    entity = await repo.create(event)

    # Assert
    assert isinstance(entity, InboxEvent)
    assert entity.message_id == "m1"


async def test__create__partitioned_no_existing_insert_succeeds__returns_new_entity() -> None:
    # Arrange
    event = _make_inbox_event()
    new_db = _make_db_inbox_event()

    lock_result = MagicMock()
    lock_result.scalar_one_or_none.return_value = None  # advisory lock

    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = None  # no existing

    insert_result = MagicMock()
    insert_result.scalar_one_or_none.return_value = new_db  # INSERT returned row

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[lock_result, select_result, insert_result])
    repo = _make_partitioned_repo(session)

    # Act
    entity = await repo.create(event)

    # Assert
    assert isinstance(entity, InboxEvent)


async def test__create__partitioned_insert_returns_none_fetch_returns_existing__returns_entity() -> None:
    # Arrange
    event = _make_inbox_event()
    existing_db = _make_db_inbox_event()

    lock_result = MagicMock()
    lock_result.scalar_one_or_none.return_value = None

    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = None  # no existing before insert

    insert_none = MagicMock()
    insert_none.scalar_one_or_none.return_value = None  # INSERT conflict → nothing

    fetch_existing = MagicMock()
    fetch_existing.scalar_one_or_none.return_value = existing_db  # _fetch_existing_event

    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=[lock_result, select_result, insert_none, fetch_existing])
    repo = _make_partitioned_repo(session)

    # Act
    entity = await repo.create(event)

    # Assert
    assert isinstance(entity, InboxEvent)


async def test__create__partitioned_insert_returns_none_fetch_returns_none__raises_error() -> None:
    # Arrange
    event = _make_inbox_event()

    result_none = MagicMock()
    result_none.scalar_one_or_none.return_value = None

    session = AsyncMock(spec=AsyncSession)
    # lock + initial select + insert + fetch_existing — all return None
    session.execute = AsyncMock(side_effect=[result_none, result_none, result_none, result_none])
    repo = _make_partitioned_repo(session)

    # Act / Assert
    with pytest.raises(EventConcurrentUpdateError):
        await repo.create(event)


# ---------------------------------------------------------------------------
# _fetch_existing_event
# ---------------------------------------------------------------------------


async def test__fetch_existing_event__without_created_at_in_dedup__queries_by_message_id_consumer_group() -> None:
    # Arrange
    event = _make_inbox_event()
    db_event = _make_db_inbox_event()
    session = _make_session_returning(scalar_value=db_event)
    repo = _make_non_partitioned_repo(session)

    # Act
    result = await repo._fetch_existing_event(event)

    # Assert
    assert result is db_event
    # Exactly one execute call (no created_at filter branching)
    session.execute.assert_called_once()  # type: ignore[attr-defined]


async def test__fetch_existing_event__with_created_at_in_dedup__adds_created_at_filter() -> None:
    # Arrange
    event = _make_inbox_event()
    db_event = _make_db_inbox_event()
    session = _make_session_returning(scalar_value=db_event)
    repo = _make_partitioned_repo(session)

    # Act
    result = await repo._fetch_existing_event(event)

    # Assert
    assert result is db_event
    session.execute.assert_called_once()  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# get_by_message_id
# ---------------------------------------------------------------------------


async def test__get_by_message_id__db_event_found__returns_entity() -> None:
    # Arrange
    db_event = _make_db_inbox_event()
    session = _make_session_returning(scalar_value=db_event)
    repo = _make_non_partitioned_repo(session)

    # Act
    result = await repo.get_by_message_id("m1", "cg1")

    # Assert
    assert isinstance(result, InboxEvent)


async def test__get_by_message_id__not_found__returns_none() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_non_partitioned_repo(session)

    # Act
    result = await repo.get_by_message_id("missing", "cg1")

    # Assert
    assert result is None


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


async def test__exists__session_returns_id__returns_true() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=uuid4())
    repo = _make_non_partitioned_repo(session)

    # Act
    result = await repo.exists("m1", "cg1")

    # Assert
    assert result is True


async def test__exists__session_returns_none__returns_false() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_non_partitioned_repo(session)

    # Act
    result = await repo.exists("m1", "cg1")

    # Assert
    assert result is False


# ---------------------------------------------------------------------------
# has_completed_sibling_for_inbox_key
# ---------------------------------------------------------------------------


async def test__has_completed_sibling__no_created_at_in_dedup__returns_false_immediately() -> None:
    # Arrange – non-partitioned model has no created_at in dedup cols
    session = AsyncMock(spec=AsyncSession)
    repo = _make_non_partitioned_repo(session)

    # Act
    result = await repo.has_completed_sibling_for_inbox_key("m1", "cg1", uuid4())

    # Assert
    assert result is False
    session.execute.assert_not_called()  # type: ignore[attr-defined]


async def test__has_completed_sibling__partitioned_session_returns_id__returns_true() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=uuid4())
    repo = _make_partitioned_repo(session)

    # Act
    result = await repo.has_completed_sibling_for_inbox_key("m1", "cg1", uuid4())

    # Assert
    assert result is True


async def test__has_completed_sibling__partitioned_session_returns_none__returns_false() -> None:
    # Arrange
    session = _make_session_returning(scalar_value=None)
    repo = _make_partitioned_repo(session)

    # Act
    result = await repo.has_completed_sibling_for_inbox_key("m1", "cg1", uuid4())

    # Assert
    assert result is False
