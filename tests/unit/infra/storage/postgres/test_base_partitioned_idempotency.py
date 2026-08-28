"""Unit tests for idempotent inserts on partitioned outbox tables.

On a partitioned table the idempotency unique index carries ``created_at`` and
cannot reject a retry by itself, so the repository serializes the key with an
advisory lock and a lookup before inserting. These tests pin that statement
sequence against a mocked session; the real-Postgres behaviour lives in
``tests/integration/postgres/test_partitioned_idempotency.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.storage.postgres.repositories.outbox import PostgresOutboxRepository
from tests.models import ConcreteOutboxEvent, ConcreteOutboxEventPartitioned

pytestmark = pytest.mark.unit

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


def _make_db_event(event_id: UUID | None = None, idempotency_key: str | None = None) -> MagicMock:
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
    db.idempotency_key = idempotency_key
    db.correlation_id = None
    db.causation_id = None
    db.schema_version = None
    db.created_at = _NOW
    db.scheduled_at = _NOW
    db.completed_at = None
    db.locked_at = None
    db.locked_by = None
    db.aggregate_type = "user"
    db.aggregate_id = uuid4()
    db.topic = "t"
    db.partition_key = "p"
    return db


def _result(scalar: object = None, rows: list | None = None) -> MagicMock:
    result = MagicMock()
    result.scalar_one_or_none.return_value = scalar
    result.scalars.return_value.all.return_value = rows or []
    return result


def _session(*results: MagicMock) -> AsyncSession:
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=list(results))
    return session


def _sql(session: AsyncSession, call_index: int) -> str:
    statement = session.execute.call_args_list[call_index].args[0]  # type: ignore[attr-defined]
    return str(statement.compile(dialect=postgresql.dialect()))


def _params(session: AsyncSession, call_index: int) -> dict:
    return session.execute.call_args_list[call_index].args[1]  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# create — partitioned table, idempotency_key set
# ---------------------------------------------------------------------------


async def test__create__partitioned_key_already_present__locks_looks_up_and_returns_existing_without_inserting() -> (
    None
):
    # Arrange
    event = _make_event(idempotency_key="idem-1")
    existing = _make_db_event(idempotency_key="idem-1")
    session = _session(_result(), _result(scalar=existing))
    repo = PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEventPartitioned)

    # Act
    result = await repo.create(event)

    # Assert
    assert result.id == existing.id
    assert session.execute.await_count == 2  # type: ignore[attr-defined]
    assert "pg_advisory_xact_lock" in _sql(session, 0)
    assert _params(session, 0) == {"namespace": "outbox_events_partitioned", "tokens": ["idem-1"]}
    assert "ORDER BY" in _sql(session, 1)
    assert "LIMIT" in _sql(session, 1)


async def test__create__partitioned_key_absent__locks_looks_up_then_inserts() -> None:
    # Arrange
    event = _make_event(idempotency_key="idem-1")
    session = _session(_result(), _result(scalar=None), _result(scalar=_make_db_event(event.id, "idem-1")))
    repo = PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEventPartitioned)

    # Act
    result = await repo.create(event)

    # Assert
    assert result.id == event.id
    assert session.execute.await_count == 3  # type: ignore[attr-defined]
    assert "INSERT INTO" in _sql(session, 2)


async def test__create__partitioned_insert_conflicts_after_lookup__returns_the_existing_row() -> None:
    # Arrange
    event = _make_event(idempotency_key="idem-1")
    existing = _make_db_event(idempotency_key="idem-1")
    session = _session(_result(), _result(scalar=None), _result(scalar=None), _result(scalar=existing))
    repo = PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEventPartitioned)

    # Act
    result = await repo.create(event)

    # Assert
    assert result.id == existing.id
    assert session.execute.await_count == 4  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("model_class", "idempotency_key"),
    [(ConcreteOutboxEvent, "idem-1"), (ConcreteOutboxEventPartitioned, None)],
    ids=["plain-table-with-key", "partitioned-table-without-key"],
)
async def test__create__index_enforces_the_identity__keeps_the_single_insert(
    model_class: type, idempotency_key: str | None
) -> None:
    # Arrange
    event = _make_event(idempotency_key=idempotency_key)
    session = _session(_result(scalar=_make_db_event(event.id, idempotency_key)))
    repo = PostgresOutboxRepository(session=session, model_class=model_class)

    # Act
    result = await repo.create(event)

    # Assert
    assert result.id == event.id
    assert session.execute.await_count == 1  # type: ignore[attr-defined]
    assert "pg_advisory_xact_lock" not in _sql(session, 0)


# ---------------------------------------------------------------------------
# _fetch_existing_event — earliest row wins, never MultipleResultsFound
# ---------------------------------------------------------------------------


async def test__fetch_existing_event__with_key__takes_the_earliest_row_instead_of_demanding_exactly_one() -> None:
    # Arrange
    event = _make_event(idempotency_key="idem-1")
    existing = _make_db_event(idempotency_key="idem-1")
    session = _session(_result(scalar=existing))
    repo = PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEvent)

    # Act
    found = await repo._fetch_existing_event(event)

    # Assert
    assert found is existing
    assert "ORDER BY" in _sql(session, 0)
    assert "LIMIT" in _sql(session, 0)


# ---------------------------------------------------------------------------
# bulk_create — partitioned table
# ---------------------------------------------------------------------------


async def test__bulk_create__partitioned__locks_every_key_once_and_treats_present_identities_as_skipped() -> None:
    # Arrange
    e1, e2, e3 = _make_event(idempotency_key="k1"), _make_event(idempotency_key="k2"), _make_event(idempotency_key="k1")
    existing_k2 = _make_db_event(idempotency_key="k2")
    inserted_e1 = _make_db_event(e1.id, "k1")
    session = _session(
        _result(),  # advisory locks for k1 and k2
        _result(rows=[existing_k2]),  # lookup: k2 is already there
        _result(rows=[inserted_e1]),  # batch insert of what is left (e1)
        _result(scalar=inserted_e1),  # e3, the in-batch duplicate, resolves to e1's row
    )
    repo = PostgresOutboxRepository(session=session, model_class=ConcreteOutboxEventPartitioned)

    # Act
    created = await repo.bulk_create([e1, e2, e3])

    # Assert
    assert _params(session, 0)["tokens"] == ["k1", "k2"]
    assert [entity.id for entity in created] == [existing_k2.id, inserted_e1.id, inserted_e1.id]
    assert session.execute.await_count == 4  # type: ignore[attr-defined]
