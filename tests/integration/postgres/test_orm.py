import pytest
from sqlalchemy import Connection, MetaData, String, Table, inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase

from omni_box.core.models.enums import EventStatus
from omni_box.infra.storage.postgres.orm import (
    InboxEventDBBase,
    InboxEventPartitionedDBBase,
    OutboxEventDBBase,
    OutboxEventPartitionedDBBase,
)
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.integration

CHECK_RULES = ("attempts_valid", "completed_status_consistency", "lock_consistency")

# sqlalchemy-foundation-kit's DB_NAMING_CONVENTION
FOUNDATION_KIT_CONVENTION = {
    "ix": "%(column_0_label)s_idx",
    "uq": "%(table_name)s_%(column_0_name)s_key",
    "ck": "%(table_name)s_%(constraint_name)s_check",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


def test__outbox_event_db_model__concrete_model__has_expected_columns_and_defaults() -> None:
    # Arrange
    table = ConcreteOutboxEvent.__table__

    # Act / Assert
    assert ConcreteOutboxEvent.__tablename__ == "outbox_events"

    expected_columns = {
        "id",
        "aggregate_type",
        "aggregate_id",
        "event_type",
        "topic",
        "partition_key",
        "payload",
        "headers",
        "status",
        "attempts_made",
        "max_attempts",
        "last_error",
        "trace_id",
        "idempotency_key",
        "correlation_id",
        "causation_id",
        "schema_version",
        "scheduled_at",
        "completed_at",
        "locked_at",
        "locked_by",
        "created_at",
        "updated_at",
    }
    actual_columns = set(table.columns.keys())
    assert expected_columns.issubset(actual_columns)

    assert table.columns["status"].default.arg == EventStatus.PENDING
    assert table.columns["attempts_made"].default.arg == 0
    assert table.columns["max_attempts"].default.arg == 6

    assert table.columns["id"].primary_key is True
    assert table.columns["aggregate_type"].nullable is False
    assert table.columns["aggregate_id"].nullable is False
    assert table.columns["event_type"].nullable is False
    assert table.columns["topic"].nullable is False
    assert table.columns["payload"].nullable is False
    assert table.columns["status"].nullable is False
    assert table.columns["scheduled_at"].nullable is False
    assert table.columns["schema_version"].nullable is True

    last_error_type = table.columns["last_error"].type
    assert isinstance(last_error_type, String)
    assert last_error_type.length == 2000

    idempotency_key_type = table.columns["idempotency_key"].type
    assert isinstance(idempotency_key_type, String)
    assert idempotency_key_type.length == 128

    correlation_id_type = table.columns["correlation_id"].type
    assert isinstance(correlation_id_type, String)
    assert correlation_id_type.length == 64

    causation_id_type = table.columns["causation_id"].type
    assert isinstance(causation_id_type, String)
    assert causation_id_type.length == 64


def test__outbox_event_db_model__concrete_model__has_expected_indexes() -> None:
    # Arrange
    table = ConcreteOutboxEvent.__table__

    # Act / Assert
    assert isinstance(table, Table)

    assert any(idx.name == "idx_outbox_events_pending_fetch" for idx in table.indexes)
    assert any(idx.name == "idx_outbox_events_locked_at" for idx in table.indexes)
    assert any(idx.name == "idx_outbox_events_completed_cleanup" for idx in table.indexes)

    indexed_columns = {col.name for idx in table.indexes for col in idx.columns}
    assert "created_at" in indexed_columns
    assert "updated_at" in indexed_columns

    pending_fetch_idx = next(idx for idx in table.indexes if idx.name == "idx_outbox_events_pending_fetch")
    assert "scheduled_at" in [c.name for c in pending_fetch_idx.columns]
    assert pending_fetch_idx.dialect_options["postgresql"]["where"] is not None
    assert EventStatus.PENDING.value in str(pending_fetch_idx.dialect_options["postgresql"]["where"])


async def test__event_bases__foundation_kit_convention__postgres_stores_the_qualified_names(
    db_engine: AsyncEngine,
) -> None:
    # Arrange
    schema = "naming_convention"

    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention=FOUNDATION_KIT_CONVENTION, schema=schema)

    class Outbox(Base, OutboxEventDBBase):
        pass

    class Inbox(Base, InboxEventDBBase):
        pass

    class OutboxPartitioned(Base, OutboxEventPartitionedDBBase):
        pass

    class InboxPartitioned(Base, InboxEventPartitionedDBBase):
        pass

    def reflect_check_names(conn: Connection) -> dict[str, set[str]]:
        inspector = inspect(conn)
        return {
            table.name: {c["name"] for c in inspector.get_check_constraints(table.name, schema=schema)}
            for table in Base.metadata.sorted_tables
        }

    # Act
    async with db_engine.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA {schema}"))
        try:
            await conn.run_sync(Base.metadata.create_all)
            stored = await conn.run_sync(reflect_check_names)
        finally:
            await conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))

    # Assert
    for table in Base.metadata.sorted_tables:
        assert stored[table.name] == {f"{table.name}_{rule}_check" for rule in CHECK_RULES}
