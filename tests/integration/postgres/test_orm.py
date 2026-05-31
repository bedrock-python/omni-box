import pytest
from sqlalchemy import String, Table

from omni_box.core.models.enums import EventStatus
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.integration


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
