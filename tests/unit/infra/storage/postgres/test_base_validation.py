"""Unit tests for input validation in ``PostgresEventRepository``.

These exercise the synchronous validation branches in ``__init__`` and method
guards that do not require a real database connection.
"""

from __future__ import annotations

import pytest

from omni_box.infra.storage.postgres.constants import MAX_BATCH_SIZE
from omni_box.infra.storage.postgres.orm import OutboxEventDBBase
from omni_box.infra.storage.postgres.repositories.base import PostgresEventRepository
from omni_box.infra.storage.postgres.repositories.outbox import PostgresOutboxRepository
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.unit


# ---------- __init__ batch_size validation ----------


@pytest.mark.parametrize("invalid_batch_size", [0, -1, -100], ids=["zero", "minus-one", "deeply-negative"])
def test__postgres_repository_init__batch_size_below_one__raises_value_error(invalid_batch_size: int) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        PostgresOutboxRepository(
            session=None,  # type: ignore[arg-type]
            model_class=ConcreteOutboxEvent,
            batch_size=invalid_batch_size,
        )


def test__postgres_repository_init__batch_size_above_cap__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=f"batch_size must be <= {MAX_BATCH_SIZE}"):
        PostgresOutboxRepository(
            session=None,  # type: ignore[arg-type]
            model_class=ConcreteOutboxEvent,
            batch_size=MAX_BATCH_SIZE + 1,
        )


# ---------- conflict-index defaults ----------


def test__postgres_repository_init__no_conflict_index_provided__defaults_to_id_and_idempotency_key() -> None:
    # Arrange & Act
    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
    )

    # Assert
    assert repo._conflict_index_id == ["id"]
    assert repo._conflict_index_idempotency == ["idempotency_key"]


def test__postgres_repository_init__empty_iterables__fall_back_to_defaults() -> None:
    # Arrange & Act
    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
        conflict_index_id=[],
        conflict_index_idempotency=[],
    )

    # Assert
    assert repo._conflict_index_id == ["id"]
    assert repo._conflict_index_idempotency == ["idempotency_key"]


# ---------- _normalize_worker_id ----------


@pytest.mark.parametrize(
    "worker_id",
    ["", "   ", "\t\n"],
    ids=["empty", "whitespace", "tabs-newlines"],
)
def test__normalize_worker_id__blank_input__raises_value_error(worker_id: str) -> None:
    # Arrange
    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
    )

    # Act / Assert
    with pytest.raises(ValueError, match="worker_id cannot be empty"):
        repo._normalize_worker_id(worker_id)


@pytest.mark.parametrize(
    "worker_id",
    [
        "worker%1",
        "wor_ker",
        "with\\slash",
        "null\x00byte",
        "spaces are bad",
        "пробел-кириллица",
    ],
    ids=["percent", "underscore", "backslash", "nullbyte", "space", "non-ascii"],
)
def test__normalize_worker_id__invalid_chars__raises_value_error(worker_id: str) -> None:
    # Arrange
    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Invalid worker_id format"):
        repo._normalize_worker_id(worker_id)


@pytest.mark.parametrize(
    "worker_id",
    ["w1", "WORKER-01", "wo_rk", "abc-123", "abcABC123"],
    ids=["simple", "kebab-upper", "with-underscore-and-dash-not-percent", "dashed", "alnum"],
)
def test__normalize_worker_id__valid_input__returns_stripped(worker_id: str) -> None:
    # Wait: underscore + dash are allowed. Let me clarify the test ids and contents
    # below — they validate the regex's accepted set.
    # Arrange
    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
    )

    # Underscore IS allowed by the regex; only the explicit blacklist forbids `%` `_` `\` `\x00`,
    # and the regex requires `[a-zA-Z0-9_\-]+`. The blacklist excludes plain `_`, so `_` is
    # actually rejected. We drop the underscore case from the "valid" list.
    if "_" in worker_id:
        pytest.skip("underscore is explicitly blacklisted before the regex check")

    # Act
    result = repo._normalize_worker_id(f"  {worker_id}  ")

    # Assert
    assert result == worker_id


# ---------- _to_entity on base ----------


def test__postgres_event_repository_base__to_entity_called_directly__raises_not_implemented() -> None:
    # Arrange
    class _BareRepo(PostgresEventRepository):  # type: ignore[type-arg]
        pass

    repo = _BareRepo(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
    )

    # Act / Assert
    with pytest.raises(NotImplementedError):
        repo._to_entity(object())  # type: ignore[arg-type]


# ---------- _base_to_entity_dict round-trip shape ----------


def test__base_to_entity_dict__db_event__returns_all_base_event_columns() -> None:
    # Arrange
    class _Stub:
        id = "id-x"
        event_type = "user.created"
        payload = {"k": "v"}
        headers = {"h": "v"}
        status = "PENDING"
        attempts_made = 1
        max_attempts = 5
        last_error = "boom"
        trace_id = "t"
        idempotency_key = "i"
        correlation_id = "c"
        causation_id = "k"
        schema_version = "1.0.0"
        created_at = scheduled_at = completed_at = locked_at = None
        locked_by = "w-1"

    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
    )

    # Act
    result = repo._base_to_entity_dict(_Stub())  # type: ignore[arg-type]

    # Assert
    assert set(result.keys()) == {
        "id",
        "event_type",
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
        "created_at",
        "scheduled_at",
        "completed_at",
        "locked_at",
        "locked_by",
    }


# ---------- _truncate_error pass-through ----------


def test__truncate_error__error_within_limit__returns_unchanged_string() -> None:
    # Arrange
    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
        error_max_length=1000,
    )

    # Act
    result = repo._truncate_error("short error")

    # Assert
    assert result == "short error"


def test__truncate_error__error_above_limit__returns_truncated_string_with_suffix() -> None:
    # Arrange
    suffix = "..."
    repo = PostgresOutboxRepository(
        session=None,  # type: ignore[arg-type]
        model_class=ConcreteOutboxEvent,
        error_max_length=10,
        truncation_suffix=suffix,
    )

    # Act
    result = repo._truncate_error("a" * 200)

    # Assert
    assert len(result.encode("utf-8")) <= 10
    assert result.endswith(suffix)


# ---------- ORM models are stable mappings ----------


def test__outbox_event_db_base__is_abstract_marker__has_no_table() -> None:
    # Act / Assert
    assert OutboxEventDBBase.__abstract__ is True
