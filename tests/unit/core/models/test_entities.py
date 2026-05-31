"""Unit tests for ``omni_box.core.models.entities``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from omni_box.core.models.entities import BaseEvent, InboxEvent, OutboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.models.schemas import BaseEventSchema
from omni_box.utils.datetime import utc_now

pytestmark = pytest.mark.unit

# ---------- Helpers ----------


def _outbox(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "aggregate_type": "User",
        "aggregate_id": uuid4(),
        "event_type": "user.created",
        "topic": "users",
        "partition_key": "k1",
        "payload": {"id": "u1"},
    }
    base.update(overrides)
    return base


def _inbox(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "message_id": "m1",
        "consumer_group": "cg1",
        "source": "src1",
        "event_type": "user.created",
        "payload": {"id": "u1"},
    }
    base.update(overrides)
    return base


# ---------- Construction defaults ----------


def test__outbox_event__valid_minimal__creates_pending_event() -> None:
    # Act
    event = OutboxEvent(**_outbox())

    # Assert
    assert event.status == EventStatus.PENDING
    assert event.attempts_made == 0
    assert event.is_locked is False
    assert event.can_retry is True


def test__inbox_event__valid_minimal__creates_event() -> None:
    # Act
    event = InboxEvent(**_inbox())

    # Assert
    assert event.event_type == "user.created"
    assert event.status == EventStatus.PENDING


# ---------- Timezone validation ----------


def test__base_event__naive_datetime__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="Datetime must be timezone-aware"):
        OutboxEvent(**_outbox(created_at=datetime.now()))


def test__base_event__non_utc_timezone__normalizes_to_utc() -> None:
    # Arrange: build a +5h tz timestamp.
    plus5 = timezone(timedelta(hours=5))
    created = datetime(2025, 1, 1, 12, 0, 0, tzinfo=plus5)

    # Act
    event = OutboxEvent(**_outbox(created_at=created, scheduled_at=created))

    # Assert
    assert event.created_at.utcoffset() == timedelta(0)


def test__base_event__none_completed_at__remains_none() -> None:
    # Act
    event = OutboxEvent(**_outbox())

    # Assert: completed_at default None and the validator returns None unchanged.
    assert event.completed_at is None


# ---------- scheduled_at bounds ----------


def test__base_event__scheduled_far_before_created__raises_validation_error() -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="cannot be significantly before created_at"):
        OutboxEvent(**_outbox(created_at=now, scheduled_at=now - timedelta(seconds=120)))


def test__base_event__scheduled_too_far_future__raises_validation_error() -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="is too far in the future"):
        OutboxEvent(**_outbox(created_at=now, scheduled_at=now + timedelta(days=400)))


@pytest.mark.parametrize(
    ("ctx", "match"),
    [
        ({"scheduled_at_skew_seconds": -1}, "scheduled_at_skew_seconds must be >= 0"),
        ({"scheduled_at_max_future_seconds": 0}, "scheduled_at_max_future_seconds must be >= 1"),
    ],
    ids=["negative-skew", "zero-max-future"],
)
def test__base_event__invalid_scheduled_at_context__raises_validation_error(ctx: dict[str, int], match: str) -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match=match):
        OutboxEvent.model_validate(_outbox(created_at=now, scheduled_at=now), context=ctx)


def test__base_event__custom_skew_context__enforced() -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="cannot be significantly before created_at"):
        OutboxEvent.model_validate(
            _outbox(created_at=now, scheduled_at=now - timedelta(seconds=10)),
            context={"scheduled_at_skew_seconds": 5},
        )


def test__base_event__custom_max_future_context__enforced() -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="is too far in the future"):
        OutboxEvent.model_validate(
            _outbox(created_at=now, scheduled_at=now + timedelta(seconds=100)),
            context={"scheduled_at_max_future_seconds": 50},
        )


# ---------- status/completed_at invariants ----------


def test__base_event__completed_status_without_timestamp__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="completed_at must be set when status is COMPLETED"):
        OutboxEvent(**_outbox(status=EventStatus.COMPLETED))


def test__base_event__completed_at_set_on_pending__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="completed_at must be None when status is"):
        OutboxEvent(**_outbox(status=EventStatus.PENDING, completed_at=utc_now()))


def test__base_event__completed_before_created__raises_validation_error() -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="cannot be before created_at"):
        OutboxEvent(
            **_outbox(
                status=EventStatus.COMPLETED,
                created_at=now,
                completed_at=now - timedelta(seconds=5),
            )
        )


def test__base_event__completed_before_scheduled__raises_validation_error() -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="cannot be before scheduled_at"):
        OutboxEvent(
            **_outbox(
                status=EventStatus.COMPLETED,
                created_at=now,
                scheduled_at=now + timedelta(seconds=10),
                completed_at=now + timedelta(seconds=5),
            )
        )


def test__base_event__completed_within_skew__is_accepted() -> None:
    # Arrange: completed_at slightly before created_at but within 1s skew limit.
    now = utc_now()

    # Act
    event = OutboxEvent(
        **_outbox(
            status=EventStatus.COMPLETED,
            attempts_made=0,
            max_attempts=6,
            created_at=now,
            scheduled_at=now,
            completed_at=now - timedelta(milliseconds=500),
        )
    )

    # Assert
    assert event.status == EventStatus.COMPLETED


# ---------- attempts invariants ----------


def test__base_event__attempts_made_above_max__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="cannot exceed max_attempts"):
        OutboxEvent(**_outbox(attempts_made=7, max_attempts=6))


def test__base_event__failed_with_wrong_attempts__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="status is FAILED, but attempts_made"):
        OutboxEvent(**_outbox(status=EventStatus.FAILED, attempts_made=3, max_attempts=6))


def test__base_event__pending_with_attempts_at_max__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="status is PENDING, but attempts_made"):
        OutboxEvent(**_outbox(status=EventStatus.PENDING, attempts_made=6, max_attempts=6))


# ---------- lock invariants ----------


def test__base_event__locked_at_without_locked_by__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="locked_at and locked_by must be both set"):
        OutboxEvent(**_outbox(locked_at=utc_now()))


def test__base_event__locked_by_without_locked_at__raises_validation_error() -> None:
    # Act / Assert
    with pytest.raises(ValidationError, match="locked_at and locked_by must be both set"):
        OutboxEvent(**_outbox(locked_by="w1"))


def test__base_event__locked_non_pending__raises_validation_error() -> None:
    # Arrange
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="locked event must be in PENDING status"):
        OutboxEvent(
            **_outbox(
                status=EventStatus.COMPLETED,
                locked_at=now,
                locked_by="w1",
                completed_at=now + timedelta(seconds=1),
            )
        )


# ---------- properties ----------


def test__base_event__pending_with_remaining_attempts__can_retry() -> None:
    # Arrange
    event = OutboxEvent(**_outbox(attempts_made=1, max_attempts=3))

    # Act / Assert
    assert event.attempts_left == 2
    assert event.failure_count == 0
    assert event.is_locked is False
    assert event.can_retry is True


def test__base_event__failed_event__failure_count_equals_attempts() -> None:
    # Arrange
    event = OutboxEvent(**_outbox(attempts_made=1, max_attempts=3))
    failed = event.model_copy(update={"status": EventStatus.FAILED, "attempts_made": 3})

    # Act / Assert
    assert failed.attempts_left == 0
    assert failed.failure_count == 3
    assert failed.can_retry is False


def test__base_event__completed_event__attempts_left_is_zero() -> None:
    # Arrange
    now = utc_now()
    event = OutboxEvent(
        **_outbox(
            status=EventStatus.COMPLETED,
            created_at=now,
            scheduled_at=now,
            completed_at=now + timedelta(seconds=1),
        )
    )

    # Act / Assert
    assert event.attempts_left == 0
    assert event.failure_count == 0


def test__base_event__locked_pending__is_locked_true() -> None:
    # Arrange
    event = OutboxEvent(**_outbox(locked_at=utc_now(), locked_by="w1"))

    # Act / Assert
    assert event.is_locked is True


# ---------- truncate_error ----------


def test__truncate_error__short_string__returns_unchanged() -> None:
    # Act / Assert
    assert BaseEvent.truncate_error("hello", 10, "...") == "hello"


def test__truncate_error__longer_than_max__truncates_with_suffix() -> None:
    # Arrange
    err = "a" * 100
    max_bytes = 10

    # Act
    result = BaseEvent.truncate_error(err, max_bytes, "...")

    # Assert
    assert len(result.encode("utf-8")) == max_bytes
    assert result.endswith("...")


def test__truncate_error__suffix_longer_than_max__hard_truncates() -> None:
    # Arrange
    max_bytes = 5

    # Act
    result = BaseEvent.truncate_error("too long error message", max_bytes, "suffix too long")

    # Assert
    assert len(result.encode("utf-8")) <= max_bytes


def test__truncate_error__strips_whitespace_before_check() -> None:
    # Act
    result = BaseEvent.truncate_error("  hi  ", 10, "...")

    # Assert
    assert result == "hi"


def test__truncate_error__empty_or_whitespace__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="Error message cannot be empty"):
        BaseEvent.truncate_error("  ", 10, "...")


def test__truncate_error__zero_max_bytes__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="max_bytes must be >= 1"):
        BaseEvent.truncate_error("foo", 0, "...")


# ---------- InboxEvent specific ----------


def test__inbox_event__completed__processed_at_aliases_completed_at() -> None:
    # Arrange
    now = utc_now()
    created = now - timedelta(seconds=10)

    # Act
    event = InboxEvent(
        **_inbox(
            status=EventStatus.COMPLETED,
            created_at=created,
            scheduled_at=created,
            completed_at=now,
        )
    )

    # Assert
    assert event.processed_at == now


def test__inbox_event__pending__processed_at_is_none() -> None:
    # Arrange
    event = InboxEvent(**_inbox())

    # Act / Assert
    assert event.processed_at is None


def test__inbox_event__no_headers__get_context_value_returns_none() -> None:
    # Arrange
    event = InboxEvent(**_inbox())

    # Act / Assert
    assert event.get_context_value("any") is None


def test__inbox_event__with_headers__get_context_value_returns_value() -> None:
    # Arrange
    event = InboxEvent(**_inbox(headers={"trace": "abc"}))

    # Act / Assert
    assert event.get_context_value("trace") == "abc"
    assert event.get_context_value("missing") is None


def test__inbox_event__get_payload_as_explicit_schema__parses_payload() -> None:
    # Arrange
    class S(BaseEventSchema, event_type="payload.explicit"):  # type: ignore[call-arg]
        foo: str

        @classmethod
        def schema_version(cls) -> str:
            return "1.0.0"

    event = InboxEvent(**_inbox(event_type="payload.explicit", payload={"foo": "bar"}, schema_version="1.0.0"))

    # Act
    result = event.get_payload_as(S)

    # Assert
    assert isinstance(result, S)
    assert result.foo == "bar"


def test__inbox_event__get_payload_as_auto_resolve__uses_registry() -> None:
    # Arrange
    class S(BaseEventSchema, event_type="payload.auto"):  # type: ignore[call-arg]
        foo: str

        @classmethod
        def schema_version(cls) -> str:
            return "1.0.0"

    event = InboxEvent(**_inbox(event_type="payload.auto", payload={"foo": "bar"}, schema_version="1.0.0"))

    # Act
    result: Any = event.get_payload_as()

    # Assert
    assert isinstance(result, S)
    assert result.foo == "bar"
