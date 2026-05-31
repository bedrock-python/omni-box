"""Unit tests for ``omni_box.core.services.results``."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.types import EventFailureUpdate
from omni_box.core.services.results import (
    BatchProcessingResult,
    EventHandlerResult,
    EventHandlerStatus,
    coerce_handler_outcome,
    handler_completed,
    handler_retry,
    handler_skipped,
)

pytestmark = pytest.mark.unit


# ---------- EventHandlerStatus enum ----------


@pytest.mark.parametrize(
    ("member", "expected"),
    [
        (EventHandlerStatus.COMPLETED, "completed"),
        (EventHandlerStatus.STALE, "stale"),
        (EventHandlerStatus.SKIPPED, "skipped"),
        (EventHandlerStatus.FAILED, "failed"),
        (EventHandlerStatus.RETRY, "retry"),
    ],
    ids=["completed", "stale", "skipped", "failed", "retry"],
)
def test__event_handler_status__member__has_expected_value(member: EventHandlerStatus, expected: str) -> None:
    # Act / Assert
    assert member.value == expected


# ---------- EventHandlerResult dataclass ----------


def test__event_handler_result__defaults__are_correct() -> None:
    # Act
    result = EventHandlerResult(success=True)

    # Assert
    assert result.success is True
    assert result.processed is True
    assert result.status is None
    assert result.error_message is None
    assert result.count_as_attempt is True
    assert result.next_retry_at is None


def test__event_handler_result__frozen__cannot_mutate() -> None:
    # Arrange
    result = EventHandlerResult(success=True)

    # Act / Assert
    with pytest.raises(AttributeError):
        result.success = False  # type: ignore[misc]


# ---------- coerce_handler_outcome ----------


def test__coerce_handler_outcome__none__returns_default_completion() -> None:
    # Act
    result = coerce_handler_outcome(None)

    # Assert
    assert result.success is True
    assert result.status == EventHandlerStatus.COMPLETED


def test__coerce_handler_outcome__existing_result__returns_same_instance() -> None:
    # Arrange
    original = EventHandlerResult(success=False, error_message="x")

    # Act
    result = coerce_handler_outcome(original)

    # Assert
    assert result is original


# ---------- handler_* factories ----------


def test__handler_completed__default_status__is_completed() -> None:
    # Act
    result = handler_completed()

    # Assert
    assert result.success is True
    assert result.status == EventHandlerStatus.COMPLETED


def test__handler_completed__custom_status__is_preserved() -> None:
    # Act
    result = handler_completed(status="custom-success")

    # Assert
    assert result.status == "custom-success"


def test__handler_skipped__default_values__skip_does_not_count_attempt() -> None:
    # Act
    result = handler_skipped()

    # Assert
    assert result.success is False
    assert result.processed is False
    assert result.count_as_attempt is False
    assert result.status == EventHandlerStatus.SKIPPED


def test__handler_retry__message_and_options__produces_retry_result() -> None:
    # Arrange
    when = datetime.now(UTC)

    # Act
    result = handler_retry("transient failure", count_as_attempt=False, next_retry_at=when)

    # Assert
    assert result.success is False
    assert result.error_message == "transient failure"
    assert result.count_as_attempt is False
    assert result.next_retry_at == when
    assert result.status == EventHandlerStatus.RETRY


def test__handler_retry__defaults__counts_as_attempt() -> None:
    # Act
    result = handler_retry("err")

    # Assert
    assert result.count_as_attempt is True
    assert result.next_retry_at is None


# ---------- BatchProcessingResult ----------


def test__batch_processing_result__defaults__are_empty_collections() -> None:
    # Act
    batch: BatchProcessingResult[OutboxEvent] = BatchProcessingResult()

    # Assert
    assert batch.processed_event_ids == []
    assert batch.failed_counted == []
    assert batch.failed_noncounted == []
    assert batch.remaining_event_ids == set()
    assert batch.commit_failed is False


def test__batch_processing_result__custom_values__are_preserved() -> None:
    # Arrange
    event_id = uuid4()
    failure = EventFailureUpdate(event_id=event_id, error="boom")

    # Act
    batch: BatchProcessingResult[OutboxEvent] = BatchProcessingResult(
        processed_event_ids=[event_id],
        failed_counted=[failure],
        failed_noncounted=[],
        remaining_event_ids={event_id},
        commit_failed=True,
    )

    # Assert
    assert batch.processed_event_ids == [event_id]
    assert batch.failed_counted == [failure]
    assert batch.commit_failed is True
