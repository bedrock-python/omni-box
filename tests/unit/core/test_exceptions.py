"""Unit tests for domain exceptions."""

from __future__ import annotations

from uuid import uuid4

import pytest

from omni_box.core.exceptions import (
    EventAlreadyLockedError,
    EventConcurrentUpdateError,
    EventLockedByAnotherWorkerError,
    EventNotLockedError,
    InvalidEventStateError,
    UnsupportedCapabilityError,
)

pytestmark = pytest.mark.unit


def test__event_not_locked_error__created_with_id__stores_id_and_includes_in_str() -> None:
    # Arrange
    ev_id = uuid4()

    # Act
    exc = EventNotLockedError(ev_id)

    # Assert
    assert exc.event_id == ev_id
    assert str(ev_id) in str(exc)


def test__event_locked_by_another_worker_error__created_with_ids__stores_all_fields() -> None:
    # Arrange
    ev_id = uuid4()

    # Act
    exc = EventLockedByAnotherWorkerError(ev_id, "w1", "w2")

    # Assert
    assert exc.event_id == ev_id
    assert exc.locked_by == "w1"
    assert exc.worker_id == "w2"
    assert "locked by w1" in str(exc)
    assert "attempted by w2" in str(exc)


def test__event_already_locked_error__created_with_id__includes_worker_in_str() -> None:
    # Arrange
    ev_id = uuid4()

    # Act
    exc = EventAlreadyLockedError(ev_id, "w1")

    # Assert
    assert exc.event_id == ev_id
    assert exc.locked_by == "w1"
    assert "already locked by w1" in str(exc)


def test__invalid_event_state_error__default_message__includes_status_and_expected() -> None:
    # Arrange
    ev_id = uuid4()

    # Act
    exc = InvalidEventStateError(ev_id, "COMPLETED", ["PENDING"])

    # Assert
    assert exc.event_id == ev_id
    assert exc.current_status == "COMPLETED"
    assert exc.expected_statuses == ["PENDING"]
    assert "in state 'COMPLETED'" in str(exc)
    assert "expected one of: PENDING" in str(exc)


def test__invalid_event_state_error__custom_message__includes_custom_message() -> None:
    # Arrange
    ev_id = uuid4()

    # Act
    exc2 = InvalidEventStateError(ev_id, "COMPLETED", ["PENDING"], message="Custom error")

    # Assert
    assert "Custom: Custom error" in str(exc2) or "Custom error" in str(exc2)


def test__event_concurrent_update_error__no_missing_ids__formats_counts() -> None:
    # Arrange / Act
    exc = EventConcurrentUpdateError(expected=5, actual=3)

    # Assert
    assert exc.expected == 5
    assert exc.actual == 3
    assert "expected 5 rows, but updated 3" in str(exc)


def test__event_concurrent_update_error__with_few_missing_ids__lists_all_ids() -> None:
    # Arrange
    ids = [uuid4() for _ in range(3)]

    # Act
    exc = EventConcurrentUpdateError(expected=3, actual=0, missing_ids=ids)

    # Assert
    assert str(ids[0]) in str(exc)
    assert "..." not in str(exc)


def test__event_concurrent_update_error__with_many_missing_ids__truncates_list() -> None:
    # Arrange
    ids = [uuid4() for _ in range(12)]

    # Act
    exc2 = EventConcurrentUpdateError(expected=12, actual=0, missing_ids=ids)

    # Assert
    assert str(ids[0]) in str(exc2)
    assert "..." in str(exc2)


def test__unsupported_capability_error__created__includes_repo_and_capability_in_str() -> None:
    # Arrange / Act
    exc = UnsupportedCapabilityError("BulkOps", "MyRepo")

    # Assert
    assert exc.capability == "BulkOps"
    assert exc.repo_type == "MyRepo"
    assert "MyRepo does not support BulkOps" in str(exc)
