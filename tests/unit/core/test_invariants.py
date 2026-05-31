from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from omni_box.core.exceptions import (
    EventAlreadyLockedError,
    EventConcurrentUpdateError,
    EventLockedByAnotherWorkerError,
    EventNotLockedError,
    InvalidEventStateError,
)
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.services.domain import OmniBoxDomainService
from omni_box.utils import utc_now

pytestmark = pytest.mark.unit


@pytest.fixture
def service() -> OmniBoxDomainService:
    return OmniBoxDomainService()


def create_base_event() -> OutboxEvent:
    now = utc_now()
    return OutboxEvent(
        aggregate_type="test",
        aggregate_id=uuid4(),
        event_type="test.event",
        topic="test-topic",
        partition_key="test-key",
        payload={"data": "test"},
        created_at=now,
        scheduled_at=now,
    )


def test__is_lock_stale__various_times__returns_expected_staleness(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    now = utc_now()
    worker_id = "worker-1"
    locked_event = service.lock_event(event, worker_id, now)

    # Act / Assert
    assert not service.is_lock_stale(locked_event, now + timedelta(seconds=30), 60)
    assert not service.is_lock_stale(locked_event, now + timedelta(seconds=60), 60)
    assert not service.is_lock_stale(locked_event, now + timedelta(seconds=60, milliseconds=1), 60.002)
    assert service.is_lock_stale(locked_event, now + timedelta(seconds=61), 60)
    assert not service.is_lock_stale(event, now + timedelta(hours=10), 60)

    with pytest.raises(ValueError, match="must be positive"):
        service.is_lock_stale(locked_event, now, 0)
    with pytest.raises(ValueError, match="must be positive"):
        service.is_lock_stale(locked_event, now, -1)


def test__lock_unlock_flow__full_lifecycle__transitions_correctly(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    now = utc_now()
    worker_id = "worker-1"

    # Act / Assert: lock
    locked_event = service.lock_event(event, worker_id, now)
    assert locked_event.is_locked
    assert locked_event.locked_by == worker_id
    assert locked_event.locked_at == now

    with pytest.raises(EventAlreadyLockedError):
        service.lock_event(locked_event, worker_id, now)
    with pytest.raises(EventAlreadyLockedError):
        service.lock_event(locked_event, "worker-2", now)

    refreshed_at = now + timedelta(seconds=10)
    refreshed_event = service.refresh_event_lock(locked_event, worker_id, refreshed_at)
    assert refreshed_event.locked_at == refreshed_at
    assert refreshed_event.locked_by == worker_id

    with pytest.raises(EventLockedByAnotherWorkerError):
        service.refresh_event_lock(locked_event, "worker-2", refreshed_at)

    unlocked_event = service.unlock_event(locked_event, worker_id)
    assert not unlocked_event.is_locked
    assert unlocked_event.locked_by is None

    with pytest.raises(EventLockedByAnotherWorkerError):
        service.unlock_event(locked_event, "worker-2")


def test__force_unlock__various_inputs__validates_and_clears_lock(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    now = utc_now()
    worker_id = "worker-1"

    # Act / Assert: unlocked event raises
    with pytest.raises(EventNotLockedError):
        service.force_unlock_event(event, reason="cleanup")

    locked_event = service.lock_event(event, worker_id, now)
    assert locked_event.is_locked

    with pytest.raises(ValueError, match="Reason for force unlock cannot be empty or whitespace"):
        service.force_unlock_event(locked_event, reason="")
    with pytest.raises(ValueError, match="Reason for force unlock cannot be empty or whitespace"):
        service.force_unlock_event(locked_event, reason="   ")
    with pytest.raises(ValueError, match="Reason for force unlock is too long"):
        service.force_unlock_event(locked_event, reason="a" * 256)

    unlocked_event = service.force_unlock_event(locked_event, reason="stale lock cleanup")
    assert not unlocked_event.is_locked
    assert unlocked_event.locked_by is None


def test__force_unlock__reason_length_after_strip__passes_255_fails_256(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    worker_id = "worker-1"
    locked_event = service.lock_event(event, worker_id, utc_now())

    # Act / Assert: 255 chars + spaces passes
    long_reason = "a" * 255 + " " * 100
    unlocked = service.force_unlock_event(locked_event, reason=long_reason)
    assert not unlocked.is_locked

    # 256 chars after strip fails
    too_long_reason = "a" * 256 + " " * 10
    with pytest.raises(ValueError, match="Reason for force unlock is too long"):
        service.force_unlock_event(locked_event, reason=too_long_reason)


def test__locked_by_whitespace__lock_and_model_validate__normalizes_or_raises(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()

    # Act / Assert
    with pytest.raises(ValueError, match="worker_id cannot be empty or whitespace"):
        service.lock_event(event, "   ", utc_now())

    with pytest.raises(ValidationError, match="string cannot be empty or whitespace"):
        event.model_validate({**event.model_dump(mode="python"), "locked_at": utc_now(), "locked_by": "   "})

    locked = service.lock_event(event, "  worker-1  ", utc_now())
    assert locked.locked_by == "worker-1"

    valid_event = event.model_validate(
        {**event.model_dump(mode="python"), "locked_at": utc_now(), "locked_by": "  worker-1  "}
    )
    assert valid_event.locked_by == "worker-1"


def test__lock__overlong_worker_id__raises_validation_error(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    now = utc_now()

    # Act / Assert
    with pytest.raises(ValidationError, match="locked_by"):
        service.lock_event(event, "w" * 256, now)


def test__model_copy_methods__whitespace_worker_id__normalizes_in_all_methods(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    now = utc_now()

    # Act / Assert: all mutating methods strip worker_id
    locked = service.lock_event(event, "  worker-1  ", now)
    assert locked.locked_by == "worker-1"

    refreshed = service.refresh_event_lock(locked, "  worker-1  ", now)
    assert refreshed.locked_by == "worker-1"

    service.assert_locked_by(locked, "  worker-1  ")

    unlocked = service.unlock_event(locked, "  worker-1  ")
    assert not unlocked.is_locked

    published = service.mark_event_completed(locked, now, "  worker-1  ")
    assert published.status == EventStatus.COMPLETED
    assert not published.is_locked

    failed = service.mark_event_failed(locked, "error", "  worker-1  ")
    assert failed.status == EventStatus.PENDING
    assert not failed.is_locked


def test__mark_event_failed__whitespace_error__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    worker_id = "worker-1"
    locked_event = service.lock_event(event, worker_id, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="Error message cannot be empty or whitespace"):
        service.mark_event_failed(locked_event, "   ", worker_id=worker_id)


def test__assert_locked_by__various_states__raises_appropriately(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    worker_id = "worker-1"

    # Act / Assert
    with pytest.raises(EventNotLockedError):
        service.assert_locked_by(event, worker_id)

    locked_event = service.lock_event(event, worker_id, utc_now())
    service.assert_locked_by(locked_event, worker_id)

    with pytest.raises(EventLockedByAnotherWorkerError):
        service.assert_locked_by(locked_event, "worker-2")


def test__mark_event_completed__correct_worker__transitions_to_completed(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    worker_id = "worker-1"
    now = utc_now()
    locked_event = service.lock_event(event, worker_id, now)

    # Act
    pub_at = now + timedelta(seconds=1)
    published = service.mark_event_completed(locked_event, pub_at, worker_id=worker_id)

    # Assert
    assert not published.is_locked
    assert published.status == EventStatus.COMPLETED
    assert published.completed_at == pub_at

    with pytest.raises(EventLockedByAnotherWorkerError):
        service.mark_event_completed(locked_event, pub_at, worker_id="worker-2")

    with pytest.raises(InvalidEventStateError):
        service.lock_event(published, worker_id, now)


def test__mark_event_failed__correct_worker__increments_attempts(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    worker_id = "worker-1"
    now = utc_now()
    locked_event = service.lock_event(event, worker_id, now)

    # Act
    failed = service.mark_event_failed(locked_event, "error", worker_id=worker_id)

    # Assert
    assert not failed.is_locked
    assert failed.status == EventStatus.PENDING
    assert failed.attempts_made == 1
    assert failed.last_error == "error"

    with pytest.raises(EventLockedByAnotherWorkerError):
        service.mark_event_failed(locked_event, "error", worker_id="worker-2")


def test__mark_event_failed__max_attempts_reached__transitions_to_failed(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    event = create_base_event().model_copy(update={"max_attempts": 1})
    worker_id = "worker-1"
    locked_event = service.lock_event(event, worker_id, now)

    # Act
    failed = service.mark_event_failed(locked_event, "fatal", worker_id=worker_id)

    # Assert
    assert failed.status == EventStatus.FAILED
    assert failed.attempts_made == 1

    with pytest.raises(InvalidEventStateError):
        service.lock_event(failed, worker_id, now)

    with pytest.raises(ValidationError, match="cannot exceed max_attempts"):
        event.model_validate(
            {
                **event.model_dump(mode="python"),
                "status": EventStatus.FAILED,
                "attempts_made": event.max_attempts + 1,
            }
        )

    with pytest.raises(ValidationError, match="must equal max_attempts"):
        event.model_validate(
            {
                **event.model_dump(mode="python"),
                "status": EventStatus.FAILED,
                "attempts_made": event.max_attempts - 1,
            }
        )


def test__mark_event_failed__count_as_attempt_false_at_limit__stays_pending(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    max_attempts = 3
    event = create_base_event().model_copy(update={"max_attempts": max_attempts, "attempts_made": max_attempts - 1})
    worker_id = "worker-1"
    locked_event = service.lock_event(event, worker_id, now)
    next_retry = now + timedelta(minutes=1)

    # Act
    failed = service.mark_event_failed(
        locked_event,
        "transient error",
        worker_id=worker_id,
        count_as_attempt=False,
        next_retry_at=next_retry,
    )

    # Assert
    assert failed.status == EventStatus.PENDING
    assert failed.attempts_made == max_attempts - 1
    assert failed.last_error == "transient error"
    assert failed.scheduled_at == next_retry

    with pytest.raises(ValueError, match="next_retry_at must be provided"):
        service.mark_event_failed(
            locked_event,
            "error",
            worker_id=worker_id,
            count_as_attempt=False,
        )


def test__mark_event_failed__long_error_message__truncates_to_limit(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    worker_id = "worker-1"
    locked_event = service.lock_event(event, worker_id, utc_now())
    long_error = "a" * 3000

    # Act
    updated = service.mark_event_failed(locked_event, long_error, worker_id=worker_id)

    # Assert
    assert len(updated.last_error or "") <= 2000
    assert "..." in (updated.last_error or "")


def test__mark_event_failed__time_validations__raise_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    event = create_base_event()
    worker_id = "worker-1"
    locked_event = service.lock_event(event, worker_id, now)

    # Act / Assert
    with pytest.raises(ValueError, match="cannot be before created_at"):
        service.mark_event_failed(
            locked_event,
            "error",
            worker_id=worker_id,
            next_retry_at=now - timedelta(hours=1),
        )

    with pytest.raises(ValueError, match="cannot be before created_at"):
        service.mark_event_completed(locked_event, now - timedelta(seconds=1), worker_id=worker_id)

    future_scheduled = now + timedelta(minutes=5)
    future_event = create_base_event().model_copy(update={"scheduled_at": future_scheduled})
    locked_future = service.lock_event(future_event, worker_id, now)
    with pytest.raises(ValueError, match="cannot be before scheduled_at"):
        service.mark_event_completed(locked_future, now + timedelta(seconds=1), worker_id=worker_id)

    with pytest.raises(ValueError, match="Error message cannot be empty or whitespace"):
        service.mark_event_failed(locked_event, "", worker_id=worker_id)


def test__outbox_event__payload_exceeds_limit__raises_validation_error() -> None:
    # Arrange
    event = create_base_event()
    large_payload = {"data": "x" * 1_000_001}

    # Act / Assert
    with pytest.raises(ValidationError, match=r"Payload size exceeds 1000000 bytes limit"):
        event.model_validate({**event.model_dump(mode="python"), "payload": large_payload})


def test__outbox_event__payload_and_headers_ior__does_not_mutate_originals() -> None:
    # Arrange
    now = utc_now()
    event = OutboxEvent(
        aggregate_type="test",
        aggregate_id=uuid4(),
        event_type="test.event",
        topic="test-topic",
        partition_key="test-key",
        payload={"a": 1},
        headers={"k": "v"},
        created_at=now,
        scheduled_at=now,
    )

    # Act
    payload = event.payload
    payload |= {"b": 2}
    headers = event.headers
    assert headers is not None
    headers |= {"x": "y"}

    # Assert
    assert event.payload["a"] == 1
    assert headers["k"] == "v"


def test__outbox_event__deep_copied_payload__mutations_do_not_affect_event() -> None:
    # Arrange
    now = utc_now()
    input_payload: dict[str, Any] = {"obj": {"a": 1}, "arr": [{"x": 1}]}
    event = OutboxEvent(
        aggregate_type="test",
        aggregate_id=uuid4(),
        event_type="test.event",
        topic="test-topic",
        partition_key="test-key",
        payload=input_payload,
        created_at=now,
        scheduled_at=now,
    )

    # Act
    payload = event.payload
    assert isinstance(payload, dict)
    obj = payload["obj"]
    assert isinstance(obj, dict)
    arr = payload["arr"]
    assert isinstance(arr, list)
    input_payload["obj"]["a"] = 2
    input_payload["arr"].append({"x": 2})

    # Assert
    assert obj["a"] == 1
    assert len(arr) == 1


def test__outbox_event__completed_invariants__raise_on_invalid_completed_at() -> None:
    # Arrange
    event = create_base_event()
    data = event.model_dump(mode="python")

    # Act / Assert
    with pytest.raises(ValidationError, match="cannot be before created_at"):
        event.model_validate(
            {
                **data,
                "status": EventStatus.COMPLETED,
                "completed_at": event.created_at - timedelta(seconds=5),
            }
        )

    with pytest.raises(ValidationError, match="cannot be before scheduled_at"):
        event.model_validate(
            {
                **data,
                "status": EventStatus.COMPLETED,
                "scheduled_at": event.created_at + timedelta(minutes=5),
                "completed_at": event.created_at + timedelta(seconds=1),
            }
        )


def test__outbox_event__non_ascii_payload__does_not_inflate_size() -> None:
    # Arrange
    event = create_base_event()
    emoji = "\U0001f600"
    emoji_payload = {"msg": emoji * 100_000}

    # Act / Assert: should NOT raise (UTF-8 bytes ~400KB < 1MB limit)
    event.model_validate({**event.model_dump(mode="python"), "payload": emoji_payload})


def test__outbox_event__nan_and_infinity_in_payload__raise_validation_error() -> None:
    # Arrange
    event = create_base_event()

    # Act / Assert
    with pytest.raises(ValidationError, match="non-JSON-serializable"):
        event.model_validate({**event.model_dump(mode="python"), "payload": {"x": float("nan")}})

    with pytest.raises(ValidationError, match="non-JSON-serializable"):
        event.model_validate({**event.model_dump(mode="python"), "payload": {"x": float("inf")}})


def test__outbox_event__header_limits__raise_on_violation() -> None:
    # Arrange
    event = create_base_event()

    # Act / Assert
    with pytest.raises(ValidationError, match=r"Header key .* is too long"):
        event.model_validate({**event.model_dump(mode="python"), "headers": {"a" * 65: "v"}})

    with pytest.raises(ValidationError, match=r"Header value for .* is too long"):
        event.model_validate({**event.model_dump(mode="python"), "headers": {"k": "v" * 513}})

    with pytest.raises(ValidationError, match=r"Too many headers \(max 100\)"):
        too_many_headers = {f"k{i}": "v" for i in range(101)}
        event.model_validate({**event.model_dump(mode="python"), "headers": too_many_headers})


def test__mark_event_failed__no_backoff__scheduled_at_unchanged(service: OmniBoxDomainService) -> None:
    # Arrange
    event = create_base_event()
    now = utc_now()
    locked_event = service.lock_event(event, "worker-1", now)
    original_scheduled_at = locked_event.scheduled_at

    # Act
    updated = service.mark_event_failed(
        locked_event,
        "Transient error",
        worker_id="worker-1",
        count_as_attempt=True,
        next_retry_at=None,
    )

    # Assert
    assert updated.status == EventStatus.PENDING
    assert updated.attempts_made == 1
    assert updated.scheduled_at == original_scheduled_at
    assert updated.last_error == "Transient error"
    assert updated.locked_at is None
    assert updated.locked_by is None


def test__event_concurrent_update_error__with_many_missing_ids__truncates_display() -> None:
    # Arrange
    missing = [uuid4() for _ in range(12)]

    # Act
    err = EventConcurrentUpdateError(expected=20, actual=8, missing_ids=missing)

    # Assert
    assert "8" in str(err)
    assert "20" in str(err)
    assert "Missing IDs:" in str(err)
    assert str(missing[0]) in str(err)
    assert str(missing[9]) in str(err)
    assert str(missing[10]) not in str(err)
    assert "..." in str(err)


def test__truncate_error__invalid_max_bytes__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="max_bytes must be"):
        OutboxEvent.truncate_error("error", max_bytes=0, suffix="... [TRUNCATED]")
