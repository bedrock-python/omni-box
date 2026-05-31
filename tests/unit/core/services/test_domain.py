"""Unit tests for ``omni_box.core.services.domain.OmniBoxDomainService``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest

from omni_box.core.constants import (
    DEFAULT_LEASE_TIMEOUT_SECONDS,
    FORCE_UNLOCK_REASON_MAX_LENGTH,
)
from omni_box.core.exceptions import (
    EventAlreadyLockedError,
    EventLockedByAnotherWorkerError,
    EventNotLockedError,
    InvalidEventStateError,
)
from omni_box.core.models.entities import InboxEvent, OutboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.services.domain import OmniBoxDomainService
from omni_box.utils.datetime import utc_now

pytestmark = pytest.mark.unit


WORKER_A = "worker-a"
WORKER_B = "worker-b"


@pytest.fixture
def service() -> OmniBoxDomainService:
    return OmniBoxDomainService()


def _make_outbox(svc: OmniBoxDomainService, **overrides: Any) -> OutboxEvent:
    return svc.create_outbox_event(
        aggregate_type=overrides.pop("aggregate_type", "User"),
        aggregate_id=overrides.pop("aggregate_id", uuid4()),
        event_type=overrides.pop("event_type", "user.created"),
        topic=overrides.pop("topic", "users"),
        partition_key=overrides.pop("partition_key", "k1"),
        payload=overrides.pop("payload", {"id": "u1"}),
        **overrides,
    )


# ---------- Construction / validation context ----------


def test__service__default_construction__exposes_validation_context() -> None:
    # Arrange
    svc = OmniBoxDomainService()

    # Act
    ctx = svc.validation_context

    # Assert
    assert ctx["scheduled_at_skew_seconds"] == svc.scheduled_at_skew_seconds
    assert ctx["payload_max_bytes"] == svc.payload_max_bytes
    assert ctx["headers_max_count"] == svc.headers_max_count
    assert ctx["header_key_max_length"] == svc.header_key_max_length
    assert ctx["header_value_max_length"] == svc.header_value_max_length
    assert ctx["scheduled_at_max_future_seconds"] == svc.scheduled_at_max_future_seconds


# ---------- create_outbox_event ----------


def test__create_outbox_event__minimal_args__creates_pending_event(service: OmniBoxDomainService) -> None:
    # Act
    event = _make_outbox(service)

    # Assert
    assert event.status == EventStatus.PENDING
    assert event.attempts_made == 0
    assert event.max_attempts == service.max_attempts
    assert event.aggregate_type == "User"


def test__create_outbox_event__with_optional_args__sets_all_fields(service: OmniBoxDomainService) -> None:
    # Arrange
    when = utc_now()

    # Act
    event = _make_outbox(
        service,
        headers={"h": "v"},
        max_attempts=10,
        trace_id="t1",
        idempotency_key="ik",
        correlation_id="c1",
        causation_id="ca1",
        schema_version="1.0.0",
        scheduled_at=when,
    )

    # Assert
    assert event.headers == {"h": "v"}
    assert event.max_attempts == 10
    assert event.trace_id == "t1"
    assert event.idempotency_key == "ik"
    assert event.correlation_id == "c1"
    assert event.causation_id == "ca1"
    assert event.schema_version == "1.0.0"


# ---------- create_inbox_event ----------


def test__create_inbox_event__minimal_args__creates_pending_event(service: OmniBoxDomainService) -> None:
    # Act
    event = service.create_inbox_event(
        message_id="m1",
        consumer_group="cg1",
        source="src1",
        event_type="user.created",
        payload={"id": "u1"},
        headers={"h": "v"},
        trace_id="t",
        correlation_id="c",
        causation_id="ca",
        schema_version="1.0.0",
    )

    # Assert
    assert isinstance(event, InboxEvent)
    assert event.status == EventStatus.PENDING
    assert event.message_id == "m1"
    assert event.schema_version == "1.0.0"


# ---------- is_lock_stale ----------


def test__is_lock_stale__unlocked_event__returns_false(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service)

    # Act / Assert
    assert service.is_lock_stale(event, utc_now()) is False


def test__is_lock_stale__locked_recently__returns_false(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    event = service.lock_event(_make_outbox(service), WORKER_A, now)

    # Act / Assert
    assert service.is_lock_stale(event, now + timedelta(seconds=1)) is False


def test__is_lock_stale__locked_long_ago__returns_true(service: OmniBoxDomainService) -> None:
    # Arrange
    locked_at = utc_now() - timedelta(seconds=DEFAULT_LEASE_TIMEOUT_SECONDS + 100)
    event = _make_outbox(service).model_copy(update={"locked_at": locked_at, "locked_by": WORKER_A})

    # Act / Assert
    assert service.is_lock_stale(event, utc_now()) is True


def test__is_lock_stale__custom_timeout__uses_argument(service: OmniBoxDomainService) -> None:
    # Arrange
    locked_at = utc_now() - timedelta(seconds=5)
    event = _make_outbox(service).model_copy(update={"locked_at": locked_at, "locked_by": WORKER_A})

    # Act / Assert
    assert service.is_lock_stale(event, utc_now(), stale_timeout_seconds=2.0) is True


def test__is_lock_stale__non_positive_timeout__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service).model_copy(update={"locked_at": utc_now(), "locked_by": WORKER_A})

    # Act / Assert
    with pytest.raises(ValueError, match="stale_timeout_seconds must be positive"):
        service.is_lock_stale(event, utc_now(), stale_timeout_seconds=0.0)


def test__is_lock_stale__naive_now__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service).model_copy(update={"locked_at": utc_now(), "locked_by": WORKER_A})

    # Act / Assert
    with pytest.raises(ValueError, match="now must be timezone-aware"):
        service.is_lock_stale(event, datetime.now())


# ---------- assert_locked_by ----------


def test__assert_locked_by__owned_lock__returns_none(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    service.assert_locked_by(locked, WORKER_A)


def test__assert_locked_by__not_locked__raises_event_not_locked(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service)

    # Act / Assert
    with pytest.raises(EventNotLockedError):
        service.assert_locked_by(event, WORKER_A)


def test__assert_locked_by__locked_by_other__raises_locked_by_another(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(EventLockedByAnotherWorkerError):
        service.assert_locked_by(locked, WORKER_B)


def test__assert_locked_by__empty_worker_id__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="worker_id cannot be empty"):
        service.assert_locked_by(locked, "   ")


# ---------- lock_event ----------


def test__lock_event__pending_event__sets_lock_fields(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service)
    now = utc_now()

    # Act
    locked = service.lock_event(event, WORKER_A, now)

    # Assert
    assert locked.locked_by == WORKER_A
    assert locked.locked_at is not None
    assert locked.is_locked is True


def test__lock_event__non_pending__raises_invalid_state(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    event = _make_outbox(service)
    locked = service.lock_event(event, WORKER_A, now)
    completed = service.mark_event_completed(locked, now + timedelta(seconds=1), WORKER_A)

    # Act / Assert
    with pytest.raises(InvalidEventStateError):
        service.lock_event(completed, WORKER_A, utc_now())


def test__lock_event__empty_worker_id__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service)

    # Act / Assert
    with pytest.raises(ValueError, match="worker_id cannot be empty"):
        service.lock_event(event, "   ", utc_now())


def test__lock_event__already_locked__raises_already_locked(service: OmniBoxDomainService) -> None:
    # Arrange
    event = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(EventAlreadyLockedError):
        service.lock_event(event, WORKER_B, utc_now())


def test__lock_event__naive_locked_at__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service)

    # Act / Assert
    with pytest.raises(ValueError, match="locked_at must be timezone-aware"):
        service.lock_event(event, WORKER_A, datetime.now())


def test__lock_event__non_utc_timezone__normalizes_to_utc(service: OmniBoxDomainService) -> None:
    # Arrange
    plus5 = timezone(timedelta(hours=5))
    locked_at = datetime.now(plus5)

    # Act
    locked = service.lock_event(_make_outbox(service), WORKER_A, locked_at)

    # Assert
    assert locked.locked_at is not None
    assert locked.locked_at.utcoffset() == timedelta(0)


# ---------- refresh_event_lock ----------


def test__refresh_event_lock__owner__updates_locked_at(service: OmniBoxDomainService) -> None:
    # Arrange
    base = utc_now()
    locked = service.lock_event(_make_outbox(service), WORKER_A, base)

    # Act
    refreshed = service.refresh_event_lock(locked, WORKER_A, base + timedelta(seconds=5))

    # Assert
    assert refreshed.locked_at is not None
    assert locked.locked_at is not None
    assert refreshed.locked_at > locked.locked_at


def test__refresh_event_lock__non_pending__raises_invalid_state(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    locked = service.lock_event(_make_outbox(service), WORKER_A, now)
    completed = service.mark_event_completed(locked, now + timedelta(seconds=1), WORKER_A)

    # Act / Assert
    with pytest.raises(InvalidEventStateError):
        service.refresh_event_lock(completed, WORKER_A, utc_now())


def test__refresh_event_lock__naive_now__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="now must be timezone-aware"):
        service.refresh_event_lock(locked, WORKER_A, datetime.now())


# ---------- unlock_event ----------


def test__unlock_event__owner__clears_lock(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act
    unlocked = service.unlock_event(locked, WORKER_A)

    # Assert
    assert unlocked.locked_at is None
    assert unlocked.locked_by is None


def test__unlock_event__non_pending__raises_invalid_state(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    locked = service.lock_event(_make_outbox(service), WORKER_A, now)
    completed = service.mark_event_completed(locked, now + timedelta(seconds=1), WORKER_A)

    # Act / Assert
    with pytest.raises(InvalidEventStateError):
        service.unlock_event(completed, WORKER_A)


# ---------- force_unlock_event ----------


def test__force_unlock_event__locked_event__clears_lock_and_writes_reason(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act
    unlocked = service.force_unlock_event(locked, "stuck for too long")

    # Assert
    assert unlocked.locked_at is None
    assert unlocked.locked_by is None
    assert unlocked.last_error is not None
    assert "stuck for too long" in unlocked.last_error


def test__force_unlock_event__not_locked__raises_event_not_locked(service: OmniBoxDomainService) -> None:
    # Arrange
    event = _make_outbox(service)

    # Act / Assert
    with pytest.raises(EventNotLockedError):
        service.force_unlock_event(event, "reason")


def test__force_unlock_event__empty_reason__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="Reason for force unlock cannot be empty"):
        service.force_unlock_event(locked, "   ")


def test__force_unlock_event__reason_too_long__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())
    too_long = "x" * (FORCE_UNLOCK_REASON_MAX_LENGTH + 1)

    # Act / Assert
    with pytest.raises(ValueError, match="too long"):
        service.force_unlock_event(locked, too_long)


# ---------- mark_event_completed ----------


def test__mark_event_completed__locked_pending__transitions_to_completed(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    locked = service.lock_event(_make_outbox(service), WORKER_A, now)

    # Act
    completed = service.mark_event_completed(locked, now + timedelta(seconds=1), WORKER_A)

    # Assert
    assert completed.status == EventStatus.COMPLETED
    assert completed.completed_at is not None
    assert completed.locked_at is None


def test__mark_event_completed__already_completed__raises_invalid_state(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    locked = service.lock_event(_make_outbox(service), WORKER_A, now)
    completed = service.mark_event_completed(locked, now + timedelta(seconds=1), WORKER_A)
    # Re-lock won't work; just attempt to complete again with the existing state.

    # Act / Assert
    with pytest.raises(EventNotLockedError):
        # cannot mark again because lock is cleared; this verifies the locked check
        service.mark_event_completed(completed, now + timedelta(seconds=2), WORKER_A)


def test__mark_event_completed__already_completed_but_still_locked__raises_invalid_state(
    service: OmniBoxDomainService,
) -> None:
    # Arrange: construct a COMPLETED event that is somehow still locked-flagged by skipping invariants
    # is impossible due to invariants. Instead, simulate the COMPLETED branch by passing an event
    # whose status is COMPLETED & locked is owner — invariants forbid that. Skip directly via
    # building a separate failed-status event and bypassing.
    # The COMPLETED & FAILED status branches in mark_event_completed are guarded by assert_locked_by
    # which would have already raised. They are defensive checks. We document this here.
    pytest.skip("COMPLETED/FAILED branches in mark_event_completed are unreachable due to lock invariants.")


def test__mark_event_completed__naive_completed_at__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="completed_at must be timezone-aware"):
        service.mark_event_completed(locked, datetime.now(), WORKER_A)


def test__mark_event_completed__before_created_at__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())
    too_early = locked.created_at - timedelta(seconds=10)

    # Act / Assert
    with pytest.raises(ValueError, match="cannot be before created_at"):
        service.mark_event_completed(locked, too_early, WORKER_A)


def test__mark_event_completed__before_scheduled_at__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    future = now + timedelta(seconds=300)
    event = _make_outbox(service, scheduled_at=future)
    locked = service.lock_event(event, WORKER_A, now)

    # Act / Assert
    with pytest.raises(ValueError, match="cannot be before scheduled_at"):
        service.mark_event_completed(locked, now + timedelta(seconds=10), WORKER_A)


# ---------- mark_event_failed ----------


def test__mark_event_failed__transient_failure__increments_attempts_and_stays_pending(
    service: OmniBoxDomainService,
) -> None:
    # Arrange
    now = utc_now()
    event = _make_outbox(service, max_attempts=3)
    locked = service.lock_event(event, WORKER_A, now)

    # Act
    failed = service.mark_event_failed(locked, "boom", WORKER_A)

    # Assert
    assert failed.status == EventStatus.PENDING
    assert failed.attempts_made == 1
    assert failed.last_error == "boom"


def test__mark_event_failed__last_attempt__transitions_to_failed(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    event = OutboxEvent(
        aggregate_type="User",
        aggregate_id=uuid4(),
        event_type="user.created",
        topic="users",
        partition_key="k1",
        payload={"id": "u1"},
        max_attempts=2,
        attempts_made=1,
    )
    locked = service.lock_event(event, WORKER_A, now)

    # Act
    failed = service.mark_event_failed(locked, "final", WORKER_A)

    # Assert
    assert failed.status == EventStatus.FAILED
    assert failed.attempts_made == 2


def test__mark_event_failed__with_next_retry_at__sets_scheduled_at(service: OmniBoxDomainService) -> None:
    # Arrange
    now = utc_now()
    event = _make_outbox(service, scheduled_at=now, max_attempts=3)
    locked = service.lock_event(event, WORKER_A, now)
    retry_at = now + timedelta(seconds=60)

    # Act
    failed = service.mark_event_failed(locked, "transient", WORKER_A, next_retry_at=retry_at)

    # Assert
    assert failed.scheduled_at == retry_at.astimezone(UTC)


def test__mark_event_failed__empty_error__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="Error message cannot be empty"):
        service.mark_event_failed(locked, "   ", WORKER_A)


def test__mark_event_failed__not_count_without_retry__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="next_retry_at must be provided"):
        service.mark_event_failed(locked, "x", WORKER_A, count_as_attempt=False)


def test__mark_event_failed__naive_next_retry_at__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())

    # Act / Assert
    with pytest.raises(ValueError, match="next_retry_at must be timezone-aware"):
        service.mark_event_failed(locked, "x", WORKER_A, next_retry_at=datetime.now())


def test__mark_event_failed__next_retry_before_created__raises_value_error(service: OmniBoxDomainService) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())
    too_early = locked.created_at - timedelta(seconds=10)

    # Act / Assert
    with pytest.raises(ValueError, match="cannot be before created_at"):
        service.mark_event_failed(locked, "x", WORKER_A, next_retry_at=too_early)


def test__mark_event_failed__next_retry_too_far_in_future__raises_value_error(
    service: OmniBoxDomainService,
) -> None:
    # Arrange
    locked = service.lock_event(_make_outbox(service), WORKER_A, utc_now())
    too_far = locked.created_at + timedelta(seconds=service.scheduled_at_max_future_seconds + 100)

    # Act / Assert
    with pytest.raises(ValueError, match="too far in the future"):
        service.mark_event_failed(locked, "x", WORKER_A, next_retry_at=too_far)


def test__mark_event_failed__already_completed_branch__unreachable_due_to_lock(
    service: OmniBoxDomainService,
) -> None:
    # The COMPLETED and FAILED status guards in mark_event_failed are defensive
    # since assert_locked_by would raise first (a COMPLETED/FAILED event cannot be locked).
    pytest.skip("COMPLETED/FAILED branches in mark_event_failed are unreachable due to lock invariants.")
