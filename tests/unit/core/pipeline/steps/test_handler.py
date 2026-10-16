"""Unit tests for ``omni_box.core.pipeline.steps.handler``."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from omni_box.core.exceptions import TransientError
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.handler import HandlerExecutionStep
from omni_box.core.services.results import EventHandlerResult, EventHandlerStatus, handler_retry
from omni_box.utils import utc_now
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


@pytest.fixture
def context() -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]


async def test__handler_step__handler_returns_none__marks_completed(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        return None

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event()

    # Act
    result = await step.execute(event, context)

    # Assert
    assert result.should_skip_event is False
    assert event.id in context.completed_ids


async def test__handler_step__handler_returns_explicit_success__marks_completed_with_status(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return EventHandlerResult(success=True, status="ok")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert event.id in context.completed_ids
    assert context.statuses[event.id] == "ok"


async def test__handler_step__handler_marks_skipped__marks_skipped_with_reason(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return EventHandlerResult(
            success=False,
            processed=False,
            error_message="not for me",
            status=EventHandlerStatus.SKIPPED,
        )

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert event.id in context.skipped_ids


async def test__handler_step__handler_skipped_without_message__uses_default_reason(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange: error_message is None -> "Explicitly skipped" branch
    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return EventHandlerResult(success=False, processed=False)

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert event.id in context.skipped_ids


async def test__handler_step__handler_explicit_failure__marks_failed(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return EventHandlerResult(
            success=False,
            processed=True,
            error_message="bad",
            count_as_attempt=True,
            status="failed",
        )

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert len(context.failed_counted) == 1
    assert context.failed_counted[0].event_id == event.id


async def test__handler_step__handler_failure_without_message__uses_unknown_error(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange: error_message None -> "Unknown error" branch
    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return EventHandlerResult(success=False, processed=True)

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert context.failed_counted[0].error == "Unknown error"


async def test__handler_step__handler_times_out__marks_failed_with_timeout_message(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    timeout = 0.01

    async def slow_handler(event: OutboxEvent, repo: object) -> None:
        # Use a future that never resolves; asyncio.wait_for cancels it on timeout
        await asyncio.Future()

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(slow_handler, timeout=timeout)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert len(context.failed_counted) == 1
    assert "timed out" in context.failed_counted[0].error


async def test__handler_step__handler_raises_unexpected__marks_failed_with_exc_type(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def broken_handler(event: OutboxEvent, repo: object) -> None:
        raise ValueError("kaboom")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(broken_handler)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    failure = context.failed_counted[0]
    assert failure.event_id == event.id
    assert "ValueError" in failure.error
    assert "kaboom" in failure.error


async def test__handler_step__handler_raises_transient_error__marks_failed_noncounted_and_reschedules(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        raise TransientError("payment gateway unreachable")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event()
    before = utc_now()

    # Act
    await step.execute(event, context)

    # Assert
    assert context.failed_counted == []
    failure = context.failed_noncounted[0]
    assert failure.event_id == event.id
    assert failure.error == "payment gateway unreachable"
    assert failure.next_retry_at is not None
    assert failure.next_retry_at > before
    assert context.statuses[event.id] == EventHandlerStatus.RETRY


# -------- backoff of counted failures --------


_SLACK = 1.0
"""Seconds allowed for the step's own run time between ``before`` and ``after``."""


def _delay(failure_at: datetime | None, before: datetime, after: datetime) -> tuple[float, float]:
    """The retry time minus the step's end and start: the delay is within ``[low, high]``."""
    assert failure_at is not None
    return (failure_at - after).total_seconds(), (failure_at - before).total_seconds()


async def test__handler_step__handler_raises__counted_retry_backs_off_from_the_base(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        raise PermissionError("not authorized for topic invoices")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    before = utc_now()

    # Act
    await step.execute(create_fake_event(), context)

    # Assert: the first retry waits between half the base (10 s) and all of it
    after = utc_now()
    low, high = _delay(context.failed_counted[0].next_retry_at, before, after)
    assert low >= 5.0 - _SLACK
    assert high <= 10.0 + _SLACK


async def test__handler_step__later_attempt__backoff_doubles_per_attempt_spent(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        raise PermissionError("still not authorized")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)
    event = create_fake_event().model_copy(update={"attempts_made": 3})
    before = utc_now()

    # Act
    await step.execute(event, context)

    # Assert: 10 s * 2**3 = 80 s, equal jitter keeps it in [40 s, 80 s]
    low, high = _delay(context.failed_counted[0].next_retry_at, before, utc_now())
    assert low >= 40.0 - _SLACK
    assert high <= 80.0 + _SLACK


async def test__handler_step__backoff_reaches_the_cap__stays_under_it(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        raise PermissionError("still not authorized")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(
        handler, retry_backoff_base_seconds=10.0, retry_backoff_cap_seconds=15.0
    )
    event = create_fake_event().model_copy(update={"attempts_made": 5})
    before = utc_now()

    # Act
    await step.execute(event, context)

    # Assert
    low, high = _delay(context.failed_counted[0].next_retry_at, before, utc_now())
    assert low >= 7.5 - _SLACK
    assert high <= 15.0 + _SLACK


async def test__handler_step__handler_times_out__counted_retry_backs_off(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        await asyncio.sleep(1)

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler, timeout=0.01)
    before = utc_now()

    # Act
    await step.execute(create_fake_event(), context)

    # Assert
    low, high = _delay(context.failed_counted[0].next_retry_at, before, utc_now())
    assert low >= 5.0 - _SLACK
    assert high <= 10.0 + _SLACK


async def test__handler_step__bare_handler_retry__backs_off(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return handler_retry("downstream returned 503")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)

    # Act
    await step.execute(create_fake_event(), context)

    # Assert
    assert context.failed_counted[0].next_retry_at is not None
    assert context.failed_counted[0].next_retry_at > utc_now() + timedelta(seconds=4)


async def test__handler_step__handler_retry_with_a_time__keeps_that_time(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    retry_at = utc_now() + timedelta(minutes=7)

    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return handler_retry("come back later", next_retry_at=retry_at)

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)

    # Act
    await step.execute(create_fake_event(), context)

    # Assert
    assert context.failed_counted[0].next_retry_at == retry_at


async def test__handler_step__noncounted_handler_retry_without_a_time__is_not_backed_off(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> EventHandlerResult:
        return handler_retry("not my fault", count_as_attempt=False)

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler)

    # Act
    await step.execute(create_fake_event(), context)

    # Assert
    assert context.failed_noncounted[0].next_retry_at is None


async def test__handler_step__backoff_base_zero__retries_on_the_next_cycle(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        raise PermissionError("not authorized")

    step: HandlerExecutionStep[OutboxEvent] = HandlerExecutionStep(handler, retry_backoff_base_seconds=0)

    # Act
    await step.execute(create_fake_event(), context)

    # Assert: no time given, so the row keeps its schedule, as before the backoff existed
    assert context.failed_counted[0].next_retry_at is None


@pytest.mark.parametrize(
    ("base", "cap"),
    [(-1.0, 3600.0), (10.0, 0.0)],
    ids=["negative-base", "zero-cap"],
)
def test__handler_step__invalid_backoff__raises(base: float, cap: float) -> None:
    # Arrange
    async def handler(event: OutboxEvent, repo: object) -> None:
        return None

    # Act / Assert
    with pytest.raises(ValueError, match="retry_backoff"):
        HandlerExecutionStep(handler, retry_backoff_base_seconds=base, retry_backoff_cap_seconds=cap)
