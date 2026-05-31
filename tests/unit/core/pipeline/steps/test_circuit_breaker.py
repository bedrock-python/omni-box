"""Unit tests for ``omni_box.core.pipeline.steps.circuit_breaker``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.circuit_breaker import CircuitBreakerStep
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


@pytest.fixture
def context() -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]


async def test__circuit_breaker__initial_state__execute_returns_next(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep()
    event = create_fake_event()

    # Act
    result = await step.execute(event, context)

    # Assert
    assert result.should_skip_event is False
    assert result.should_stop_pipeline is False


async def test__circuit_breaker__failure_below_threshold__does_not_open(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep(failure_threshold=3)
    event = create_fake_event()
    context.mark_failed(event.id, "x", count_as_attempt=True)

    # Act
    await step.on_event_end(event, context)

    # Assert
    assert step._is_open is False
    assert step._consecutive_failures == 1


async def test__circuit_breaker__failures_reach_threshold__opens_breaker(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    threshold = 2
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep(failure_threshold=threshold)

    # Act
    for _ in range(threshold):
        ev = create_fake_event(id=uuid4())
        context.mark_failed(ev.id, "boom", count_as_attempt=True)
        await step.on_event_end(ev, context)

    # Assert
    assert step._is_open is True
    assert step._opened_at is not None


async def test__circuit_breaker__success_after_failure__resets_counter(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep(failure_threshold=2)
    failed_event = create_fake_event(id=uuid4())
    context.mark_failed(failed_event.id, "x", count_as_attempt=True)
    await step.on_event_end(failed_event, context)
    assert step._consecutive_failures == 1

    # Act: a success (no failure recorded) resets
    good_event = create_fake_event(id=uuid4())
    await step.on_event_end(good_event, context)

    # Assert
    assert step._consecutive_failures == 0
    assert step._is_open is False
    assert step._opened_at is None


async def test__circuit_breaker__success_with_no_prior_failures__keeps_counter_zero(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange: no prior failures; covers the else-branch where counter is 0
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep()
    event = create_fake_event()

    # Act
    await step.on_event_end(event, context)

    # Assert
    assert step._consecutive_failures == 0


async def test__circuit_breaker__open__execute_stops_pipeline(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep(failure_threshold=1)
    failed_event = create_fake_event()
    context.mark_failed(failed_event.id, "boom")
    await step.on_event_end(failed_event, context)
    assert step._is_open is True

    # Act
    result = await step.execute(create_fake_event(), context)

    # Assert
    assert result.should_stop_pipeline is True


async def test__circuit_breaker__open_past_recovery_timeout__half_opens_on_next_execute(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    recovery_seconds = 30
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep(
        failure_threshold=1,
        recovery_timeout_seconds=recovery_seconds,
    )
    failed_event = create_fake_event()
    context.mark_failed(failed_event.id, "boom")
    await step.on_event_end(failed_event, context)
    # Pretend the breaker has been open for longer than the recovery window
    step._opened_at = datetime.now(UTC) - timedelta(seconds=recovery_seconds + 5)

    # Act
    result = await step.execute(create_fake_event(), context)

    # Assert
    assert result.should_stop_pipeline is False
    assert step._is_open is False
    assert step._opened_at is None


async def test__circuit_breaker__threshold_already_open__on_event_end_does_not_reopen(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange: open the breaker first
    step: CircuitBreakerStep[OutboxEvent] = CircuitBreakerStep(failure_threshold=1)
    first = create_fake_event(id=uuid4())
    context.mark_failed(first.id, "boom")
    await step.on_event_end(first, context)
    original_opened_at = step._opened_at
    assert step._is_open is True

    # Act: another failure when already open does not reset opened_at
    next_event = create_fake_event(id=uuid4())
    context.mark_failed(next_event.id, "boom")
    await step.on_event_end(next_event, context)

    # Assert
    assert step._is_open is True
    assert step._opened_at == original_opened_at
