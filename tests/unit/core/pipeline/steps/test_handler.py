"""Unit tests for ``omni_box.core.pipeline.steps.handler``."""

from __future__ import annotations

import asyncio

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.handler import HandlerExecutionStep
from omni_box.core.services.results import EventHandlerResult, EventHandlerStatus
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
