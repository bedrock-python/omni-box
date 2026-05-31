"""Unit tests for handler execution step."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.handler import HandlerExecutionStep
from omni_box.core.services.results import EventHandlerStatus, handler_completed, handler_retry

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test__handler_execution_step__handler_returns_completed__marks_completed() -> None:
    # Arrange
    handler = AsyncMock(return_value=handler_completed())
    step = HandlerExecutionStep(handler)

    event = MagicMock()
    event.id = uuid4()
    context = MagicMock(spec=ProcessingContext)
    context.repo = MagicMock()

    # Act
    await step.execute(event, context)

    # Assert
    handler.assert_called_once_with(event, context.repo)
    context.mark_completed.assert_called_once_with(event.id, status=EventHandlerStatus.COMPLETED)


@pytest.mark.asyncio
async def test__handler_execution_step__handler_returns_retry__marks_failed_noncounted() -> None:
    # Arrange
    handler = AsyncMock(return_value=handler_retry("fail", count_as_attempt=False))
    step = HandlerExecutionStep(handler)

    event = MagicMock()
    event.id = uuid4()
    context = MagicMock(spec=ProcessingContext)
    context.repo = MagicMock()

    # Act
    await step.execute(event, context)

    # Assert
    context.mark_failed.assert_called_once_with(
        event.id, "fail", count_as_attempt=False, next_retry_at=None, status=EventHandlerStatus.RETRY
    )


@pytest.mark.asyncio
async def test__handler_execution_step__handler_times_out__marks_failed_with_timeout_message() -> None:
    # Arrange
    async def slow_handler(_, __):
        await asyncio.sleep(1)
        return handler_completed()

    step = HandlerExecutionStep(slow_handler, timeout=0.01)

    event = MagicMock()
    event.id = uuid4()
    context = MagicMock(spec=ProcessingContext)
    context.repo = MagicMock()

    # Act
    await step.execute(event, context)

    # Assert
    context.mark_failed.assert_called_once()
    args = context.mark_failed.call_args[0]
    kwargs = context.mark_failed.call_args[1]
    assert "timed out" in args[1]
    assert kwargs["status"] == "failed"


@pytest.mark.asyncio
async def test__handler_execution_step__handler_raises_exception__marks_failed_with_exc_type() -> None:
    # Arrange
    handler = AsyncMock(side_effect=RuntimeError("crash"))
    step = HandlerExecutionStep(handler)

    event = MagicMock()
    event.id = uuid4()
    context = MagicMock(spec=ProcessingContext)
    context.repo = MagicMock()

    # Act
    await step.execute(event, context)

    # Assert
    context.mark_failed.assert_called_once()
    args = context.mark_failed.call_args[0]
    kwargs = context.mark_failed.call_args[1]
    assert "RuntimeError: crash" in args[1]
    assert kwargs["status"] == "failed"


@pytest.mark.asyncio
async def test__handler_execution_step__handler_returns_none__marks_completed() -> None:
    # Arrange
    handler = AsyncMock(return_value=None)
    step = HandlerExecutionStep(handler)

    event = MagicMock()
    event.id = uuid4()
    context = MagicMock(spec=ProcessingContext)
    context.repo = MagicMock()

    # Act
    await step.execute(event, context)

    # Assert
    context.mark_completed.assert_called_once_with(event.id, status=EventHandlerStatus.COMPLETED)
