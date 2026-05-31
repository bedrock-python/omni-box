"""Unit tests for new pipeline steps."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.circuit_breaker import CircuitBreakerStep
from omni_box.core.pipeline.steps.dlq import DLQStep, DLQStorage
from omni_box.core.pipeline.steps.otel import HAS_OTEL, OpenTelemetryStep

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test__dlq_step__failure_at_max_attempts__moves_to_dlq() -> None:
    # Arrange
    event = MagicMock()
    event.id = uuid4()
    event.attempts_made = 2
    event.max_attempts = 3

    context = ProcessingContext(repo=MagicMock(), worker_id="w1")
    context.mark_failed(event.id, error="fatal", count_as_attempt=True)

    dlq_storage = AsyncMock(spec=DLQStorage)
    step = DLQStep(dlq_storage=dlq_storage)

    # Act
    await step.on_event_end(event, context)

    # Assert
    dlq_storage.move_to_dlq.assert_awaited_once_with(event, "fatal")


@pytest.mark.asyncio
async def test__dlq_step__failure_below_max_attempts__does_not_move_to_dlq() -> None:
    # Arrange
    event = MagicMock()
    event.id = uuid4()
    event.attempts_made = 1
    event.max_attempts = 3

    context = ProcessingContext(repo=MagicMock(), worker_id="w1")
    context.mark_failed(event.id, error="fatal", count_as_attempt=True)

    dlq_storage = AsyncMock(spec=DLQStorage)
    step = DLQStep(dlq_storage=dlq_storage)

    # Act
    await step.on_event_end(event, context)

    # Assert
    dlq_storage.move_to_dlq.assert_not_awaited()


@pytest.mark.asyncio
async def test__circuit_breaker_step__failures_reach_threshold__opens_circuit() -> None:
    # Arrange
    step: CircuitBreakerStep[Any] = CircuitBreakerStep(failure_threshold=2)
    event1 = MagicMock()
    event1.id = uuid4()
    event2 = MagicMock()
    event2.id = uuid4()

    context = ProcessingContext(repo=MagicMock(), worker_id="w1")

    # Act
    context.mark_failed(event1.id, error="f1")
    await step.on_event_end(event1, context)
    assert not step._is_open

    context.mark_failed(event2.id, error="f2")
    await step.on_event_end(event2, context)

    # Assert
    assert step._is_open

    result = await step.execute(MagicMock(), context)  # type: ignore[unreachable]
    assert result.should_stop_pipeline


@pytest.mark.asyncio
async def test__otel_step__event_lifecycle__span_created_and_removed() -> None:
    # Arrange
    if not HAS_OTEL:
        pytest.skip("OpenTelemetry not installed")

    step: OpenTelemetryStep[Any] = OpenTelemetryStep(service_name="test")
    event = MagicMock()
    event.id = uuid4()
    event.event_type = "test.event"

    context = ProcessingContext(repo=MagicMock(), worker_id="w1")

    # Act
    await step.on_event_start(event, context)

    # Assert
    assert str(event.id) in step._current_spans

    await step.on_event_end(event, context)
    assert str(event.id) not in step._current_spans
