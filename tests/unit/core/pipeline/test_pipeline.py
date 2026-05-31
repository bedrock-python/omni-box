"""Unit tests for ``omni_box.core.pipeline.pipeline``."""

from __future__ import annotations

from uuid import uuid4

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.exceptions import PipelineStoppedError
from omni_box.core.pipeline.pipeline import ProcessingPipeline
from omni_box.core.pipeline.step import StepResult
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


class _RecordingStep:
    """Step that records calls and returns a configurable result."""

    def __init__(self, result: StepResult | None = None) -> None:
        self._result = result or StepResult.next()
        self.execute_events: list[OutboxEvent] = []

    async def execute(self, event: OutboxEvent, context: ProcessingContext[OutboxEvent]) -> StepResult:
        self.execute_events.append(event)
        return self._result


class _BatchHookedStep(_RecordingStep):
    def __init__(self, result: StepResult | None = None) -> None:
        super().__init__(result)
        self.batch_start_count = 0
        self.batch_end_count = 0

    async def on_batch_start(self, context: ProcessingContext[OutboxEvent]) -> None:
        self.batch_start_count += 1

    async def on_batch_end(self, context: ProcessingContext[OutboxEvent]) -> None:
        self.batch_end_count += 1


class _EventHookedStep(_RecordingStep):
    def __init__(self, result: StepResult | None = None) -> None:
        super().__init__(result)
        self.event_start_calls: list[OutboxEvent] = []
        self.event_end_calls: list[OutboxEvent] = []

    async def on_event_start(self, event: OutboxEvent, context: ProcessingContext[OutboxEvent]) -> None:
        self.event_start_calls.append(event)

    async def on_event_end(self, event: OutboxEvent, context: ProcessingContext[OutboxEvent]) -> None:
        self.event_end_calls.append(event)


@pytest.fixture
def context() -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]


def test__pipeline__init_no_steps__has_empty_step_list() -> None:
    # Arrange / Act
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline()

    # Assert
    assert pipeline._steps == []


def test__pipeline__init_with_steps__preserves_order() -> None:
    # Arrange
    step_a = _RecordingStep()
    step_b = _RecordingStep()

    # Act
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step_a, step_b])  # type: ignore[list-item]

    # Assert
    assert pipeline._steps == [step_a, step_b]


def test__pipeline__add_step__appends_at_end() -> None:
    # Arrange
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline()
    step = _RecordingStep()

    # Act
    pipeline.add_step(step)  # type: ignore[arg-type]

    # Assert
    assert pipeline._steps == [step]


async def test__process_event__all_steps_continue__executes_in_order(context: ProcessingContext[OutboxEvent]) -> None:
    # Arrange
    step1 = _RecordingStep()
    step2 = _RecordingStep()
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step1, step2])  # type: ignore[list-item]
    event = create_fake_event()

    # Act
    await pipeline.process_event(event, context)

    # Assert
    assert step1.execute_events == [event]
    assert step2.execute_events == [event]


async def test__process_event__first_step_skips__second_step_not_called(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step1 = _RecordingStep(result=StepResult.skip())
    step2 = _RecordingStep()
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step1, step2])  # type: ignore[list-item]
    event = create_fake_event()

    # Act
    await pipeline.process_event(event, context)

    # Assert
    assert step1.execute_events == [event]
    assert step2.execute_events == []


async def test__process_event__step_stops_pipeline__raises_stopped_error(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step = _RecordingStep(result=StepResult.stop())
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step])  # type: ignore[list-item]
    event = create_fake_event()

    # Act / Assert
    with pytest.raises(PipelineStoppedError):
        await pipeline.process_event(event, context)


async def test__process_event__event_hooks__start_and_end_called_for_all_events(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step = _EventHookedStep()
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step])  # type: ignore[list-item]
    event = create_fake_event()

    # Act
    await pipeline.process_event(event, context)

    # Assert
    assert step.event_start_calls == [event]
    assert step.event_end_calls == [event]


async def test__process_event__step_raises__event_end_hooks_still_called(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step = _EventHookedStep(result=StepResult.stop())
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step])  # type: ignore[list-item]
    event = create_fake_event()

    # Act / Assert
    with pytest.raises(PipelineStoppedError):
        await pipeline.process_event(event, context)

    # Hook must still be called via finally
    assert step.event_end_calls == [event]


async def test__process_batch__batch_hooks__called_once_each(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step = _BatchHookedStep()
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step])  # type: ignore[list-item]
    events = [create_fake_event(), create_fake_event()]

    # Act
    await pipeline.process_batch(events, context)

    # Assert
    assert step.batch_start_count == 1
    assert step.batch_end_count == 1
    assert step.execute_events == events


async def test__process_batch__pre_skipped_event__handler_not_invoked(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step = _RecordingStep()
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step])  # type: ignore[list-item]
    skipped_event = create_fake_event(id=uuid4())
    other_event = create_fake_event(id=uuid4())
    context.mark_skipped(skipped_event.id, reason="pre-skipped")

    # Act
    await pipeline.process_batch([skipped_event, other_event], context)

    # Assert: only ``other_event`` got executed
    assert step.execute_events == [other_event]


async def test__process_batch__step_signals_stop__remaining_events_skipped_and_end_hooks_run(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step = _BatchHookedStep(result=StepResult.stop())
    pipeline: ProcessingPipeline[OutboxEvent] = ProcessingPipeline([step])  # type: ignore[list-item]
    events = [create_fake_event(), create_fake_event()]

    # Act
    await pipeline.process_batch(events, context)

    # Assert: stop after first event; batch_end still called
    assert step.execute_events == events[:1]
    assert step.batch_end_count == 1
