"""Unit tests for ``omni_box.core.pipeline.step``."""

from __future__ import annotations

from uuid import uuid4

import pytest

from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.step import (
    BaseProcessingStep,
    BatchHooks,
    EventHooks,
    ProcessingStep,
    StepResult,
)
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


def test__step_result__next__returns_continue_flags() -> None:
    # Act
    result = StepResult.next()

    # Assert
    assert result.should_skip_event is False
    assert result.should_stop_pipeline is False


def test__step_result__skip__sets_skip_flag_only() -> None:
    # Act
    result = StepResult.skip()

    # Assert
    assert result.should_skip_event is True
    assert result.should_stop_pipeline is False


def test__step_result__stop__sets_stop_flag_only() -> None:
    # Act
    result = StepResult.stop()

    # Assert
    assert result.should_skip_event is False
    assert result.should_stop_pipeline is True


def test__step_result__instance__is_frozen() -> None:
    # Arrange
    result = StepResult.next()

    # Act / Assert
    with pytest.raises((AttributeError, Exception)):
        result.should_skip_event = True  # type: ignore[misc]


async def test__base_processing_step__execute__returns_next_result() -> None:
    # Arrange
    step: BaseProcessingStep = BaseProcessingStep()
    ctx: ProcessingContext = ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]
    event = create_fake_event(id=uuid4())

    # Act
    result = await step.execute(event, ctx)

    # Assert
    assert result == StepResult.next()


async def test__base_processing_step__lifecycle_hooks__are_noops() -> None:
    # Arrange
    step: BaseProcessingStep = BaseProcessingStep()
    ctx: ProcessingContext = ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]
    event = create_fake_event(id=uuid4())

    # Act / Assert (no exceptions, no state changes)
    await step.on_batch_start(ctx)
    await step.on_batch_end(ctx)
    await step.on_event_start(event, ctx)
    await step.on_event_end(event, ctx)


def test__base_processing_step__protocol_membership__matches_runtime_protocols() -> None:
    # Arrange
    step: BaseProcessingStep = BaseProcessingStep()

    # Act / Assert: BaseProcessingStep implements all three protocols structurally
    assert isinstance(step, ProcessingStep)
    assert isinstance(step, BatchHooks)
    assert isinstance(step, EventHooks)
