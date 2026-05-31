"""Unit tests for ``omni_box.core.pipeline.steps.dlq``."""

from __future__ import annotations

from uuid import uuid4

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.dlq import DLQStep, DLQStorage
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


class _DLQFake(DLQStorage[OutboxEvent]):
    def __init__(self, raise_error: bool = False) -> None:
        self.calls: list[tuple[OutboxEvent, str]] = []
        self.raise_error = raise_error

    async def move_to_dlq(self, event: OutboxEvent, error: str) -> None:
        if self.raise_error:
            raise RuntimeError("dlq storage offline")
        self.calls.append((event, error))


@pytest.fixture
def context() -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]


async def test__dlq__execute__returns_next(context: ProcessingContext[OutboxEvent]) -> None:
    # Arrange
    storage = _DLQFake()
    step: DLQStep[OutboxEvent] = DLQStep(storage)
    event = create_fake_event()

    # Act
    result = await step.execute(event, context)

    # Assert
    assert result.should_skip_event is False
    assert result.should_stop_pipeline is False


async def test__dlq__no_failure_for_event__does_not_move_to_dlq(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    storage = _DLQFake()
    step: DLQStep[OutboxEvent] = DLQStep(storage)
    event = create_fake_event()

    # Act
    await step.on_event_end(event, context)

    # Assert
    assert storage.calls == []


async def test__dlq__failure_under_max_attempts__does_not_move(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    storage = _DLQFake()
    step: DLQStep[OutboxEvent] = DLQStep(storage)
    # attempts_made=0, +1=1, max_attempts=6 default => not at limit
    event = create_fake_event()
    context.mark_failed(event.id, "boom")

    # Act
    await step.on_event_end(event, context)

    # Assert
    assert storage.calls == []


async def test__dlq__failure_at_or_above_max_attempts__moves_to_dlq(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    storage = _DLQFake()
    step: DLQStep[OutboxEvent] = DLQStep(storage)
    event = OutboxEvent(
        id=uuid4(),
        aggregate_type="A",
        aggregate_id=uuid4(),
        event_type="x",
        topic="t",
        partition_key="p",
        payload={"k": 1},
        max_attempts=3,
        attempts_made=2,  # +1 = 3 => reaches max
    )
    context.mark_failed(event.id, "fatal")

    # Act
    await step.on_event_end(event, context)

    # Assert
    assert storage.calls == [(event, "fatal")]


async def test__dlq__non_counted_failure_at_max__does_not_move(
    context: ProcessingContext[OutboxEvent],
) -> None:
    """Transient failures (``count_as_attempt=False``) must never reach DLQ."""
    # Arrange
    storage = _DLQFake()
    step: DLQStep[OutboxEvent] = DLQStep(storage)
    event = OutboxEvent(
        id=uuid4(),
        aggregate_type="A",
        aggregate_id=uuid4(),
        event_type="x",
        topic="t",
        partition_key="p",
        payload={"k": 1},
        max_attempts=3,
        attempts_made=2,
    )
    # Even though projected attempt count (3) >= max_attempts (3), this is a
    # transient failure and must not be routed to DLQ.
    context.mark_failed(event.id, "transient", count_as_attempt=False)

    # Act
    await step.on_event_end(event, context)

    # Assert
    assert storage.calls == []


async def test__dlq__storage_raises__exception_is_swallowed(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    storage = _DLQFake(raise_error=True)
    step: DLQStep[OutboxEvent] = DLQStep(storage)
    event = OutboxEvent(
        id=uuid4(),
        aggregate_type="A",
        aggregate_id=uuid4(),
        event_type="x",
        topic="t",
        partition_key="p",
        payload={"k": 1},
        max_attempts=2,
        attempts_made=1,
    )
    context.mark_failed(event.id, "fatal")

    # Act (must not raise)
    await step.on_event_end(event, context)

    # Assert: storage was attempted but no successful call recorded
    assert storage.calls == []
