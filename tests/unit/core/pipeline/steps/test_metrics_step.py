"""Unit tests for ``omni_box.core.pipeline.steps.metrics``."""

from __future__ import annotations

from uuid import uuid4

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.metrics import MetricsStep
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


class _MetricsRecorder:
    """Records every call to the metrics protocol."""

    def __init__(self) -> None:
        self.processed: list[tuple[int, str | None, str | None]] = []
        self.failed: list[tuple[int, str | None, str | None]] = []
        self.duplicate: list[tuple[int, str | None, str | None]] = []
        self.durations: list[tuple[float, str | None]] = []

    def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self.processed.append((count, event_type, status))

    def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self.failed.append((count, event_type, status))

    def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self.duplicate.append((count, event_type, status))

    def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
        self.durations.append((seconds, event_type))


@pytest.fixture
def context() -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]


async def test__metrics_step__execute__starts_tracking_event(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    metrics = _MetricsRecorder()
    step: MetricsStep[OutboxEvent] = MetricsStep(metrics)
    event = create_fake_event()

    # Act
    result = await step.execute(event, context)

    # Assert
    assert result.should_skip_event is False
    assert event.id in step._start_times
    assert step._event_types[event.id] == event.event_type


async def test__metrics_step__no_metrics_collector__on_batch_end_clears_caches(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step: MetricsStep[OutboxEvent] = MetricsStep(None)
    event = create_fake_event()
    await step.execute(event, context)
    context.mark_completed(event.id)

    # Act (must not raise)
    await step.on_batch_end(context)

    # Assert: per-batch caches are always released to avoid cross-batch leaks
    assert step._start_times == {}
    assert step._event_types == {}


async def test__metrics_step__batch_end__emits_metric_per_outcome(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    metrics = _MetricsRecorder()
    step: MetricsStep[OutboxEvent] = MetricsStep(metrics)

    completed = create_fake_event(id=uuid4(), event_type="user.created")
    failed_c = create_fake_event(id=uuid4(), event_type="user.updated")
    failed_nc = create_fake_event(id=uuid4(), event_type="user.deleted")
    skipped = create_fake_event(id=uuid4(), event_type="user.duplicate")

    for ev in [completed, failed_c, failed_nc, skipped]:
        await step.execute(ev, context)

    context.mark_completed(completed.id, status="completed")
    context.mark_failed(failed_c.id, "boom", count_as_attempt=True, status="failed")
    context.mark_failed(failed_nc.id, "retry", count_as_attempt=False, status="retry")
    context.mark_skipped(skipped.id, reason="dup", status="skipped")

    # Act
    await step.on_batch_end(context)

    # Assert
    assert metrics.processed == [(1, "user.created", "completed")]
    assert (1, "user.updated", "failed") in metrics.failed
    assert (1, "user.deleted", "retry") in metrics.failed
    assert metrics.duplicate == [(1, "user.duplicate", "skipped")]
    assert len(metrics.durations) == 1
    assert metrics.durations[0][1] == "user.created"
    assert metrics.durations[0][0] >= 0.0
    assert step._start_times == {}
    assert step._event_types == {}


async def test__metrics_step__completed_without_start_time__skips_duration_observation(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange: completed event was never seen by execute() -> no start_time
    metrics = _MetricsRecorder()
    step: MetricsStep[OutboxEvent] = MetricsStep(metrics)
    event_id = uuid4()
    context.mark_completed(event_id, status="completed")

    # Act
    await step.on_batch_end(context)

    # Assert: still increments processed count, but no duration observed
    assert metrics.processed == [(1, None, "completed")]
    assert metrics.durations == []
