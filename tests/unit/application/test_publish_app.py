"""Unit tests for ``omni_box.application.services.publish.OutboxPublisher``."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from omni_box.application.services.publish import OutboxPublisher
from omni_box.core.constants import DEFAULT_PUBLISH_TIMEOUT_SECONDS
from omni_box.core.exceptions import StorageError
from omni_box.core.pipeline.steps import HandlerExecutionStep
from omni_box.core.services.metrics import NoOpOutboxMetrics
from omni_box.core.services.results import BatchProcessingResult
from tests.helpers import FakeEventPublisher, FakeOutboxStore, create_fake_event

pytestmark = pytest.mark.unit


# -------- helpers --------


class _RecordingProcessor:
    """Minimal stand-in for the internal pipeline-based processor."""

    def __init__(self, result: BatchProcessingResult | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._result: BatchProcessingResult = result or BatchProcessingResult()

    async def process_batch(self, **kwargs: Any) -> BatchProcessingResult:
        self.calls.append(kwargs)
        return self._result


# -------- construction --------


def test__outbox_publisher__no_metrics__defaults_to_noop_metrics() -> None:
    # Arrange
    repo = FakeOutboxStore()
    broker = FakeEventPublisher()

    # Act
    publisher = OutboxPublisher(repo, broker)

    # Assert
    assert isinstance(publisher._metrics, NoOpOutboxMetrics)


def _handler_step(publisher: OutboxPublisher) -> Any:
    return next(s for s in publisher._processor._pipeline._steps if isinstance(s, HandlerExecutionStep))


def test__outbox_publisher__defaults__uses_default_publish_timeout() -> None:
    # Arrange
    repo = FakeOutboxStore()
    broker = FakeEventPublisher()

    # Act
    publisher = OutboxPublisher(repo, broker)

    # Assert
    assert _handler_step(publisher)._timeout == DEFAULT_PUBLISH_TIMEOUT_SECONDS


def test__outbox_publisher__custom_publish_timeout__propagates_to_handler_step() -> None:
    # Arrange
    repo = FakeOutboxStore()
    broker = FakeEventPublisher()

    # Act
    publisher = OutboxPublisher(repo, broker, publish_timeout=4.25)

    # Assert
    assert _handler_step(publisher)._timeout == 4.25


def test__outbox_publisher__job_name__is_outbox_publisher() -> None:
    # Arrange / Act
    publisher = OutboxPublisher(FakeOutboxStore(), FakeEventPublisher())

    # Assert
    assert publisher._processor._job_name == "outbox_publisher"


# -------- publish_batch delegation --------


async def test__outbox_publisher__publish_batch_no_filters__delegates_to_processor() -> None:
    # Arrange
    publisher = OutboxPublisher(FakeOutboxStore(), FakeEventPublisher())
    proc = _RecordingProcessor()
    publisher._processor = proc  # type: ignore[assignment]

    # Act
    result = await publisher.publish_batch(worker_id="w1", batch_size=10)

    # Assert
    assert isinstance(result, BatchProcessingResult)
    assert proc.calls == [{"worker_id": "w1", "batch_size": 10, "shutdown_requested_func": None}]


async def test__outbox_publisher__publish_batch_with_filters__forwards_fetch_filters() -> None:
    # Arrange
    publisher = OutboxPublisher(FakeOutboxStore(), FakeEventPublisher())
    proc = _RecordingProcessor()
    publisher._processor = proc  # type: ignore[assignment]

    def shutdown() -> bool:
        return False

    # Act
    await publisher.publish_batch(worker_id="w2", batch_size=5, shutdown_requested_func=shutdown, topic="t1")

    # Assert
    assert proc.calls == [
        {
            "worker_id": "w2",
            "batch_size": 5,
            "shutdown_requested_func": shutdown,
            "topic": "t1",
        }
    ]


async def test__outbox_publisher__concurrency_limit_set__uses_semaphore_path() -> None:
    # Arrange
    publisher = OutboxPublisher(FakeOutboxStore(), FakeEventPublisher(), concurrency_limit=1)
    proc = _RecordingProcessor()
    publisher._processor = proc  # type: ignore[assignment]
    assert publisher._semaphore is not None

    # Act
    await publisher.publish_batch(worker_id="w1", batch_size=3)

    # Assert
    assert proc.calls == [{"worker_id": "w1", "batch_size": 3, "shutdown_requested_func": None}]


async def test__outbox_publisher__no_concurrency_limit__semaphore_is_none() -> None:
    # Arrange
    publisher = OutboxPublisher(FakeOutboxStore(), FakeEventPublisher())

    # Assert
    assert publisher._semaphore is None


# -------- end-to-end with fakes --------


async def test__outbox_publisher__pending_events__publishes_all_and_marks_completed() -> None:
    # Arrange
    event1 = create_fake_event(id=uuid4(), topic="t1")
    event2 = create_fake_event(id=uuid4(), topic="t2")
    outbox = FakeOutboxStore(pending_events=[event1, event2])
    broker = FakeEventPublisher()
    publisher = OutboxPublisher(outbox, broker)

    # Act
    result = await publisher.publish_batch(worker_id="w1", batch_size=100)

    # Assert
    assert isinstance(result, BatchProcessingResult)
    assert [e.id for e in broker.published_events] == [event1.id, event2.id]
    assert outbox.published_ids == [event1.id, event2.id]
    assert outbox.fetch_calls == [(100, "w1", 300)]


async def test__outbox_publisher__broker_failure__marks_event_failed() -> None:
    # Arrange
    event = create_fake_event(id=uuid4(), topic="topic.fail")
    outbox = FakeOutboxStore(pending_events=[event])
    broker = FakeEventPublisher(fail_for_topic="topic.fail")
    publisher = OutboxPublisher(outbox, broker)

    # Act
    await publisher.publish_batch(worker_id="w1", batch_size=10)

    # Assert
    assert outbox.published_ids == []
    assert len(outbox.failed_calls) == 1
    failed_event_id, error_msg, _ = outbox.failed_calls[0]
    assert failed_event_id == event.id
    assert "publish failed" in error_msg


async def test__outbox_publisher__bulk_mark_completed_storage_error__events_still_published() -> None:
    # Arrange
    event = create_fake_event(id=uuid4(), topic="topic.ok")
    outbox = FakeOutboxStore(pending_events=[event])
    broker = FakeEventPublisher()
    publisher = OutboxPublisher(outbox, broker)

    # Act
    with (
        patch("omni_box.core.services.processor.logger", MagicMock()),
        patch.object(outbox, "bulk_mark_completed", side_effect=StorageError("db error")),
    ):
        await publisher.publish_batch(worker_id="w1", batch_size=10)

    # Assert
    assert len(broker.published_events) == 1


async def test__outbox_publisher__fetch_raises__top_level_exception_propagates() -> None:
    # Arrange
    outbox = FakeOutboxStore()
    broker = FakeEventPublisher()
    publisher = OutboxPublisher(outbox, broker)

    # Act / Assert
    with (
        patch("omni_box.core.services.processor.logger", MagicMock()),
        patch.object(outbox, "fetch_and_lock_pending", side_effect=RuntimeError("fetch error")),
        pytest.raises(RuntimeError, match="fetch error"),
    ):
        await publisher.publish_batch(worker_id="w1", batch_size=10)
