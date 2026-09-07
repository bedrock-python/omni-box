"""Unit tests for ``omni_box.core.pipeline.steps.publisher``."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from omni_box.core.constants import DEFAULT_TRANSIENT_RETRY_DELAY_SECONDS
from omni_box.core.exceptions import TransientError
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps import HandlerExecutionStep, PublisherExecutionStep
from omni_box.core.services.results import EventHandlerStatus
from omni_box.utils import utc_now
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


class _Broker:
    """Publisher fake that raises what it is told to for the topics it is given."""

    def __init__(self, failing: dict[str, BaseException] | None = None) -> None:
        self.failing = failing or {}
        self.published: list[OutboxEvent] = []

    async def publish(self, event: OutboxEvent, repo: object) -> None:
        if event.topic in self.failing:
            raise self.failing[event.topic]
        self.published.append(event)


@pytest.fixture
def context() -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]


def test__publisher_step__constructed__is_a_handler_execution_step() -> None:
    # Arrange / Act
    step: PublisherExecutionStep[OutboxEvent] = PublisherExecutionStep(_Broker().publish)

    # Assert
    assert isinstance(step, HandlerExecutionStep)


async def test__publisher_step__publish_succeeds__marks_completed(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    broker = _Broker()
    step: PublisherExecutionStep[OutboxEvent] = PublisherExecutionStep(broker.publish)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert context.completed_ids == [event.id]
    assert broker.published == [event]


async def test__publisher_step__transient_error__marks_failed_noncounted_one_tick_ahead(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    broker = _Broker(failing={"topic.down": TransientError("Kafka broker unreachable: NodeNotReadyError")})
    step: PublisherExecutionStep[OutboxEvent] = PublisherExecutionStep(broker.publish)
    event = create_fake_event(topic="topic.down")
    before = utc_now()

    # Act
    await step.execute(event, context)

    # Assert
    assert context.failed_counted == []
    failure = context.failed_noncounted[0]
    assert failure.event_id == event.id
    assert failure.error == "Kafka broker unreachable: NodeNotReadyError"
    assert failure.next_retry_at is not None
    assert before < failure.next_retry_at <= utc_now() + timedelta(seconds=DEFAULT_TRANSIENT_RETRY_DELAY_SECONDS)
    assert context.statuses[event.id] == EventHandlerStatus.RETRY


async def test__publisher_step__publish_times_out__marks_failed_noncounted(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    async def hanging_publish(event: OutboxEvent, repo: object) -> None:
        await asyncio.Future()

    step: PublisherExecutionStep[OutboxEvent] = PublisherExecutionStep(hanging_publish, timeout=0.01)
    event = create_fake_event()

    # Act
    await step.execute(event, context)

    # Assert
    assert context.failed_counted == []
    failure = context.failed_noncounted[0]
    assert failure.error == "Publish timed out after 0.01s"
    assert failure.next_retry_at is not None


async def test__publisher_step__transient_error__defers_rest_of_batch_without_calling_publisher(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    broker = _Broker(failing={"topic.down": TransientError("Kafka broker unreachable: RequestTimedOutError")})
    step: PublisherExecutionStep[OutboxEvent] = PublisherExecutionStep(broker.publish)
    first, second, third = create_fake_event(topic="topic.down"), create_fake_event(), create_fake_event()

    # Act
    for event in (first, second, third):
        await step.execute(event, context)

    # Assert
    assert broker.published == []
    assert context.completed_ids == []
    assert context.failed_counted == []
    assert [f.event_id for f in context.failed_noncounted] == [first.id, second.id, third.id]
    probed, *deferred = context.failed_noncounted
    assert probed.next_retry_at is not None
    for failure in deferred:
        assert failure.error == probed.error
        assert failure.next_retry_at is None  # the schedule of a row we never touched stays as it is
        assert context.statuses[failure.event_id] == EventHandlerStatus.RETRY


async def test__publisher_step__next_batch__publishes_again_after_a_deferred_one(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    broker = _Broker(failing={"topic.down": TransientError("Kafka broker unreachable")})
    step: PublisherExecutionStep[OutboxEvent] = PublisherExecutionStep(broker.publish)
    await step.execute(create_fake_event(topic="topic.down"), context)
    next_context: ProcessingContext[OutboxEvent] = ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]
    event = create_fake_event()

    # Act
    await step.execute(event, next_context)

    # Assert
    assert broker.published == [event]
    assert next_context.completed_ids == [event.id]


async def test__publisher_step__broker_rejects_the_event__counts_and_the_batch_goes_on(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    broker = _Broker(failing={"topic.bad": ValueError("payload rejected")})
    step: PublisherExecutionStep[OutboxEvent] = PublisherExecutionStep(broker.publish)
    bad, good = create_fake_event(topic="topic.bad"), create_fake_event()

    # Act
    await step.execute(bad, context)
    await step.execute(good, context)

    # Assert
    assert [f.event_id for f in context.failed_counted] == [bad.id]
    assert context.failed_counted[0].error == "ValueError: payload rejected"
    assert context.failed_noncounted == []
    assert context.completed_ids == [good.id]
    assert broker.published == [good]
