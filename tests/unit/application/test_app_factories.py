"""Unit tests for ``omni_box.application.factories``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest

from omni_box.application.factories import (
    create_dispatching_processor,
    create_inbox_processor,
    create_outbox_processor,
)
from omni_box.core.constants import (
    DEFAULT_PROCESS_TIMEOUT_SECONDS,
    DEFAULT_PUBLISH_TIMEOUT_SECONDS,
)
from omni_box.core.dispatch.registry import EventRouter
from omni_box.core.models.entities import InboxEvent, OutboxEvent
from omni_box.core.pipeline.steps import (
    CircuitBreakerStep,
    DLQStep,
    HandlerExecutionStep,
    MetricsStep,
    OpenTelemetryStep,
    SiblingDeduplicationStep,
)
from omni_box.core.pipeline.strategies.fetch import FilteredFetchStrategy
from omni_box.core.protocols import (
    EventPublisher,
    InboxEventRepository,
    OutboxEventRepository,
)
from omni_box.core.protocols.metrics import InboxMetrics, OutboxMetrics
from omni_box.core.services.results import handler_completed

if TYPE_CHECKING:
    from omni_box.core.pipeline.step import ProcessingStep
    from omni_box.core.services.results import EventHandlerResult

pytestmark = pytest.mark.unit


# -------- helpers / fakes --------


class _RecordingStep:
    """Minimal in-memory ``ProcessingStep`` used to verify ordering / passthrough."""

    def __init__(self, label: str) -> None:
        self.label = label

    async def on_event_start(self, event: object, context: object) -> object:
        return None

    async def on_event_end(self, event: object, context: object) -> None:
        return None


class _DLQ:
    """In-memory DLQ storage stub satisfying ``DLQStorage`` protocol."""

    def __init__(self) -> None:
        self.moved: list[tuple[object, str]] = []

    async def move_to_dlq(self, event: object, error: str) -> None:
        self.moved.append((event, error))


@pytest.fixture
def inbox_repo() -> MagicMock:
    return MagicMock(spec=InboxEventRepository)


@pytest.fixture
def outbox_repo() -> MagicMock:
    return MagicMock(spec=OutboxEventRepository)


@pytest.fixture
def inbox_handler() -> Callable[..., Awaitable[EventHandlerResult | None]]:
    return AsyncMock(return_value=handler_completed())


@pytest.fixture
def publisher() -> MagicMock:
    return MagicMock(spec=EventPublisher)


@pytest.fixture
def inbox_metrics() -> MagicMock:
    return MagicMock(spec=InboxMetrics)


@pytest.fixture
def outbox_metrics() -> MagicMock:
    return MagicMock(spec=OutboxMetrics)


# -------- create_inbox_processor --------


def test__create_inbox_processor__defaults__has_dedup_and_handler_steps(
    inbox_repo: MagicMock,
    inbox_handler: Callable[..., Awaitable[EventHandlerResult | None]],
) -> None:
    # Arrange / Act
    processor = create_inbox_processor(repo=inbox_repo, handler=inbox_handler)

    # Assert
    steps = processor._pipeline._steps
    assert [type(s) for s in steps] == [SiblingDeduplicationStep, HandlerExecutionStep]
    assert processor._job_name == "inbox_processor"
    assert processor._metrics is None


def test__create_inbox_processor__skip_duplicate_siblings_false__omits_dedup_step(
    inbox_repo: MagicMock,
    inbox_handler: Callable[..., Awaitable[EventHandlerResult | None]],
) -> None:
    # Arrange / Act
    processor = create_inbox_processor(repo=inbox_repo, handler=inbox_handler, skip_duplicate_siblings=False)

    # Assert
    assert [type(s) for s in processor._pipeline._steps] == [HandlerExecutionStep]


def test__create_inbox_processor__filter_sources_provided__uses_filtered_fetch_strategy(
    inbox_repo: MagicMock,
    inbox_handler: Callable[..., Awaitable[EventHandlerResult | None]],
) -> None:
    # Arrange / Act
    processor = create_inbox_processor(repo=inbox_repo, handler=inbox_handler, filter_sources=["src1", "src2"])

    # Assert
    assert isinstance(processor._fetch_strategy, FilteredFetchStrategy)


def test__create_inbox_processor__all_optional_features_enabled__builds_full_pipeline(
    inbox_repo: MagicMock,
    inbox_handler: Callable[..., Awaitable[EventHandlerResult | None]],
    inbox_metrics: MagicMock,
) -> None:
    # Arrange
    dlq: _DLQ = _DLQ()
    before: list[ProcessingStep[InboxEvent]] = [_RecordingStep("before")]  # type: ignore[list-item]
    after: list[ProcessingStep[InboxEvent]] = [_RecordingStep("after")]  # type: ignore[list-item]

    # Act
    processor = create_inbox_processor(
        repo=inbox_repo,
        handler=inbox_handler,
        metrics=inbox_metrics,
        dlq_storage=dlq,  # type: ignore[arg-type]
        enable_otel=True,
        enable_circuit_breaker=True,
        circuit_breaker_failure_threshold=7,
        circuit_breaker_recovery_timeout=42,
        job_name="custom_inbox",
        additional_steps_before=before,
        additional_steps_after=after,
    )

    # Assert
    types = [type(s) for s in processor._pipeline._steps]
    assert types == [
        OpenTelemetryStep,
        CircuitBreakerStep,
        DLQStep,
        _RecordingStep,
        SiblingDeduplicationStep,
        HandlerExecutionStep,
        _RecordingStep,
        MetricsStep,
    ]
    assert processor._job_name == "custom_inbox"
    assert processor._metrics is inbox_metrics


def test__create_inbox_processor__handler_step__uses_provided_process_timeout(
    inbox_repo: MagicMock,
    inbox_handler: Callable[..., Awaitable[EventHandlerResult | None]],
) -> None:
    # Arrange / Act
    processor = create_inbox_processor(repo=inbox_repo, handler=inbox_handler, process_timeout=11.5)

    # Assert
    handler_step = next(s for s in processor._pipeline._steps if isinstance(s, HandlerExecutionStep))
    assert handler_step._timeout == 11.5


def test__create_inbox_processor__handler_step__defaults_to_module_default_timeout(
    inbox_repo: MagicMock,
    inbox_handler: Callable[..., Awaitable[EventHandlerResult | None]],
) -> None:
    # Arrange / Act
    processor = create_inbox_processor(repo=inbox_repo, handler=inbox_handler)

    # Assert
    handler_step = next(s for s in processor._pipeline._steps if isinstance(s, HandlerExecutionStep))
    assert handler_step._timeout == DEFAULT_PROCESS_TIMEOUT_SECONDS


# -------- create_outbox_processor --------


def test__create_outbox_processor__defaults__has_only_handler_step(
    outbox_repo: MagicMock, publisher: MagicMock
) -> None:
    # Arrange / Act
    processor = create_outbox_processor(repo=outbox_repo, publisher=publisher)

    # Assert
    assert [type(s) for s in processor._pipeline._steps] == [HandlerExecutionStep]
    assert processor._job_name == "outbox_processor"
    assert processor._metrics is None


def test__create_outbox_processor__all_optional_features_enabled__builds_full_pipeline(
    outbox_repo: MagicMock, publisher: MagicMock, outbox_metrics: MagicMock
) -> None:
    # Arrange
    dlq: _DLQ = _DLQ()
    before: list[ProcessingStep[OutboxEvent]] = [_RecordingStep("before")]  # type: ignore[list-item]
    after: list[ProcessingStep[OutboxEvent]] = [_RecordingStep("after")]  # type: ignore[list-item]

    # Act
    processor = create_outbox_processor(
        repo=outbox_repo,
        publisher=publisher,
        metrics=outbox_metrics,
        dlq_storage=dlq,  # type: ignore[arg-type]
        enable_otel=True,
        enable_circuit_breaker=True,
        circuit_breaker_failure_threshold=3,
        circuit_breaker_recovery_timeout=15,
        job_name="custom_outbox",
        additional_steps_before=before,
        additional_steps_after=after,
        publish_timeout=2.5,
    )

    # Assert
    types = [type(s) for s in processor._pipeline._steps]
    assert types == [
        OpenTelemetryStep,
        CircuitBreakerStep,
        DLQStep,
        _RecordingStep,
        HandlerExecutionStep,
        _RecordingStep,
        MetricsStep,
    ]
    assert processor._job_name == "custom_outbox"
    assert processor._metrics is outbox_metrics

    handler_step = next(s for s in processor._pipeline._steps if isinstance(s, HandlerExecutionStep))
    assert handler_step._timeout == 2.5


def test__create_outbox_processor__handler_step__defaults_to_module_default_timeout(
    outbox_repo: MagicMock, publisher: MagicMock
) -> None:
    # Arrange / Act
    processor = create_outbox_processor(repo=outbox_repo, publisher=publisher)

    # Assert
    handler_step = next(s for s in processor._pipeline._steps if isinstance(s, HandlerExecutionStep))
    assert handler_step._timeout == DEFAULT_PUBLISH_TIMEOUT_SECONDS


# -------- create_dispatching_processor --------


def test__create_dispatching_processor__defaults__has_dedup_and_handler_steps(
    inbox_repo: MagicMock,
) -> None:
    # Arrange
    router = MagicMock(spec=EventRouter)
    router.dispatch = AsyncMock(return_value=handler_completed())

    # Act
    processor = create_dispatching_processor(repo=inbox_repo, router=router)

    # Assert
    assert [type(s) for s in processor._pipeline._steps] == [
        SiblingDeduplicationStep,
        HandlerExecutionStep,
    ]
    assert processor._job_name == "dispatching_processor"


def test__create_dispatching_processor__filter_sources_provided__uses_filtered_fetch_strategy(
    inbox_repo: MagicMock,
) -> None:
    # Arrange
    router = MagicMock(spec=EventRouter)

    # Act
    processor = create_dispatching_processor(repo=inbox_repo, router=router, filter_sources=["s1"])

    # Assert
    assert isinstance(processor._fetch_strategy, FilteredFetchStrategy)


def test__create_dispatching_processor__skip_duplicate_siblings_false__omits_dedup_step(
    inbox_repo: MagicMock,
) -> None:
    # Arrange
    router = MagicMock(spec=EventRouter)

    # Act
    processor = create_dispatching_processor(repo=inbox_repo, router=router, skip_duplicate_siblings=False)

    # Assert
    assert [type(s) for s in processor._pipeline._steps] == [HandlerExecutionStep]


async def test__create_dispatching_processor__handler_invoked__delegates_to_router_dispatch(
    inbox_repo: MagicMock,
) -> None:
    # Arrange
    router = MagicMock(spec=EventRouter)
    router.dispatch = AsyncMock(return_value=handler_completed())
    deps: dict[str, object] = {"dep_key": "dep_val"}
    processor = create_dispatching_processor(repo=inbox_repo, router=router, dependencies=deps)
    handler_step = next(s for s in processor._pipeline._steps if isinstance(s, HandlerExecutionStep))
    event = InboxEvent(message_id="m1", consumer_group="cg1", source="src1", event_type="ev1", payload={"p": 1})

    # Act
    await handler_step._handler(event, inbox_repo)  # type: ignore[attr-defined]

    # Assert
    router.dispatch.assert_called_once_with(event, "src1", inbox_repo, dep_key="dep_val")


async def test__create_dispatching_processor__no_dependencies__dispatches_without_kwargs(
    inbox_repo: MagicMock,
) -> None:
    # Arrange
    router = MagicMock(spec=EventRouter)
    router.dispatch = AsyncMock(return_value=handler_completed())
    processor = create_dispatching_processor(repo=inbox_repo, router=router)
    handler_step = next(s for s in processor._pipeline._steps if isinstance(s, HandlerExecutionStep))
    event = InboxEvent(message_id="m1", consumer_group="cg1", source="src9", event_type="ev1", payload={"p": 1})

    # Act
    await handler_step._handler(event, inbox_repo)  # type: ignore[attr-defined]

    # Assert
    router.dispatch.assert_called_once_with(event, "src9", inbox_repo)


def test__create_dispatching_processor__all_optional_features_enabled__builds_full_pipeline(
    inbox_repo: MagicMock, inbox_metrics: MagicMock
) -> None:
    # Arrange
    router = MagicMock(spec=EventRouter)
    dlq: _DLQ = _DLQ()
    before: list[ProcessingStep[InboxEvent]] = [_RecordingStep("before")]  # type: ignore[list-item]
    after: list[ProcessingStep[InboxEvent]] = [_RecordingStep("after")]  # type: ignore[list-item]

    # Act
    processor = create_dispatching_processor(
        repo=inbox_repo,
        router=router,
        metrics=inbox_metrics,
        dlq_storage=dlq,  # type: ignore[arg-type]
        enable_otel=True,
        enable_circuit_breaker=True,
        filter_sources=["s1"],
        process_timeout=4.4,
        job_name="custom_dispatch",
        additional_steps_before=before,
        additional_steps_after=after,
    )

    # Assert
    types = [type(s) for s in processor._pipeline._steps]
    assert types == [
        OpenTelemetryStep,
        CircuitBreakerStep,
        DLQStep,
        _RecordingStep,
        SiblingDeduplicationStep,
        HandlerExecutionStep,
        _RecordingStep,
        MetricsStep,
    ]
    assert processor._job_name == "custom_dispatch"
    assert processor._metrics is inbox_metrics
    assert isinstance(processor._fetch_strategy, FilteredFetchStrategy)
    handler_step = next(s for s in processor._pipeline._steps if isinstance(s, HandlerExecutionStep))
    assert handler_step._timeout == 4.4
