"""Unit tests for ``omni_box.core.pipeline.steps.otel``."""

from __future__ import annotations

import importlib
import sys
from typing import Any
from uuid import uuid4

import pytest
from opentelemetry.trace import StatusCode

from omni_box.core.models.entities import InboxEvent, OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps import otel as otel_module
from omni_box.core.pipeline.steps.otel import HAS_OTEL, OpenTelemetryStep
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


class _RecordingSpan:
    def __init__(self) -> None:
        self.attributes: dict[str, Any] = {}
        self.status: Any = None
        self.ended = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_status(self, status: Any) -> None:
        self.status = status

    def end(self) -> None:
        self.ended = True


class _RecordingTracer:
    def __init__(self) -> None:
        self.started: list[tuple[str, _RecordingSpan]] = []

    def start_span(self, name: str) -> _RecordingSpan:
        span = _RecordingSpan()
        self.started.append((name, span))
        return span


@pytest.fixture
def context() -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]


async def test__otel_step__execute__returns_next(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    if not HAS_OTEL:  # pragma: no cover
        pytest.skip("OpenTelemetry not installed")
    step: OpenTelemetryStep[OutboxEvent] = OpenTelemetryStep(service_name="svc")
    event = create_fake_event()

    # Act
    result = await step.execute(event, context)

    # Assert
    assert result.should_skip_event is False
    assert result.should_stop_pipeline is False


async def test__otel_step__no_tracer__on_event_start_is_noop(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    step: OpenTelemetryStep[OutboxEvent] = OpenTelemetryStep(service_name="svc")
    step._tracer = None  # simulate missing OTEL
    event = create_fake_event()

    # Act
    await step.on_event_start(event, context)

    # Assert
    assert step._current_spans == {}


async def test__otel_step__on_event_start__creates_span_with_attributes(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    tracer = _RecordingTracer()
    step: OpenTelemetryStep[OutboxEvent] = OpenTelemetryStep(service_name="svc")
    step._tracer = tracer  # type: ignore[assignment]
    event = create_fake_event()

    # Act
    await step.on_event_start(event, context)

    # Assert
    assert len(tracer.started) == 1
    name, span = tracer.started[0]
    assert event.event_type in name
    assert span.attributes["event.id"] == str(event.id)
    assert span.attributes["event.type"] == event.event_type
    assert span.attributes["worker.id"] == context.worker_id
    assert str(event.id) in step._current_spans


async def test__otel_step__event_with_trace_id__sets_trace_attribute(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    tracer = _RecordingTracer()
    step: OpenTelemetryStep[OutboxEvent] = OpenTelemetryStep(service_name="svc")
    step._tracer = tracer  # type: ignore[assignment]
    event = InboxEvent(
        id=uuid4(),
        event_type="t",
        payload={"a": 1},
        message_id="m1",
        consumer_group="cg",
        source="s",
        trace_id="trace-abc",
    )

    # Act
    inbox_ctx: ProcessingContext[InboxEvent] = ProcessingContext(repo=_Repo(), worker_id="w1")  # type: ignore[arg-type]
    await step.on_event_start(event, inbox_ctx)  # type: ignore[arg-type]

    # Assert
    _, span = tracer.started[0]
    assert span.attributes["event.trace_id"] == "trace-abc"


async def test__otel_step__on_event_end_without_span__is_noop(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange: no on_event_start was called for this event
    step: OpenTelemetryStep[OutboxEvent] = OpenTelemetryStep(service_name="svc")
    event = create_fake_event()

    # Act
    await step.on_event_end(event, context)

    # Assert: nothing tracked
    assert step._current_spans == {}


async def test__otel_step__on_event_end_success__sets_ok_status_and_ends_span(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    tracer = _RecordingTracer()
    step: OpenTelemetryStep[OutboxEvent] = OpenTelemetryStep(service_name="svc")
    step._tracer = tracer  # type: ignore[assignment]
    event = create_fake_event()
    await step.on_event_start(event, context)

    # Act
    await step.on_event_end(event, context)

    # Assert
    _, span = tracer.started[0]
    assert span.ended is True
    assert span.status is not None
    assert span.status.status_code == StatusCode.OK
    assert str(event.id) not in step._current_spans


async def test__otel_step__on_event_end_failure__sets_error_status(
    context: ProcessingContext[OutboxEvent],
) -> None:
    # Arrange
    tracer = _RecordingTracer()
    step: OpenTelemetryStep[OutboxEvent] = OpenTelemetryStep(service_name="svc")
    step._tracer = tracer  # type: ignore[assignment]
    event = create_fake_event()
    await step.on_event_start(event, context)
    context.mark_failed(event.id, "test error")

    # Act
    await step.on_event_end(event, context)

    # Assert
    _, span = tracer.started[0]
    assert span.status is not None
    assert span.status.status_code == StatusCode.ERROR


def test__otel_step__module_has_otel_flag__is_boolean() -> None:
    # Arrange / Act / Assert
    assert isinstance(otel_module.HAS_OTEL, bool)


def test__otel_module__import_error_fallback__sets_has_otel_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: block ``opentelemetry`` import and reimport the otel module.
    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

    def blocked_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    # Remove cached opentelemetry submodules so the import-time try/except runs
    to_remove = [k for k in sys.modules if k == "opentelemetry" or k.startswith("opentelemetry.")]
    for k in to_remove:
        monkeypatch.delitem(sys.modules, k, raising=False)
    monkeypatch.delitem(sys.modules, "omni_box.core.pipeline.steps.otel", raising=False)

    if isinstance(__builtins__, dict):
        monkeypatch.setitem(__builtins__, "__import__", blocked_import)
    else:
        monkeypatch.setattr(__builtins__, "__import__", blocked_import)

    # Act
    reloaded = importlib.import_module("omni_box.core.pipeline.steps.otel")

    # Assert
    assert reloaded.HAS_OTEL is False
