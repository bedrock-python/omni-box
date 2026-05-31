"""OpenTelemetry tracing step for event processing."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from ..step import BaseProcessingStep, StepResult

if TYPE_CHECKING:
    from opentelemetry.trace import Span

    from ...models.entities import BaseEvent
    from ..context import ProcessingContext

logger = structlog.get_logger(__name__)

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False


class OpenTelemetryStep[T: BaseEvent](BaseProcessingStep[T]):
    """OpenTelemetry tracing step."""

    def __init__(self, service_name: str = "omni-box") -> None:
        self._service_name = service_name
        self._tracer = trace.get_tracer(service_name) if HAS_OTEL else None
        self._current_spans: dict[str, Span] = {}

    async def on_batch_start(self, context: ProcessingContext[T]) -> None:
        """End and drop any spans left over from a previous batch."""
        self._close_dangling_spans()

    async def on_event_start(self, event: T, context: ProcessingContext[T]) -> None:
        """Start span for event processing."""
        if not self._tracer:
            return

        span_name = f"process {event.event_type}"
        span = self._tracer.start_span(span_name)
        span.set_attribute("event.id", str(event.id))
        span.set_attribute("event.type", event.event_type)
        span.set_attribute("worker.id", context.worker_id)

        if hasattr(event, "trace_id") and event.trace_id:
            span.set_attribute("event.trace_id", event.trace_id)

        self._current_spans[str(event.id)] = span

    async def on_event_end(self, event: T, context: ProcessingContext[T]) -> None:
        """End span for event processing."""
        span = self._current_spans.pop(str(event.id), None)
        if not span:
            return

        if event.id in context.failed_ids:
            span.set_status(Status(StatusCode.ERROR))
        else:
            span.set_status(Status(StatusCode.OK))

        span.end()

    async def on_batch_end(self, context: ProcessingContext[T]) -> None:
        """Guarantee no spans leak past the batch boundary."""
        self._close_dangling_spans()

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """No-op execution, tracing handled by hooks."""
        return StepResult.next()

    def _close_dangling_spans(self) -> None:
        if not self._current_spans:
            return
        logger.warning(
            "OpenTelemetryStep is closing leftover spans",
            count=len(self._current_spans),
        )
        for span in self._current_spans.values():
            try:
                span.set_status(Status(StatusCode.ERROR, "abandoned"))
                span.end()
            except Exception:
                logger.debug("Failed to end leftover span", exc_info=True)
        self._current_spans.clear()
