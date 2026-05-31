from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from ..step import BaseProcessingStep, StepResult

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ...protocols.metrics import ProcessingMetrics
    from ..context import ProcessingContext

logger = structlog.get_logger(__name__)


class MetricsStep[T: BaseEvent](BaseProcessingStep[T]):
    """Metrics collection step for event processing."""

    def __init__(self, metrics: ProcessingMetrics | None) -> None:
        self.metrics = metrics
        self._start_times: dict[UUID, float] = {}
        self._event_types: dict[UUID, str] = {}

    async def on_batch_start(self, context: ProcessingContext[T]) -> None:
        """Reset per-batch state to prevent leaks across batches."""
        self._start_times.clear()
        self._event_types.clear()

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """Start tracking metrics for a single event."""
        self._start_times[event.id] = perf_counter()
        self._event_types[event.id] = event.event_type
        return StepResult.next()

    async def on_batch_end(self, context: ProcessingContext[T]) -> None:
        """Record batch-level metrics."""
        if self.metrics is None:
            self._start_times.clear()
            self._event_types.clear()
            return

        metrics = self.metrics
        try:
            now = perf_counter()
            for event_id in context.completed_ids:
                event_type = self._event_types.get(event_id)
                status = context.statuses.get(event_id)
                if start_time := self._start_times.get(event_id):
                    duration = now - start_time
                    metrics.observe_handler_duration(duration, event_type=event_type)

                metrics.inc_processed(1, event_type=event_type, status=status)

            for failure in context.failed_counted:
                event_type = self._event_types.get(failure.event_id)
                status = context.statuses.get(failure.event_id)
                metrics.inc_failed(1, event_type=event_type, status=status)

            for failure in context.failed_noncounted:
                event_type = self._event_types.get(failure.event_id)
                status = context.statuses.get(failure.event_id)
                metrics.inc_failed(1, event_type=event_type, status=status)

            for event_id in context.skipped_ids:
                event_type = self._event_types.get(event_id)
                status = context.statuses.get(event_id)
                metrics.inc_duplicate(1, event_type=event_type, status=status)
        finally:
            # Always release per-batch state, even if metrics emission raised.
            self._start_times.clear()
            self._event_types.clear()
