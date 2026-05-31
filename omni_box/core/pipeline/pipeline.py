from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from .exceptions import PipelineStoppedError
from .step import BatchHooks, EventHooks, ProcessingStep

if TYPE_CHECKING:
    from ..models.entities import BaseEvent
    from .context import ProcessingContext

logger = structlog.get_logger(__name__)


class ProcessingPipeline[T: BaseEvent]:
    """Pipeline for event processing with multiple sequential steps."""

    def __init__(self, steps: list[ProcessingStep[T]] | None = None) -> None:
        """Initialize the pipeline with an optional list of steps.

        Args:
            steps: Initial sequence of processing steps.
        """
        self._steps: list[ProcessingStep[T]] = steps or []

    def add_step(self, step: ProcessingStep[T]) -> None:
        """Add a processing step to the end of the pipeline.

        Steps are executed in the order they were added.
        """
        self._steps.append(step)

    async def process_event(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> None:
        """Process a single event through all steps in the pipeline.

        This method triggers ``on_event_start`` and ``on_event_end`` hooks for
        steps that implement them. Execution continues until a step signals
        to skip the event or stop the pipeline.

        Args:
            event: The domain event to process.
            context: Shared processing context for state and result tracking.

        Raises:
            PipelineStoppedError: If a step signals that the entire batch
                processing should stop immediately.
        """
        # 1. Event Start Hooks
        for step in self._steps:
            if isinstance(step, EventHooks):
                await step.on_event_start(event, context)

        # 2. Sequential Step Execution
        try:
            for step in self._steps:
                result = await step.execute(event, context)
                if result.should_skip_event:
                    break
                if result.should_stop_pipeline:
                    raise PipelineStoppedError
        finally:
            # 3. Event End Hooks
            for step in self._steps:
                if isinstance(step, EventHooks):
                    await step.on_event_end(event, context)

    async def process_batch(
        self,
        events: list[T],
        context: ProcessingContext[T],
    ) -> None:
        """Process a batch of events through the pipeline.

        Triggers ``on_batch_start`` and ``on_batch_end`` hooks. Processes
        events sequentially until all are done or a step signals to stop.

        Args:
            events: List of domain events to process in this batch.
            context: Shared processing context for state and result tracking.
        """
        # 1. Batch Start Hooks
        for step in self._steps:
            if isinstance(step, BatchHooks):
                await step.on_batch_start(context)

        # 2. Sequential Event Processing
        try:
            for event in events:
                if event.id in context.skipped_ids:
                    continue

                await self.process_event(event, context)
        except PipelineStoppedError:
            logger.info("Batch processing stopped early due to pipeline signal")

        # 3. Batch End Hooks
        for step in self._steps:
            if isinstance(step, BatchHooks):
                await step.on_batch_end(context)
