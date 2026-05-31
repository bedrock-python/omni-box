from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..models.entities import BaseEvent
    from .context import ProcessingContext


@dataclass(frozen=True, slots=True)
class StepResult:
    """Result of executing a single step on an event."""

    should_skip_event: bool = False
    """If True, skip the remaining steps in the pipeline for THIS event."""
    should_stop_pipeline: bool = False
    """If True, stop processing the WHOLE BATCH after this step."""

    @classmethod
    def next(cls) -> StepResult:
        """Continue to the next step in the pipeline."""
        return cls(should_skip_event=False, should_stop_pipeline=False)

    @classmethod
    def skip(cls) -> StepResult:
        """Skip the remaining steps for THIS event."""
        return cls(should_skip_event=True, should_stop_pipeline=False)

    @classmethod
    def stop(cls) -> StepResult:
        """Stop processing the WHOLE BATCH immediately."""
        return cls(should_skip_event=False, should_stop_pipeline=True)


@runtime_checkable
class ProcessingStep[T: BaseEvent](Protocol):
    """Protocol for an event processing step.

    A step is a single unit of logic that can be executed as part of
    an event processing pipeline.
    """

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """Execute the step logic for a single event.

        Args:
            event: The domain event being processed.
            context: Shared processing context for tracking state and results.

        Returns:
            A ``StepResult`` that tells the pipeline whether to continue,
            skip the current event, or stop the whole batch processing.
        """
        ...


@runtime_checkable
class BatchHooks[T: BaseEvent](Protocol):
    """Optional hooks for batch lifecycle.

    If a step implements this protocol, the pipeline will trigger these
    hooks at the start and end of each batch processing.
    """

    async def on_batch_start(self, context: ProcessingContext[T]) -> None:
        """Hook called once BEFORE the batch processing starts."""
        ...

    async def on_batch_end(self, context: ProcessingContext[T]) -> None:
        """Hook called once AFTER the batch processing finishes."""
        ...


@runtime_checkable
class EventHooks[T: BaseEvent](Protocol):
    """Optional hooks for single event lifecycle.

    If a step implements this protocol, the pipeline will trigger these
    hooks for every event processed by the pipeline.
    """

    async def on_event_start(self, event: T, context: ProcessingContext[T]) -> None:
        """Hook called before processing of an event starts."""
        ...

    async def on_event_end(self, event: T, context: ProcessingContext[T]) -> None:
        """Hook called after processing of an event finishes."""
        ...


class BaseProcessingStep[T: BaseEvent]:
    """Base class for processing steps with default no-op hooks."""

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """Execute the step for a single event."""
        return StepResult.next()

    async def on_batch_start(self, context: ProcessingContext[T]) -> None:
        """Hook called once BEFORE the batch processing starts."""
        pass

    async def on_batch_end(self, context: ProcessingContext[T]) -> None:
        """Hook called once AFTER the batch processing finishes."""
        pass

    async def on_event_start(self, event: T, context: ProcessingContext[T]) -> None:
        """Hook called before processing of an event starts."""
        pass

    async def on_event_end(self, event: T, context: ProcessingContext[T]) -> None:
        """Hook called after processing of an event finishes."""
        pass
