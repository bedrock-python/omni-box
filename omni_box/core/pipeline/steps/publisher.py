"""Publisher step for the outbox pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from ...services.results import EventHandlerStatus
from ..step import StepResult
from .handler import HandlerExecutionStep

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ..context import ProcessingContext

logger = structlog.get_logger(__name__)

# ``ProcessingContext.extra`` key holding the broker error that ended this batch's
# publishing. Per batch, not per step: one processor may run several batches at once.
_DEFERRED_BROKER_ERROR = "publisher_broker_unreachable"


class PublisherExecutionStep[T: BaseEvent](HandlerExecutionStep[T]):
    """Step that publishes each event through a broker publisher.

    Everything in an outbox batch goes to the same broker, so a broker that
    does not answer is a property of the cycle, not of the row. A
    ``TransientError`` or a timeout out of the publisher is recorded without
    spending an attempt, and the publisher is not called again in this batch:
    the remaining events are marked the same way, their schedule untouched,
    and come back in the next cycle. Any other exception is about the event
    and counts, as it does in ``HandlerExecutionStep``.
    """

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """Publish the event, unless this batch already found the broker unreachable."""
        deferred = context.extra.get(_DEFERRED_BROKER_ERROR)
        if isinstance(deferred, str):
            context.mark_failed(event.id, deferred, count_as_attempt=False, status=EventHandlerStatus.RETRY)
            return StepResult.next()
        return await super().execute(event, context)

    def _timed_out(self, event: T, context: ProcessingContext[T]) -> None:
        """A publish the broker did not acknowledge in time is the broker's problem, not the row's."""
        self._retry_later(event, context, f"Publish timed out after {self._timeout}s")

    def _retry_later(self, event: T, context: ProcessingContext[T], error: str) -> None:
        super()._retry_later(event, context, error)
        context.extra[_DEFERRED_BROKER_ERROR] = error
        logger.warning(
            "Broker unreachable, deferring the rest of the batch",
            event_id=str(event.id),
            worker_id=context.worker_id,
            error=error,
        )


__all__ = ["PublisherExecutionStep"]
