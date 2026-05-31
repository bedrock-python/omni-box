"""Dead Letter Queue (DLQ) step for event processing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

import structlog

from ..step import BaseProcessingStep, StepResult

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ..context import ProcessingContext

logger = structlog.get_logger(__name__)


@runtime_checkable
class DLQStorage[T: BaseEvent](Protocol):
    """Protocol for DLQ storage."""

    async def move_to_dlq(self, event: T, error: str) -> None:
        """Move an event to DLQ storage."""
        ...


class DLQStep[T: BaseEvent](BaseProcessingStep[T]):
    """Dead Letter Queue (DLQ) step.

    This step is *best-effort*: the move into DLQ is not part of the same
    transaction as the status update applied by the commit strategy. A failure
    in ``move_to_dlq`` is logged and swallowed; the event will still transition
    to ``FAILED`` and stop being retried. Pair with an idempotent DLQ sink
    (e.g. Kafka with a unique key) to avoid duplicates on replay.

    Only failures that ``count_as_attempt=True`` are considered: transient
    failures (``count_as_attempt=False``) never trigger DLQ even if the
    pre-attempt counter would otherwise indicate the cap is reached.
    """

    def __init__(self, dlq_storage: DLQStorage[T]) -> None:
        self._dlq_storage = dlq_storage

    async def on_event_end(self, event: T, context: ProcessingContext[T]) -> None:
        """Check if event should be moved to DLQ."""
        # Only counted failures (those that increment ``attempts_made``) can
        # exhaust the retry budget. Transient/non-counted failures must never
        # be routed to DLQ even if the projected attempt count would exceed
        # ``max_attempts``.
        counted = next((f for f in context.failed_counted if f.event_id == event.id), None)
        if counted is None:
            return

        projected_attempts = event.attempts_made + 1
        if projected_attempts < event.max_attempts:
            return

        logger.warning(
            "Event reached max attempts, moving to DLQ",
            event_id=str(event.id),
            event_type=event.event_type,
            attempts=projected_attempts,
            max_attempts=event.max_attempts,
            error=counted.error,
        )

        try:
            await self._dlq_storage.move_to_dlq(event, counted.error)
        except Exception:
            logger.exception("Failed to move event to DLQ", event_id=str(event.id))

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """No-op execution, DLQ handled by hooks."""
        return StepResult.next()
