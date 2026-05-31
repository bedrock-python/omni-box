from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from ...services.results import EventHandlerResult, coerce_handler_outcome
from ..step import BaseProcessingStep, StepResult

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ...protocols.repository import EventRepository
    from ..context import ProcessingContext


class HandlerExecutionStep[T: BaseEvent](BaseProcessingStep[T]):
    """Step that executes a handler for each event."""

    def __init__(
        self,
        handler: Callable[[T, EventRepository[T]], Awaitable[EventHandlerResult | None]],
        timeout: float = 30.0,
    ) -> None:
        self._handler = handler
        self._timeout = timeout

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """Process event with handler."""
        try:
            raw = await asyncio.wait_for(self._handler(event, context.repo), timeout=self._timeout)
            outcome = coerce_handler_outcome(raw)

            if not outcome.processed:
                context.mark_skipped(
                    event.id, reason=outcome.error_message or "Explicitly skipped", status=outcome.status
                )
            elif outcome.success:
                context.mark_completed(event.id, status=outcome.status)
            else:
                context.mark_failed(
                    event.id,
                    outcome.error_message or "Unknown error",
                    count_as_attempt=outcome.count_as_attempt,
                    next_retry_at=outcome.next_retry_at,
                    status=outcome.status,
                )
        except TimeoutError:
            context.mark_failed(
                event.id,
                f"Handler execution timed out after {self._timeout}s",
                count_as_attempt=True,
                status="failed",
            )
        except Exception as e:
            context.mark_failed(event.id, f"{type(e).__name__}: {e}", status="failed")

        return StepResult.next()
