from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING

from ....utils.datetime import utc_now
from ...constants import DEFAULT_TRANSIENT_RETRY_DELAY_SECONDS
from ...exceptions import TransientError
from ...services.results import EventHandlerResult, EventHandlerStatus, coerce_handler_outcome
from ..step import BaseProcessingStep, StepResult

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ...protocols.repository import EventRepository
    from ..context import ProcessingContext


class HandlerExecutionStep[T: BaseEvent](BaseProcessingStep[T]):
    """Step that executes a handler for each event.

    A ``TransientError`` out of the handler says the failure belongs to a
    downstream and not to the event: it is recorded without spending an
    attempt and rescheduled a moment ahead. Every other exception, the
    timeout included, counts.
    """

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
        except TransientError as e:
            self._retry_later(event, context, str(e))
        except TimeoutError:
            self._timed_out(event, context)
        except Exception as e:
            context.mark_failed(event.id, f"{type(e).__name__}: {e}", status="failed")

        return StepResult.next()

    def _timed_out(self, event: T, context: ProcessingContext[T]) -> None:
        """Record the handler timeout. Counts as an attempt: the handler is the event's own work."""
        context.mark_failed(
            event.id,
            f"Handler execution timed out after {self._timeout}s",
            count_as_attempt=True,
            status="failed",
        )

    def _retry_later(self, event: T, context: ProcessingContext[T], error: str) -> None:
        """Record a failure that is not the event's fault: no attempt spent, retried on the next cycle."""
        context.mark_failed(
            event.id,
            error,
            count_as_attempt=False,
            next_retry_at=utc_now() + timedelta(seconds=DEFAULT_TRANSIENT_RETRY_DELAY_SECONDS),
            status=EventHandlerStatus.RETRY,
        )
