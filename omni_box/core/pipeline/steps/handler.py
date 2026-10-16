from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from ....utils.backoff import calculate_backoff_with_jitter
from ....utils.datetime import utc_now
from ...constants import (
    DEFAULT_BACKOFF_BASE_SECONDS,
    DEFAULT_BACKOFF_CAP_SECONDS,
    DEFAULT_TRANSIENT_RETRY_DELAY_SECONDS,
)
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

    A counted failure that does not say when to retry (an exception, a
    timeout, a ``handler_retry()`` without ``next_retry_at``) is retried after
    an exponential backoff: ``retry_backoff_base_seconds * 2 ** attempts_made``,
    capped at ``retry_backoff_cap_seconds``, with equal jitter (a random delay
    between half of that and all of it). ``retry_backoff_base_seconds=0``
    retries on the next cycle.
    """

    def __init__(
        self,
        handler: Callable[[T, EventRepository[T]], Awaitable[EventHandlerResult | None]],
        timeout: float = 30.0,
        *,
        retry_backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
        retry_backoff_cap_seconds: float = DEFAULT_BACKOFF_CAP_SECONDS,
    ) -> None:
        if retry_backoff_base_seconds < 0:
            raise ValueError(f"retry_backoff_base_seconds must be >= 0, got {retry_backoff_base_seconds}")
        if retry_backoff_cap_seconds <= 0:
            raise ValueError(f"retry_backoff_cap_seconds must be > 0, got {retry_backoff_cap_seconds}")
        self._handler = handler
        self._timeout = timeout
        self._backoff_base = retry_backoff_base_seconds
        self._backoff_cap = retry_backoff_cap_seconds

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
                next_retry_at = outcome.next_retry_at
                if next_retry_at is None and outcome.count_as_attempt:
                    next_retry_at = self._backoff(event)
                context.mark_failed(
                    event.id,
                    outcome.error_message or "Unknown error",
                    count_as_attempt=outcome.count_as_attempt,
                    next_retry_at=next_retry_at,
                    status=outcome.status,
                )
        except TransientError as e:
            self._retry_later(event, context, str(e))
        except TimeoutError:
            self._timed_out(event, context)
        except Exception as e:
            context.mark_failed(
                event.id, f"{type(e).__name__}: {e}", next_retry_at=self._backoff(event), status="failed"
            )

        return StepResult.next()

    def _backoff(self, event: T) -> datetime | None:
        """When a counted failure is retried; ``None`` keeps the row's schedule, so the next cycle."""
        if self._backoff_base == 0:
            return None
        delay = calculate_backoff_with_jitter(
            event.attempts_made, self._backoff_base, self._backoff_cap, jitter_mode="equal"
        )
        return utc_now() + timedelta(seconds=delay)

    def _timed_out(self, event: T, context: ProcessingContext[T]) -> None:
        """Record the handler timeout. Counts as an attempt: the handler is the event's own work."""
        context.mark_failed(
            event.id,
            f"Handler execution timed out after {self._timeout}s",
            count_as_attempt=True,
            next_retry_at=self._backoff(event),
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
