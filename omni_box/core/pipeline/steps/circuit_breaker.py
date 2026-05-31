"""Circuit breaker step for event processing."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import structlog

from ....utils.datetime import utc_now
from ..step import BaseProcessingStep, StepResult

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ..context import ProcessingContext

logger = structlog.get_logger(__name__)


class CircuitBreakerStep[T: BaseEvent](BaseProcessingStep[T]):
    """Stop batch processing if failure threshold reached.

    State is kept in-process; it does not survive worker restarts and is not
    shared between replicas. For multi-worker deployments add an external
    coordination layer (e.g. Redis-backed counters) on top of this step.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_seconds: int = 60,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout_seconds < 0:
            raise ValueError("recovery_timeout_seconds must be >= 0")
        self._failure_threshold = failure_threshold
        self._recovery_timeout = timedelta(seconds=recovery_timeout_seconds)

        self._consecutive_failures = 0
        self._is_open = False
        self._opened_at: datetime | None = None

    async def execute(
        self,
        event: T,
        context: ProcessingContext[T],
    ) -> StepResult:
        """Check if circuit is open."""
        if self._is_open:
            if self._opened_at and (utc_now() - self._opened_at) > self._recovery_timeout:
                logger.info("Circuit breaker recovery timeout reached, half-opening")
                self._is_open = False
                self._opened_at = None
            else:
                logger.warning(
                    "Circuit breaker is OPEN, stopping batch processing",
                    event_id=str(event.id),
                    consecutive_failures=self._consecutive_failures,
                )
                return StepResult.stop()

        return StepResult.next()

    async def on_event_end(self, event: T, context: ProcessingContext[T]) -> None:
        """Track failures for circuit breaker."""
        failure = context.get_failure(event.id)
        if failure:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold and not self._is_open:
                logger.error(
                    "Circuit breaker OPENED due to consecutive failures",
                    consecutive_failures=self._consecutive_failures,
                    threshold=self._failure_threshold,
                )
                self._is_open = True
                self._opened_at = utc_now()
        else:
            if self._consecutive_failures > 0:
                logger.info("Circuit breaker resetting due to success")
            self._consecutive_failures = 0
            self._is_open = False
            self._opened_at = None
