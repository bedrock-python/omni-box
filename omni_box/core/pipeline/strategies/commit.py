from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast

import structlog

from ...protocols.features import SupportsBulkOperations

if TYPE_CHECKING:
    from ...models.entities import BaseEvent
    from ..context import ProcessingContext

logger = structlog.get_logger(__name__)


class CommitStrategy[T: BaseEvent](Protocol):
    """Protocol for committing processing results back to storage."""

    async def commit(self, context: ProcessingContext[T]) -> bool:
        """Apply results from the context to storage."""
        ...


class BulkCommitStrategy[T: BaseEvent]:
    """Efficient batch update strategy for committing results."""

    async def commit(self, context: ProcessingContext[T]) -> bool:
        """Apply results using bulk database operations."""
        repo = cast(SupportsBulkOperations[T], context.repo)
        try:
            if context.completed_ids:
                await repo.bulk_mark_completed(context.completed_ids, context.worker_id)

            if context.failed_counted:
                await repo.bulk_mark_failed(context.failed_counted, context.worker_id, count_as_attempt=True)

            if context.failed_noncounted:
                await repo.bulk_mark_failed(context.failed_noncounted, context.worker_id, count_as_attempt=False)
        except Exception as e:
            logger.exception(
                "Bulk commit failed",
                worker_id=context.worker_id,
                error=str(e),
                num_completed=len(context.completed_ids),
                num_failed_counted=len(context.failed_counted),
                num_failed_noncounted=len(context.failed_noncounted),
            )
            return False

        return True


class SingleCommitStrategy[T: BaseEvent]:
    """Conservative sequential update strategy for committing results."""

    async def commit(self, context: ProcessingContext[T]) -> bool:
        """Apply results individually for each event."""
        repo = context.repo
        try:
            for event_id in context.completed_ids:
                await repo.mark_completed(event_id, context.worker_id)

            for failure in context.failed_counted:
                await repo.mark_failed(
                    failure.event_id,
                    failure.error,
                    context.worker_id,
                    failure.next_retry_at,
                    count_as_attempt=True,
                )

            for failure in context.failed_noncounted:
                await repo.mark_failed(
                    failure.event_id,
                    failure.error,
                    context.worker_id,
                    failure.next_retry_at,
                    count_as_attempt=False,
                )
        except Exception as e:
            logger.exception(
                "Single commit strategy failed",
                worker_id=context.worker_id,
                error=str(e),
            )
            return False

        return True
