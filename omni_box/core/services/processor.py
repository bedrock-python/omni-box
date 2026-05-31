from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Unpack

import structlog

from ..pipeline.context import ProcessingContext
from .results import BatchProcessingResult

if TYPE_CHECKING:
    from ..models.entities import BaseEvent
    from ..pipeline.pipeline import ProcessingPipeline
    from ..pipeline.strategies.commit import CommitStrategy
    from ..pipeline.strategies.fetch import FetchStrategy
    from ..protocols.metrics import ProcessingMetrics
    from ..protocols.repository import EventRepository, FetchFilters


logger = structlog.get_logger(__name__)


class EventBatchProcessor[T: BaseEvent]:
    """Unified batch processor powered by pipeline architecture."""

    def __init__(
        self,
        repo: EventRepository[T],
        pipeline: ProcessingPipeline[T],
        fetch_strategy: FetchStrategy[T],
        commit_strategy: CommitStrategy[T],
        *,
        job_name: str = "event_processor",
        metrics: ProcessingMetrics | None = None,
    ) -> None:
        self._repo = repo
        self._pipeline = pipeline
        self._fetch_strategy = fetch_strategy
        self._commit_strategy = commit_strategy
        self._job_name = job_name
        self._metrics = metrics

    async def process_batch(
        self,
        worker_id: str,
        batch_size: int,
        shutdown_requested_func: Callable[[], bool] | None = None,
        **fetch_filters: Unpack[FetchFilters],
    ) -> BatchProcessingResult:
        """Fetch and process a batch of events using the configured pipeline."""
        try:
            # 1. Fetch
            events = await self._fetch_strategy.fetch(
                self._repo,
                batch_size=batch_size,
                worker_id=worker_id,
                **fetch_filters,
            )

            if not events:
                return BatchProcessingResult()

            # 2. Process
            context = ProcessingContext[T](
                repo=self._repo,
                worker_id=worker_id,
                metrics=self._metrics,
            )

            await self._pipeline.process_batch(events, context)

            # 3. Commit
            success = await self._commit_strategy.commit(context)
            if not success:
                logger.error(
                    "Failed to commit batch processing results",
                    worker_id=worker_id,
                    job_name=self._job_name,
                )

            # 4. Result
            return BatchProcessingResult(
                processed_event_ids=context.completed_ids,
                failed_counted=context.failed_counted,
                failed_noncounted=context.failed_noncounted,
                remaining_event_ids=context.skipped_ids,
                commit_failed=not success,
            )
        except (Exception, asyncio.CancelledError) as e:
            if not isinstance(e, asyncio.CancelledError):
                logger.exception(
                    f"{self._job_name} batch failed due to unexpected error",
                    error=str(e),
                    worker_id=worker_id,
                    error_type=type(e).__name__,
                )
            raise
