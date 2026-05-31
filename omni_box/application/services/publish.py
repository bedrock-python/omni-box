"""Outbox publisher core service."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Unpack

import structlog

from ...core.constants import (
    DEFAULT_PUBLISH_TIMEOUT_SECONDS,
)
from ...core.services.metrics import NoOpOutboxMetrics
from ..factories import create_outbox_processor

if TYPE_CHECKING:
    from ...core.models.entities import OutboxEvent
    from ...core.models.types import PositiveInt
    from ...core.protocols import (
        EventPublisher,
        FetchFilters,
        OutboxEventRepository,
    )
    from ...core.protocols.metrics import OutboxMetrics
    from ...core.services.results import BatchProcessingResult


logger = structlog.get_logger(__name__)


class OutboxPublisher:
    """High-level service for publishing outbox events to a message broker."""

    def __init__(
        self,
        repo: OutboxEventRepository,
        broker: EventPublisher,
        metrics: OutboxMetrics | None = None,
        publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS,
        concurrency_limit: int | None = None,
    ) -> None:
        self._repo = repo
        self._broker = broker
        self._metrics = metrics or NoOpOutboxMetrics()
        self._processor = create_outbox_processor(
            repo=repo,
            publisher=broker,
            metrics=self._metrics,
            job_name="outbox_publisher",
            publish_timeout=publish_timeout,
        )
        self._semaphore = asyncio.Semaphore(concurrency_limit) if concurrency_limit else None

    async def publish_batch(
        self,
        worker_id: str,
        batch_size: PositiveInt,
        shutdown_requested_func: Callable[[], bool] | None = None,
        **fetch_filters: Unpack[FetchFilters],
    ) -> BatchProcessingResult[OutboxEvent]:
        if self._semaphore:
            async with self._semaphore:
                return await self._processor.process_batch(
                    worker_id=worker_id,
                    batch_size=batch_size,
                    shutdown_requested_func=shutdown_requested_func,
                    **fetch_filters,
                )

        return await self._processor.process_batch(
            worker_id=worker_id,
            batch_size=batch_size,
            shutdown_requested_func=shutdown_requested_func,
            **fetch_filters,
        )
