from __future__ import annotations

from typing import TYPE_CHECKING, Self

from ..constants import DEFAULT_LEASE_TIMEOUT_SECONDS
from ..protocols.features import (
    SupportsBulkOperations,
    SupportsDistributedLocking,
)
from ..protocols.repository import RepositoryCapabilities
from ..services.processor import EventBatchProcessor
from .pipeline import ProcessingPipeline
from .strategies.commit import BulkCommitStrategy, SingleCommitStrategy
from .strategies.fetch import (
    DistributedLockingFetchStrategy,
    OptimisticLockingFetchStrategy,
)

if TYPE_CHECKING:
    from ..models.entities import BaseEvent
    from ..protocols.metrics import ProcessingMetrics
    from ..protocols.repository import EventRepository
    from .step import ProcessingStep
    from .strategies.commit import CommitStrategy
    from .strategies.fetch import FetchStrategy


class EventProcessorBuilder[T: BaseEvent]:
    """Fluent API for constructing highly-customizable event processors."""

    def __init__(self, repo: EventRepository[T]) -> None:
        """Initialize builder with a specific event repository.

        Args:
            repo: The repository to fetch and commit events from/to.
        """
        self._repo = repo
        self._pipeline = ProcessingPipeline[T]()
        self._fetch_strategy: FetchStrategy[T] | None = None
        self._commit_strategy: CommitStrategy[T] | None = None
        self._metrics: ProcessingMetrics | None = None
        self._lease_ttl: int = DEFAULT_LEASE_TIMEOUT_SECONDS
        self._job_name: str = "event_processor"

    def with_fetch_strategy(self, strategy: FetchStrategy[T]) -> Self:
        """Explicitly set the fetching strategy."""
        self._fetch_strategy = strategy
        return self

    def with_commit_strategy(self, strategy: CommitStrategy[T]) -> Self:
        """Explicitly set the commit strategy."""
        self._commit_strategy = strategy
        return self

    def add_step(self, step: ProcessingStep[T]) -> Self:
        """Add a processing step to the pipeline."""
        self._pipeline.add_step(step)
        return self

    def with_metrics(self, metrics: ProcessingMetrics | None) -> Self:
        """Set the metrics collector."""
        self._metrics = metrics
        return self

    def with_lease_ttl(self, ttl_seconds: int) -> Self:
        """Set the distributed lock lease TTL (in seconds)."""
        if ttl_seconds <= 0:
            raise ValueError("lease_ttl must be positive")
        self._lease_ttl = ttl_seconds
        return self

    def with_job_name(self, name: str) -> Self:
        """Set the job name for logging/metrics context."""
        self._job_name = name
        return self

    def build(self) -> EventBatchProcessor[T]:
        """Finalize the processor configuration."""
        capabilities = getattr(self._repo, "capabilities", None)

        if self._fetch_strategy is None:
            if (
                isinstance(capabilities, RepositoryCapabilities) and capabilities.supports_distributed_locking
            ) or isinstance(self._repo, SupportsDistributedLocking):
                self._fetch_strategy = DistributedLockingFetchStrategy[T](ttl=self._lease_ttl)
            else:
                self._fetch_strategy = OptimisticLockingFetchStrategy[T]()

        if self._commit_strategy is None:
            if (isinstance(capabilities, RepositoryCapabilities) and capabilities.supports_bulk) or isinstance(
                self._repo, SupportsBulkOperations
            ):
                self._commit_strategy = BulkCommitStrategy[T]()
            else:
                self._commit_strategy = SingleCommitStrategy[T]()

        return EventBatchProcessor(
            repo=self._repo,
            pipeline=self._pipeline,
            fetch_strategy=self._fetch_strategy,
            commit_strategy=self._commit_strategy,
            job_name=self._job_name,
            metrics=self._metrics,
        )
