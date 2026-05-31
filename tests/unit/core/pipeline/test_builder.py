"""Unit tests for ``omni_box.core.pipeline.builder``."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from omni_box.core.constants import DEFAULT_LEASE_TIMEOUT_SECONDS
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.types import EventFailureUpdate
from omni_box.core.pipeline.builder import EventProcessorBuilder
from omni_box.core.pipeline.step import BaseProcessingStep, StepResult
from omni_box.core.pipeline.strategies.commit import BulkCommitStrategy, SingleCommitStrategy
from omni_box.core.pipeline.strategies.fetch import (
    DistributedLockingFetchStrategy,
    OptimisticLockingFetchStrategy,
)
from omni_box.core.protocols.repository import RepositoryCapabilities

pytestmark = pytest.mark.unit


class _PlainRepo:
    """Repo with no extra capabilities (no `capabilities` attribute)."""

    async def create(self, event: OutboxEvent) -> OutboxEvent:
        return event

    async def get_by_id(self, event_id: UUID) -> None:
        return None

    async def fetch_pending(self, limit: int, **filters: Any) -> list[OutboxEvent]:
        return []

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        return None

    async def mark_failed(
        self, event_id: UUID, error: str, worker_id: str, next_retry_at: Any, count_as_attempt: bool = True
    ) -> None:
        return None


class _CapabilitiesRepo(_PlainRepo):
    def __init__(self, *, bulk: bool = False, locking: bool = False, retention: bool = False) -> None:
        self._caps = RepositoryCapabilities(
            supports_bulk=bulk,
            supports_distributed_locking=locking,
            supports_retention=retention,
        )

    @property
    def capabilities(self) -> RepositoryCapabilities:
        return self._caps


class _DistributedLockingRepo(_PlainRepo):
    """Repo that structurally implements SupportsDistributedLocking."""

    async def fetch_and_lock_pending(
        self, limit: int, worker_id: str, ttl: int | None = None, **filters: Any
    ) -> list[OutboxEvent]:
        return []

    async def refresh_lock(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def release_lock(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def force_unlock(self, event_id: UUID, reason: str) -> bool:
        return True


class _BulkRepo(_PlainRepo):
    """Repo that structurally implements SupportsBulkOperations."""

    async def bulk_mark_completed(self, event_ids: list[UUID], worker_id: str) -> int:
        return len(event_ids)

    async def bulk_create(self, events: list[OutboxEvent]) -> list[OutboxEvent]:
        return events

    async def bulk_mark_failed(
        self, failures: list[EventFailureUpdate], worker_id: str, count_as_attempt: bool = True
    ) -> int:
        return len(failures)

    async def bulk_release_locks(self, event_ids: list[UUID], worker_id: str) -> int:
        return len(event_ids)


def test__builder__plain_repo__selects_optimistic_and_single_strategies() -> None:
    # Arrange
    repo = _PlainRepo()
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    processor = builder.build()

    # Assert
    assert isinstance(processor._fetch_strategy, OptimisticLockingFetchStrategy)
    assert isinstance(processor._commit_strategy, SingleCommitStrategy)
    assert processor._job_name == "event_processor"
    assert processor._metrics is None


def test__builder__capabilities_locking__selects_distributed_locking_fetch() -> None:
    # Arrange
    repo = _CapabilitiesRepo(locking=True)
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    processor = builder.build()

    # Assert
    assert isinstance(processor._fetch_strategy, DistributedLockingFetchStrategy)


def test__builder__structural_locking__selects_distributed_locking_fetch() -> None:
    # Arrange: repo without capabilities, structurally satisfies the protocol
    repo = _DistributedLockingRepo()
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    processor = builder.build()

    # Assert
    assert isinstance(processor._fetch_strategy, DistributedLockingFetchStrategy)
    assert processor._fetch_strategy.ttl == DEFAULT_LEASE_TIMEOUT_SECONDS


def test__builder__capabilities_bulk__selects_bulk_commit() -> None:
    # Arrange
    repo = _CapabilitiesRepo(bulk=True)
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    processor = builder.build()

    # Assert
    assert isinstance(processor._commit_strategy, BulkCommitStrategy)


def test__builder__structural_bulk__selects_bulk_commit() -> None:
    # Arrange
    repo = _BulkRepo()
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    processor = builder.build()

    # Assert
    assert isinstance(processor._commit_strategy, BulkCommitStrategy)


def test__builder__custom_strategies__override_defaults() -> None:
    # Arrange
    repo = _PlainRepo()
    fetch = DistributedLockingFetchStrategy[OutboxEvent](ttl=42)
    commit: BulkCommitStrategy[OutboxEvent] = BulkCommitStrategy()
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    processor = builder.with_fetch_strategy(fetch).with_commit_strategy(commit).with_job_name("custom").build()

    # Assert
    assert processor._fetch_strategy is fetch
    assert processor._commit_strategy is commit
    assert processor._job_name == "custom"


def test__builder__with_lease_ttl__sets_ttl_on_auto_locking_strategy() -> None:
    # Arrange
    repo = _DistributedLockingRepo()
    expected_ttl = 99
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    processor = builder.with_lease_ttl(expected_ttl).build()

    # Assert
    assert isinstance(processor._fetch_strategy, DistributedLockingFetchStrategy)
    assert processor._fetch_strategy.ttl == expected_ttl


@pytest.mark.parametrize("invalid_ttl", [0, -1, -100], ids=["zero", "neg-one", "neg-hundred"])
def test__builder__with_lease_ttl_non_positive__raises_value_error(invalid_ttl: int) -> None:
    # Arrange
    repo = _PlainRepo()
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act / Assert
    with pytest.raises(ValueError, match="lease_ttl must be positive"):
        builder.with_lease_ttl(invalid_ttl)


def test__builder__add_step_and_with_metrics__are_propagated_to_processor() -> None:
    # Arrange
    repo = _PlainRepo()
    step = BaseProcessingStep[OutboxEvent]()

    class _Metrics:
        def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
            return None

        def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
            return None

        def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
            return None

        def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
            return None

    metrics = _Metrics()
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]

    # Act
    res = builder.add_step(step).with_metrics(metrics)
    processor = res.build()

    # Assert (chaining returns self)
    assert res is builder
    assert step in processor._pipeline._steps
    assert processor._metrics is metrics


def test__builder__fluent_chain__returns_self_on_every_method() -> None:
    # Arrange
    repo = _PlainRepo()
    builder: EventProcessorBuilder[OutboxEvent] = EventProcessorBuilder(repo)  # type: ignore[arg-type]
    fetch = OptimisticLockingFetchStrategy[OutboxEvent]()
    commit = SingleCommitStrategy[OutboxEvent]()

    # Act
    chained = (
        builder.with_fetch_strategy(fetch)
        .with_commit_strategy(commit)
        .with_metrics(None)
        .with_lease_ttl(10)
        .with_job_name("x")
    )

    # Assert
    assert chained is builder


def test__step_result__returned_from_base__is_next_signal() -> None:
    # Arrange / Act
    result = StepResult.next()

    # Assert: imported StepResult is reachable & well-formed
    assert (result.should_skip_event, result.should_stop_pipeline) == (False, False)
