"""Unit tests for fetch and commit strategies."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.strategies.commit import BulkCommitStrategy, SingleCommitStrategy
from omni_box.core.pipeline.strategies.fetch import (
    DistributedLockingFetchStrategy,
    FilteredFetchStrategy,
    OptimisticLockingFetchStrategy,
)
from omni_box.core.protocols import (
    EventRepository,
    SupportsBulkOperations,
    SupportsDistributedLocking,
)

pytestmark = pytest.mark.unit


class MockLockRepo(EventRepository, SupportsDistributedLocking):
    async def fetch_and_lock_pending(self, *args, **kwargs):
        pass

    async def release_stale_locks(self, *args, **kwargs):
        pass

    async def force_unlock(self, *args, **kwargs):
        pass


class MockBulkRepo(EventRepository, SupportsBulkOperations):
    async def bulk_mark_completed(self, *args, **kwargs):
        pass

    async def bulk_mark_failed(self, *args, **kwargs):
        pass


@pytest.mark.asyncio
async def test__optimistic_locking_fetch__some_marks_fail__only_successful_returned() -> None:
    # Arrange
    repo = AsyncMock(spec=EventRepository)
    event1 = MagicMock()
    event1.id = uuid4()
    event2 = MagicMock()
    event2.id = uuid4()
    repo.fetch_pending.return_value = [event1, event2]
    repo.mark_processing.side_effect = [True, False]

    strategy: OptimisticLockingFetchStrategy[Any] = OptimisticLockingFetchStrategy()

    # Act
    locked = await strategy.fetch(repo, batch_size=10, worker_id="w1")

    # Assert
    assert len(locked) == 1
    assert locked[0] == event1
    assert repo.mark_processing.call_count == 2


@pytest.mark.asyncio
async def test__distributed_locking_fetch__forwards_filters_to_repo() -> None:
    # Arrange
    repo = AsyncMock(spec=MockLockRepo)
    repo.fetch_and_lock_pending.return_value = []

    strategy: DistributedLockingFetchStrategy[Any] = DistributedLockingFetchStrategy(ttl=123)

    # Act
    await strategy.fetch(repo, batch_size=10, worker_id="w1", topic="tp")

    # Assert
    repo.fetch_and_lock_pending.assert_called_once_with(limit=10, worker_id="w1", ttl=123, topic="tp")


@pytest.mark.asyncio
async def test__bulk_commit_strategy__completed_and_failed__calls_each_bulk_method() -> None:
    # Arrange
    repo = AsyncMock(spec=MockBulkRepo)
    context = ProcessingContext(repo=repo, worker_id="w1")
    context.mark_completed(uuid4())
    context.mark_failed(uuid4(), "permanent", count_as_attempt=True)
    context.mark_failed(uuid4(), "transient", count_as_attempt=False)

    strategy: BulkCommitStrategy[Any] = BulkCommitStrategy()

    # Act
    success = await strategy.commit(context)

    # Assert
    assert success is True
    repo.bulk_mark_completed.assert_called_once_with(context.completed_ids, "w1")
    repo.bulk_mark_failed.assert_any_call(context.failed_counted, "w1", count_as_attempt=True)
    repo.bulk_mark_failed.assert_any_call(context.failed_noncounted, "w1", count_as_attempt=False)


@pytest.mark.asyncio
async def test__single_commit_strategy__completed_and_failed__calls_individual_methods() -> None:
    # Arrange
    repo = AsyncMock(spec=EventRepository)
    context = ProcessingContext(repo=repo, worker_id="w1")
    context.mark_completed(uuid4())
    context.mark_failed(uuid4(), "permanent", count_as_attempt=True)
    context.mark_failed(uuid4(), "transient", count_as_attempt=False)

    strategy: SingleCommitStrategy[Any] = SingleCommitStrategy()

    # Act
    success = await strategy.commit(context)

    # Assert
    assert success is True
    repo.mark_completed.assert_called_once()
    repo.mark_failed.assert_any_call(
        context.failed_counted[0].event_id,
        "permanent",
        "w1",
        None,
        count_as_attempt=True,
    )
    repo.mark_failed.assert_any_call(
        context.failed_noncounted[0].event_id,
        "transient",
        "w1",
        None,
        count_as_attempt=False,
    )


@pytest.mark.asyncio
async def test__filtered_fetch__locking_repo__passes_source_filter() -> None:
    # Arrange
    repo = AsyncMock(spec=MockLockRepo)
    repo.fetch_and_lock_pending.return_value = []

    strategy: FilteredFetchStrategy[Any] = FilteredFetchStrategy(sources=["src1"])

    # Act
    await strategy.fetch(repo, worker_id="w1", batch_size=10, topic="tp")

    # Assert
    repo.fetch_and_lock_pending.assert_called_once_with(limit=10, worker_id="w1", ttl=300, source=["src1"], topic="tp")


@pytest.mark.asyncio
async def test__filtered_fetch__non_locking_repo__falls_back_to_optimistic_fetch() -> None:
    # Arrange
    repo = AsyncMock(spec=EventRepository)
    repo.fetch_pending.return_value = []

    strategy: FilteredFetchStrategy[Any] = FilteredFetchStrategy(sources=["src1"])

    # Act
    await strategy.fetch(repo, worker_id="w1", batch_size=10)

    # Assert
    repo.fetch_pending.assert_called_once_with(limit=10, source=["src1"])


@pytest.mark.asyncio
async def test__bulk_commit_strategy__repo_raises__returns_false() -> None:
    # Arrange
    repo = AsyncMock(spec=MockBulkRepo)
    repo.bulk_mark_completed.side_effect = RuntimeError("Commit failed")
    context = ProcessingContext(repo=repo, worker_id="w1")
    context.mark_completed(uuid4())

    strategy: BulkCommitStrategy[Any] = BulkCommitStrategy()

    # Act
    success = await strategy.commit(context)

    # Assert
    assert success is False


@pytest.mark.asyncio
async def test__single_commit_strategy__repo_raises__returns_false() -> None:
    # Arrange
    repo = AsyncMock(spec=EventRepository)
    repo.mark_completed.side_effect = RuntimeError("Commit failed")
    context = ProcessingContext(repo=repo, worker_id="w1")
    context.mark_completed(uuid4())

    strategy: SingleCommitStrategy[Any] = SingleCommitStrategy()

    # Act
    success = await strategy.commit(context)

    # Assert
    assert success is False
