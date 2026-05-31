"""Unit tests for ``omni_box.core.pipeline.strategies.fetch``."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from omni_box.core.constants import DEFAULT_LEASE_TIMEOUT_SECONDS
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.strategies.fetch import (
    DistributedLockingFetchStrategy,
    FilteredFetchStrategy,
    OptimisticLockingFetchStrategy,
)
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _OptimisticRepo:
    """Plain repo: only supports fetch_pending + mark_processing."""

    def __init__(
        self,
        pending: list[OutboxEvent],
        *,
        mark_results: list[bool] | None = None,
    ) -> None:
        self.pending = pending
        self._mark_results = mark_results or [True] * len(pending)
        self.fetch_calls: list[tuple[int, dict[str, Any]]] = []
        self.mark_calls: list[tuple[UUID, str]] = []

    async def fetch_pending(self, limit: int, **filters: Any) -> list[OutboxEvent]:
        self.fetch_calls.append((limit, filters))
        return list(self.pending)[:limit]

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        self.mark_calls.append((event_id, worker_id))
        idx = len(self.mark_calls) - 1
        return self._mark_results[idx] if idx < len(self._mark_results) else True


class _LockingRepo(_OptimisticRepo):
    """Repo that supports distributed locking."""

    def __init__(self, pending: list[OutboxEvent]) -> None:
        super().__init__(pending)
        self.lock_calls: list[tuple[int, str, int | None, dict[str, Any]]] = []

    async def fetch_and_lock_pending(
        self,
        limit: int,
        worker_id: str,
        ttl: int | None = None,
        **filters: Any,
    ) -> list[OutboxEvent]:
        self.lock_calls.append((limit, worker_id, ttl, filters))
        return list(self.pending)[:limit]

    async def refresh_lock(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def release_lock(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def force_unlock(self, event_id: UUID, reason: str) -> bool:
        return True


# ---- OptimisticLockingFetchStrategy --------------------------------------


async def test__optimistic_fetch__all_marks_succeed__returns_all_events() -> None:
    # Arrange
    events = [create_fake_event(), create_fake_event()]
    repo = _OptimisticRepo(events)
    strategy = OptimisticLockingFetchStrategy[OutboxEvent]()

    # Act
    locked = await strategy.fetch(repo, batch_size=10, worker_id="w1", topic="t")  # type: ignore[arg-type]

    # Assert
    assert locked == events
    assert repo.fetch_calls == [(10, {"topic": "t"})]
    assert {c[0] for c in repo.mark_calls} == {e.id for e in events}


async def test__optimistic_fetch__some_marks_fail__only_successful_returned() -> None:
    # Arrange
    events = [create_fake_event(), create_fake_event(), create_fake_event()]
    repo = _OptimisticRepo(events, mark_results=[True, False, True])
    strategy = OptimisticLockingFetchStrategy[OutboxEvent]()

    # Act
    locked = await strategy.fetch(repo, batch_size=5, worker_id="w1")  # type: ignore[arg-type]

    # Assert
    assert locked == [events[0], events[2]]


async def test__optimistic_fetch__empty_pending__returns_empty() -> None:
    # Arrange
    repo = _OptimisticRepo([])
    strategy = OptimisticLockingFetchStrategy[OutboxEvent]()

    # Act
    locked = await strategy.fetch(repo, batch_size=5, worker_id="w1")  # type: ignore[arg-type]

    # Assert
    assert locked == []


# ---- DistributedLockingFetchStrategy -------------------------------------


def test__distributed_locking__default_ttl__matches_constant() -> None:
    # Arrange / Act
    strategy = DistributedLockingFetchStrategy[OutboxEvent]()

    # Assert
    assert strategy.ttl == DEFAULT_LEASE_TIMEOUT_SECONDS


async def test__distributed_locking_fetch__forwards_args_to_repo() -> None:
    # Arrange
    events = [create_fake_event()]
    repo = _LockingRepo(events)
    custom_ttl = 123
    strategy = DistributedLockingFetchStrategy[OutboxEvent](ttl=custom_ttl)

    # Act
    locked = await strategy.fetch(repo, batch_size=7, worker_id="w-1", topic="t")  # type: ignore[arg-type]

    # Assert
    assert locked == events
    assert repo.lock_calls == [(7, "w-1", custom_ttl, {"topic": "t"})]


# ---- FilteredFetchStrategy -----------------------------------------------


async def test__filtered_fetch__locking_repo_with_sources__merges_filters_and_uses_locking() -> None:
    # Arrange
    events = [create_fake_event()]
    repo = _LockingRepo(events)
    strategy = FilteredFetchStrategy[OutboxEvent](sources=["src-a"], ttl=55)

    # Act
    locked = await strategy.fetch(repo, batch_size=4, worker_id="wx", topic="tp")  # type: ignore[arg-type]

    # Assert
    assert locked == events
    assert repo.lock_calls == [
        (4, "wx", 55, {"source": ["src-a"], "topic": "tp"}),
    ]


async def test__filtered_fetch__non_locking_repo_with_sources__falls_back_to_optimistic_path() -> None:
    # Arrange
    events = [create_fake_event(), create_fake_event()]
    repo = _OptimisticRepo(events, mark_results=[True, False])
    strategy = FilteredFetchStrategy[OutboxEvent](sources=["src-b"])

    # Act
    locked = await strategy.fetch(repo, batch_size=10, worker_id="w")  # type: ignore[arg-type]

    # Assert
    assert locked == [events[0]]
    assert repo.fetch_calls == [(10, {"source": ["src-b"]})]


async def test__filtered_fetch__no_sources_and_locking_repo__only_passes_original_filters() -> None:
    # Arrange
    repo = _LockingRepo([])
    strategy = FilteredFetchStrategy[OutboxEvent](sources=None)

    # Act
    await strategy.fetch(repo, batch_size=2, worker_id="w", topic="t")  # type: ignore[arg-type]

    # Assert: no "source" added since sources=None
    assert repo.lock_calls == [(2, "w", DEFAULT_LEASE_TIMEOUT_SECONDS, {"topic": "t"})]


async def test__filtered_fetch__no_sources_and_plain_repo__falls_back_without_source_filter() -> None:
    # Arrange
    repo = _OptimisticRepo([])
    strategy = FilteredFetchStrategy[OutboxEvent](sources=None)

    # Act
    await strategy.fetch(repo, batch_size=3, worker_id="w")  # type: ignore[arg-type]

    # Assert
    assert repo.fetch_calls == [(3, {})]
