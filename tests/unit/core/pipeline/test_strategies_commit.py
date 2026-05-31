"""Unit tests for ``omni_box.core.pipeline.strategies.commit``."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.types import EventFailureUpdate
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.strategies.commit import BulkCommitStrategy, SingleCommitStrategy

pytestmark = pytest.mark.unit


class _BulkRepoFake:
    """Records bulk commit calls and optionally raises on configured method."""

    def __init__(self, *, raise_on: str | None = None) -> None:
        self.raise_on = raise_on
        self.completed_calls: list[tuple[list[UUID], str]] = []
        self.failed_calls: list[tuple[list[EventFailureUpdate], str, bool]] = []

    async def bulk_mark_completed(self, event_ids: list[UUID], worker_id: str) -> int:
        if self.raise_on == "completed":
            raise RuntimeError("boom completed")
        self.completed_calls.append((event_ids, worker_id))
        return len(event_ids)

    async def bulk_mark_failed(
        self,
        failures: list[EventFailureUpdate],
        worker_id: str,
        count_as_attempt: bool = True,
    ) -> int:
        if self.raise_on == "failed":
            raise RuntimeError("boom failed")
        self.failed_calls.append((failures, worker_id, count_as_attempt))
        return len(failures)


class _SingleRepoFake:
    """Records single-item commit calls; optional failure injection."""

    def __init__(self, *, raise_on: str | None = None) -> None:
        self.raise_on = raise_on
        self.completed_calls: list[tuple[UUID, str]] = []
        self.failed_calls: list[tuple[UUID, str, str, datetime | None, bool]] = []

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        if self.raise_on == "completed":
            raise RuntimeError("boom completed")
        self.completed_calls.append((event_id, worker_id))

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None,
        count_as_attempt: bool = True,
    ) -> None:
        if self.raise_on == "failed":
            raise RuntimeError("boom failed")
        self.failed_calls.append((event_id, error, worker_id, next_retry_at, count_as_attempt))


def _ctx(repo: Any) -> ProcessingContext[OutboxEvent]:
    return ProcessingContext(repo=repo, worker_id="w1")


# ---- BulkCommitStrategy --------------------------------------------------


async def test__bulk_commit__empty_context__returns_true_without_calls() -> None:
    # Arrange
    repo = _BulkRepoFake()
    ctx = _ctx(repo)

    # Act
    ok = await BulkCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is True
    assert repo.completed_calls == []
    assert repo.failed_calls == []


async def test__bulk_commit__with_completed_and_both_failure_kinds__calls_each_method() -> None:
    # Arrange
    repo = _BulkRepoFake()
    ctx = _ctx(repo)
    completed_id = uuid4()
    counted_id = uuid4()
    non_counted_id = uuid4()
    ctx.mark_completed(completed_id)
    ctx.mark_failed(counted_id, "permanent", count_as_attempt=True)
    ctx.mark_failed(non_counted_id, "transient", count_as_attempt=False)

    # Act
    ok = await BulkCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is True
    assert repo.completed_calls == [([completed_id], "w1")]
    assert len(repo.failed_calls) == 2
    assert repo.failed_calls[0][2] is True
    assert repo.failed_calls[1][2] is False


async def test__bulk_commit__bulk_mark_completed_raises__returns_false() -> None:
    # Arrange
    repo = _BulkRepoFake(raise_on="completed")
    ctx = _ctx(repo)
    ctx.mark_completed(uuid4())

    # Act
    ok = await BulkCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is False


async def test__bulk_commit__bulk_mark_failed_raises__returns_false() -> None:
    # Arrange
    repo = _BulkRepoFake(raise_on="failed")
    ctx = _ctx(repo)
    ctx.mark_failed(uuid4(), "boom", count_as_attempt=True)

    # Act
    ok = await BulkCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is False


# ---- SingleCommitStrategy ------------------------------------------------


async def test__single_commit__empty_context__returns_true_with_no_calls() -> None:
    # Arrange
    repo = _SingleRepoFake()
    ctx = _ctx(repo)

    # Act
    ok = await SingleCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is True
    assert repo.completed_calls == []
    assert repo.failed_calls == []


async def test__single_commit__mixed_results__applies_each_item_individually() -> None:
    # Arrange
    repo = _SingleRepoFake()
    ctx = _ctx(repo)
    completed_id = uuid4()
    counted_id = uuid4()
    non_counted_id = uuid4()
    ctx.mark_completed(completed_id)
    ctx.mark_failed(counted_id, "permanent", count_as_attempt=True)
    ctx.mark_failed(non_counted_id, "transient", count_as_attempt=False)

    # Act
    ok = await SingleCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is True
    assert repo.completed_calls == [(completed_id, "w1")]
    assert (counted_id, "permanent", "w1", None, True) in repo.failed_calls
    assert (non_counted_id, "transient", "w1", None, False) in repo.failed_calls


async def test__single_commit__mark_completed_raises__returns_false() -> None:
    # Arrange
    repo = _SingleRepoFake(raise_on="completed")
    ctx = _ctx(repo)
    ctx.mark_completed(uuid4())

    # Act
    ok = await SingleCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is False


async def test__single_commit__mark_failed_raises__returns_false() -> None:
    # Arrange
    repo = _SingleRepoFake(raise_on="failed")
    ctx = _ctx(repo)
    ctx.mark_failed(uuid4(), "boom", count_as_attempt=True)

    # Act
    ok = await SingleCommitStrategy[OutboxEvent]().commit(ctx)

    # Assert
    assert ok is False
