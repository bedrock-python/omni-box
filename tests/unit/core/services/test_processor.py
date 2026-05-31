"""Unit tests for ``omni_box.core.services.processor``."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.pipeline import ProcessingPipeline
from omni_box.core.services.processor import EventBatchProcessor
from omni_box.core.services.results import BatchProcessingResult
from tests.helpers import create_fake_event

pytestmark = pytest.mark.unit


class _Repo:
    pass


class _FetchFake:
    def __init__(self, events: list[OutboxEvent] | BaseException | None = None) -> None:
        self._events = events
        self.calls: list[tuple[int, str, dict[str, Any]]] = []

    async def fetch(
        self,
        repo: Any,
        batch_size: int,
        worker_id: str,
        **filters: Any,
    ) -> list[OutboxEvent]:
        self.calls.append((batch_size, worker_id, filters))
        if isinstance(self._events, BaseException):
            raise self._events
        return list(self._events or [])


class _CommitFake:
    def __init__(self, *, ok: bool = True) -> None:
        self.ok = ok
        self.contexts: list[ProcessingContext[OutboxEvent]] = []

    async def commit(self, context: ProcessingContext[OutboxEvent]) -> bool:
        self.contexts.append(context)
        return self.ok


class _PipelineSpy(ProcessingPipeline[OutboxEvent]):
    def __init__(self) -> None:
        super().__init__([])
        self.batches: list[list[OutboxEvent]] = []

    async def process_batch(
        self,
        events: list[OutboxEvent],
        context: ProcessingContext[OutboxEvent],
    ) -> None:
        self.batches.append(events)
        # simulate a successful completion of the first event
        if events:
            context.mark_completed(events[0].id, status="completed")


async def test__processor__no_events__returns_empty_result_and_skips_pipeline() -> None:
    # Arrange
    repo = _Repo()
    fetch = _FetchFake(events=[])
    commit = _CommitFake()
    pipeline = _PipelineSpy()
    processor: EventBatchProcessor[OutboxEvent] = EventBatchProcessor(
        repo,
        pipeline,
        fetch,
        commit,  # type: ignore[arg-type]
    )

    # Act
    result = await processor.process_batch(worker_id="w-1", batch_size=10)

    # Assert
    assert isinstance(result, BatchProcessingResult)
    assert result.processed_event_ids == []
    assert pipeline.batches == []
    assert commit.contexts == []


async def test__processor__events_committed_successfully__returns_populated_result() -> None:
    # Arrange
    repo = _Repo()
    events = [create_fake_event()]
    fetch = _FetchFake(events=events)
    commit = _CommitFake(ok=True)
    pipeline = _PipelineSpy()
    processor: EventBatchProcessor[OutboxEvent] = EventBatchProcessor(
        repo,
        pipeline,
        fetch,
        commit,
        job_name="job-x",  # type: ignore[arg-type]
    )

    # Act
    result = await processor.process_batch(worker_id="w-1", batch_size=5, topic="t")

    # Assert
    assert result.processed_event_ids == [events[0].id]
    assert result.commit_failed is False
    assert fetch.calls == [(5, "w-1", {"topic": "t"})]
    assert pipeline.batches == [events]


async def test__processor__commit_returns_false__result_has_commit_failed_true() -> None:
    # Arrange
    repo = _Repo()
    events = [create_fake_event()]
    fetch = _FetchFake(events=events)
    commit = _CommitFake(ok=False)
    pipeline = _PipelineSpy()
    processor: EventBatchProcessor[OutboxEvent] = EventBatchProcessor(
        repo,
        pipeline,
        fetch,
        commit,  # type: ignore[arg-type]
    )

    # Act
    result = await processor.process_batch(worker_id="w-1", batch_size=5)

    # Assert
    assert result.commit_failed is True


async def test__processor__fetch_raises__propagates_exception() -> None:
    # Arrange
    repo = _Repo()
    fetch = _FetchFake(events=RuntimeError("db down"))
    commit = _CommitFake()
    pipeline = _PipelineSpy()
    processor: EventBatchProcessor[OutboxEvent] = EventBatchProcessor(
        repo,
        pipeline,
        fetch,
        commit,  # type: ignore[arg-type]
    )

    # Act / Assert
    with pytest.raises(RuntimeError, match="db down"):
        await processor.process_batch(worker_id="w-1", batch_size=5)


async def test__processor__fetch_cancelled__propagates_without_logging() -> None:
    # Arrange
    repo = _Repo()
    fetch = _FetchFake(events=asyncio.CancelledError())
    commit = _CommitFake()
    pipeline = _PipelineSpy()
    processor: EventBatchProcessor[OutboxEvent] = EventBatchProcessor(
        repo,
        pipeline,
        fetch,
        commit,  # type: ignore[arg-type]
    )

    # Act / Assert
    with pytest.raises(asyncio.CancelledError):
        await processor.process_batch(worker_id="w-1", batch_size=5)
