"""Unit tests for ``omni_box.core.pipeline.context``."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.protocols import EventRepository

pytestmark = pytest.mark.unit


class _FakeRepo:
    """Minimal stand-in implementing nothing — only used as object identity."""


@pytest.fixture
def repo() -> EventRepository:
    return _FakeRepo()  # type: ignore[return-value]


def test__processing_context__default_init__has_empty_collections(repo: EventRepository) -> None:
    # Arrange / Act
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")

    # Assert
    assert ctx.repo is repo
    assert ctx.worker_id == "w1"
    assert ctx.metrics is None
    assert ctx.completed_ids == []
    assert ctx.failed_counted == []
    assert ctx.failed_noncounted == []
    assert ctx.skipped_ids == set()
    assert ctx.statuses == {}
    assert ctx.extra == {}


def test__mark_completed__without_status__appends_id_and_no_status_recorded(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()

    # Act
    ctx.mark_completed(event_id)

    # Assert
    assert ctx.completed_ids == [event_id]
    assert event_id not in ctx.statuses


def test__mark_completed__with_status__records_status(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()

    # Act
    ctx.mark_completed(event_id, status="completed")

    # Assert
    assert ctx.statuses[event_id] == "completed"


def test__mark_failed__count_as_attempt_true__goes_to_failed_counted(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()
    retry_at = datetime(2030, 1, 1, tzinfo=UTC)

    # Act
    ctx.mark_failed(event_id, "boom", count_as_attempt=True, next_retry_at=retry_at, status="failed")

    # Assert
    assert len(ctx.failed_counted) == 1
    assert ctx.failed_counted[0].event_id == event_id
    assert ctx.failed_counted[0].error == "boom"
    assert ctx.failed_counted[0].next_retry_at == retry_at
    assert ctx.failed_noncounted == []
    assert ctx.statuses[event_id] == "failed"


def test__mark_failed__count_as_attempt_false__goes_to_failed_noncounted(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()

    # Act
    ctx.mark_failed(event_id, "transient", count_as_attempt=False)

    # Assert
    assert ctx.failed_counted == []
    assert len(ctx.failed_noncounted) == 1
    assert ctx.failed_noncounted[0].event_id == event_id


def test__mark_failed__error_is_none__substitutes_unknown_error(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()

    # Act
    ctx.mark_failed(event_id, error=None)

    # Assert
    assert ctx.failed_counted[0].error == "unknown error"


def test__mark_skipped__with_status__records_id_and_status(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()

    # Act
    ctx.mark_skipped(event_id, reason="duplicate", status="skipped")

    # Assert
    assert event_id in ctx.skipped_ids
    assert ctx.statuses[event_id] == "skipped"


def test__mark_skipped__without_status__only_records_id(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()

    # Act
    ctx.mark_skipped(event_id, reason="duplicate")

    # Assert
    assert event_id in ctx.skipped_ids
    assert event_id not in ctx.statuses


def test__failed_ids__contains_union_of_counted_and_noncounted(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    counted_id = uuid4()
    noncounted_id = uuid4()
    ctx.mark_failed(counted_id, "x", count_as_attempt=True)
    ctx.mark_failed(noncounted_id, "y", count_as_attempt=False)

    # Act
    failed = ctx.failed_ids

    # Assert
    assert failed == {counted_id, noncounted_id}


def test__get_failure__counted_id__returns_failure_record(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()
    ctx.mark_failed(event_id, "err", count_as_attempt=True)

    # Act
    failure = ctx.get_failure(event_id)

    # Assert
    assert failure is not None
    assert failure.event_id == event_id


def test__get_failure__noncounted_id__returns_failure_record(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    event_id = uuid4()
    ctx.mark_failed(event_id, "err", count_as_attempt=False)

    # Act
    failure = ctx.get_failure(event_id)

    # Assert
    assert failure is not None
    assert failure.event_id == event_id


def test__get_failure__unknown_id__returns_none(repo: EventRepository) -> None:
    # Arrange
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")

    # Act
    failure = ctx.get_failure(uuid4())

    # Assert
    assert failure is None


def test__get_failure__nonmatching_counted_and_noncounted__returns_none(repo: EventRepository) -> None:
    # Arrange: both lists populated but no entry matches the queried id.
    # Covers the branch where get_failure iterates and finds nothing.
    ctx: ProcessingContext = ProcessingContext(repo=repo, worker_id="w1")
    ctx.mark_failed(uuid4(), "a", count_as_attempt=True)
    ctx.mark_failed(uuid4(), "b", count_as_attempt=False)

    # Act
    failure = ctx.get_failure(uuid4())

    # Assert
    assert failure is None
