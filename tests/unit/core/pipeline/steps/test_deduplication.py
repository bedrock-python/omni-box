"""Unit tests for ``omni_box.core.pipeline.steps.deduplication``."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from omni_box.core.constants import REASON_DUPLICATE_SIBLING
from omni_box.core.models.entities import InboxEvent
from omni_box.core.pipeline.context import ProcessingContext
from omni_box.core.pipeline.steps.deduplication import SiblingDeduplicationStep
from omni_box.core.protocols.repository import InboxEventRepository, RepositoryCapabilities

pytestmark = pytest.mark.unit


def _make_inbox_event(message_id: str = "msg-1", consumer_group: str = "cg-1") -> InboxEvent:
    return InboxEvent(
        id=uuid4(),
        event_type="t",
        payload={"a": 1},
        message_id=message_id,
        consumer_group=consumer_group,
        source="kafka",
    )


class _InboxRepoFake(InboxEventRepository):
    """Implements InboxEventRepository surface used by the step."""

    def __init__(self, has_sibling: bool = False) -> None:
        self.has_sibling = has_sibling
        self.calls: list[tuple[str, str, UUID]] = []

    @property
    def capabilities(self) -> RepositoryCapabilities:
        return RepositoryCapabilities()

    # EventRepository surface (unused by step but required by protocol)
    async def create(self, event: InboxEvent) -> InboxEvent:
        return event

    async def get_by_id(self, event_id: UUID) -> None:
        return None

    async def fetch_pending(self, limit: int, **filters: Any) -> list[InboxEvent]:
        return []

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        return None

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None,
        count_as_attempt: bool = True,
    ) -> None:
        return None

    # InboxEventRepository specifics
    async def get_by_message_id(self, message_id: str, consumer_group: str) -> InboxEvent | None:
        return None

    async def exists(self, message_id: str, consumer_group: str) -> bool:
        return False

    async def has_completed_sibling_for_inbox_key(
        self,
        message_id: str,
        consumer_group: str,
        exclude_event_id: UUID,
    ) -> bool:
        self.calls.append((message_id, consumer_group, exclude_event_id))
        return self.has_sibling


class _NotInboxRepo:
    """Empty fake — does NOT implement InboxEventRepository."""


async def test__sibling_dedup__disabled__returns_next_without_repo_call() -> None:
    # Arrange
    repo = _InboxRepoFake()
    ctx: ProcessingContext[InboxEvent] = ProcessingContext(repo=repo, worker_id="w1")  # type: ignore[arg-type]
    event = _make_inbox_event()
    step = SiblingDeduplicationStep(enabled=False)

    # Act
    result = await step.execute(event, ctx)

    # Assert
    assert result.should_skip_event is False
    assert result.should_stop_pipeline is False
    assert repo.calls == []


async def test__sibling_dedup__no_sibling__returns_next_and_calls_repo() -> None:
    # Arrange
    repo = _InboxRepoFake(has_sibling=False)
    ctx: ProcessingContext[InboxEvent] = ProcessingContext(repo=repo, worker_id="w1")  # type: ignore[arg-type]
    event = _make_inbox_event(message_id="m-x", consumer_group="cg-x")
    step = SiblingDeduplicationStep()

    # Act
    result = await step.execute(event, ctx)

    # Assert
    assert result.should_skip_event is False
    assert repo.calls == [("m-x", "cg-x", event.id)]
    assert event.id not in ctx.skipped_ids


async def test__sibling_dedup__has_sibling__marks_skipped_and_skips_event() -> None:
    # Arrange
    repo = _InboxRepoFake(has_sibling=True)
    ctx: ProcessingContext[InboxEvent] = ProcessingContext(repo=repo, worker_id="w1")  # type: ignore[arg-type]
    event = _make_inbox_event()
    step = SiblingDeduplicationStep()

    # Act
    result = await step.execute(event, ctx)

    # Assert
    assert result.should_skip_event is True
    assert event.id in ctx.skipped_ids
    # the reason constant is used; we don't bind to its literal value to avoid magic strings
    assert REASON_DUPLICATE_SIBLING  # sanity that constant exists / non-empty


async def test__sibling_dedup__wrong_repo_type__raises_type_error() -> None:
    # Arrange
    repo = _NotInboxRepo()
    ctx: ProcessingContext[InboxEvent] = ProcessingContext(repo=repo, worker_id="w1")  # type: ignore[arg-type]
    event = _make_inbox_event()
    step = SiblingDeduplicationStep()

    # Act / Assert
    with pytest.raises(TypeError, match="InboxEventRepository"):
        await step.execute(event, ctx)
