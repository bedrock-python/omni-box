"""Unit tests for inbox consume runner."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from omni_box.application.services.consume import (
    AckStrategy,
    CommitOffsetPolicy,
    InboxConsumerRunner,
)
from omni_box.core.exceptions import InboxPersistError
from omni_box.core.models.entities import InboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.protocols import AckHandle, ConsumedMessage, EventConsumer
from omni_box.core.protocols.repository import InboxEventRepository
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol
from omni_box.core.services.results import EventHandlerResult
from omni_box.utils.datetime import utc_now

pytestmark = pytest.mark.unit


class FakeAckHandle(AckHandle):
    def __init__(self) -> None:
        self.commit_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1


class FakeConsumer(EventConsumer):
    def __init__(self, messages: list[ConsumedMessage]) -> None:
        self._messages = deque(messages)
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def getone(self) -> ConsumedMessage:
        return self._messages.popleft()


class FakeInboxRepo:
    def __init__(self) -> None:
        self.events: dict[UUID, InboxEvent] = {}
        self.by_key: dict[tuple[str, str], UUID] = {}

    async def create(self, event: InboxEvent) -> InboxEvent:
        key = (event.message_id, event.consumer_group)
        existing_id = self.by_key.get(key)
        if existing_id is not None:
            return self.events[existing_id]
        self.by_key[key] = event.id
        self.events[event.id] = event
        return event

    async def get_by_id(self, event_id: UUID) -> InboxEvent | None:
        return self.events.get(event_id)

    async def fetch_pending(self, limit: int) -> list[InboxEvent]:
        return [e for e in self.events.values() if e.status == EventStatus.PENDING][:limit]

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        event = self.events[event_id]
        if event.status != EventStatus.PENDING or event.locked_at is not None:
            return False
        self.events[event_id] = event.model_copy(update={"locked_by": worker_id, "locked_at": utc_now()})
        return True

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        event = self.events[event_id]
        self.events[event_id] = event.model_copy(
            update={
                "status": EventStatus.COMPLETED,
                "completed_at": utc_now(),
                "locked_by": None,
                "locked_at": None,
            }
        )

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None,
        count_as_attempt: bool = True,
    ) -> None:
        event = self.events[event_id]
        self.events[event_id] = event.model_copy(
            update={
                "attempts_made": event.attempts_made + 1 if count_as_attempt else event.attempts_made,
                "last_error": error,
                "scheduled_at": next_retry_at or event.scheduled_at,
                "locked_by": None,
                "locked_at": None,
            }
        )

    async def get_by_message_id(self, message_id: str, consumer_group: str) -> InboxEvent | None:
        event_id = self.by_key.get((message_id, consumer_group))
        return self.events.get(event_id) if event_id is not None else None

    async def exists(self, message_id: str, consumer_group: str) -> bool:
        return (message_id, consumer_group) in self.by_key

    async def has_completed_sibling_for_inbox_key(
        self, message_id: str, consumer_group: str, exclude_event_id: UUID
    ) -> bool:
        return any(
            e.message_id == message_id
            and e.consumer_group == consumer_group
            and e.id != exclude_event_id
            and e.status == EventStatus.COMPLETED
            for e in self.events.values()
        )


class FakeTransactionProvider(InboxTransactionProviderProtocol):
    def __init__(self, repo: InboxEventRepository) -> None:
        self.repo = repo
        self.transaction_calls = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_calls += 1
        yield self.repo


def _message(message_id: str, ack_handle: FakeAckHandle) -> ConsumedMessage:
    return ConsumedMessage(
        message_id=message_id,
        source="identity-service",
        event_type="user.created",
        payload={"user_id": "u-1"},
        headers={"event_type": "user.created"},
        ack_handle=ack_handle,
    )


@pytest.mark.asyncio
async def test__inbox_consumer_runner__exactly_once_success__commits_only_after_handler_success() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()
    handled: list[str] = []

    async def handler(event: InboxEvent, repo: InboxEventRepository, **dependencies: Any) -> EventHandlerResult:
        handled.append(event.message_id)
        await repo.mark_processing(event.id, "w-1")
        await repo.mark_completed(event.id, "w-1")
        return EventHandlerResult(processed=True, success=True)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=handler,  # type: ignore[arg-type]
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.processed is True
    assert ack.commit_calls == 1
    assert handled == ["m-1"]


@pytest.mark.asyncio
async def test__inbox_consumer_runner__at_most_once__commits_before_db() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()
    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.AT_MOST_ONCE,
    )

    # Act
    await runner.start()
    try:
        await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert ack.commit_calls == 1


@pytest.mark.asyncio
async def test__inbox_consumer_runner__at_least_once_on_success_policy__commits_after_success() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()

    async def handler(event: InboxEvent, repo: InboxEventRepository, **dependencies: Any) -> EventHandlerResult:
        return EventHandlerResult(processed=True, success=True)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=handler,  # type: ignore[arg-type]
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_SUCCESS,
    )

    # Act
    await runner.start()
    try:
        await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert ack.commit_calls == 1


@pytest.mark.asyncio
async def test__inbox_consumer_runner__at_least_once_on_success_policy_failure__still_commits() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()

    async def handler(event: InboxEvent, repo: InboxEventRepository, **dependencies: Any) -> EventHandlerResult:
        return EventHandlerResult(processed=True, success=False)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=handler,  # type: ignore[arg-type]
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_SUCCESS,
    )

    # Act
    await runner.start()
    try:
        await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert ack.commit_calls == 1


@pytest.mark.asyncio
async def test__inbox_consumer_runner__exactly_once_duplicate__marks_duplicate_and_commits() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()

    msg = _message("m-1", ack)
    now = utc_now()
    event = InboxEvent(
        message_id=msg.message_id,
        consumer_group="cg-1",
        source=msg.source,
        event_type=msg.event_type,
        payload=msg.payload,
        status=EventStatus.COMPLETED,
        created_at=now - timedelta(seconds=10),
        scheduled_at=now - timedelta(seconds=10),
        completed_at=now,
    )
    await repo.create(event)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.duplicate is True
    assert ack.commit_calls == 1


@pytest.mark.asyncio
async def test__inbox_consumer_runner__handler_timeout__returns_not_processed() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()

    async def slow_handler(event: InboxEvent, repo: InboxEventRepository, **dependencies: Any) -> EventHandlerResult:
        await asyncio.sleep(0.2)
        return EventHandlerResult(processed=True, success=True)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=slow_handler,
        worker_id="w-1",
        consumer_group="cg-1",
        process_timeout=0.1,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.processed is False
    assert ack.commit_calls == 0


@pytest.mark.asyncio
async def test__inbox_consumer_runner__db_failure_on_create__raises_inbox_persist_error() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])

    class FailingRepo:
        async def create(self, event):
            return None

    tx_provider = MagicMock(spec=InboxTransactionProviderProtocol)
    tx_provider.transaction.return_value.__aenter__.return_value = FailingRepo()

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        worker_id="w-1",
        consumer_group="cg-1",
    )

    # Act / Assert
    await runner.start()
    with pytest.raises(InboxPersistError, match="Failed to persist inbox event"):
        await runner.process_one()


@pytest.mark.asyncio
async def test__inbox_consumer_runner__exactly_once_handler_failure__does_not_commit() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-2", ack)])
    repo = FakeInboxRepo()

    async def handler(_event: InboxEvent, repo: InboxEventRepository) -> EventHandlerResult:
        await repo.mark_processing(_event.id, "w-1")
        return EventHandlerResult(processed=True, success=False)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=handler,  # type: ignore[arg-type]
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.processed is True
    assert ack.commit_calls == 0


@pytest.mark.asyncio
async def test__inbox_consumer_runner__at_least_once_on_persist__commits_before_handler() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-3", ack)])
    repo = FakeInboxRepo()

    async def handler(_event: InboxEvent, repo: InboxEventRepository) -> EventHandlerResult:
        await repo.mark_processing(_event.id, "w-1")
        return EventHandlerResult(processed=True, success=False)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=handler,  # type: ignore[arg-type]
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_PERSIST,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.processed is True
    assert ack.commit_calls == 1


@pytest.mark.asyncio
async def test__inbox_consumer_runner__multiple_messages__calls_transaction_per_message() -> None:
    # Arrange
    ack1 = FakeAckHandle()
    ack2 = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack1), _message("m-2", ack2)])

    repo = FakeInboxRepo()
    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=None,
        worker_id="w-1",
        consumer_group="cg-1",
    )

    # Act
    await runner.start()
    try:
        await runner.process_one()
        await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert tx_provider.transaction_calls == 2
    assert len(repo.events) == 2
    message_ids = [e.message_id for e in repo.events.values()]
    assert "m-1" in message_ids
    assert "m-2" in message_ids


@pytest.mark.asyncio
async def test__inbox_consumer_runner__no_handler__persists_event_and_commits() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()
    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=None,
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.processed is False
    assert ack.commit_calls == 1
    assert len(repo.events) == 1
    event = next(iter(repo.events.values()))
    assert event.message_id == "m-1"
    assert event.status == EventStatus.PENDING


@pytest.mark.asyncio
async def test__inbox_consumer_runner__handler_returns_not_processed__event_stays_pending() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()

    async def handler(_event: InboxEvent, _repo: InboxEventRepository) -> EventHandlerResult:
        return EventHandlerResult(processed=False, success=False)

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=handler,  # type: ignore[arg-type]
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.processed is False
    assert ack.commit_calls == 1
    event = next(iter(repo.events.values()))
    assert event.status == EventStatus.PENDING


@pytest.mark.asyncio
async def test__inbox_consumer_runner__exactly_once_commit_on_failed__handler_raises_still_commits() -> None:
    # Arrange
    ack = FakeAckHandle()
    consumer = FakeConsumer([_message("m-1", ack)])
    repo = FakeInboxRepo()

    async def handler(_event: InboxEvent, _repo: InboxEventRepository) -> EventHandlerResult:
        raise RuntimeError("oops")

    tx_provider = FakeTransactionProvider(repo)  # type: ignore

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=tx_provider,
        handler=handler,  # type: ignore[arg-type]
        worker_id="w-1",
        consumer_group="cg-1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        exactly_once_commit_on_failed=True,
    )

    # Act
    await runner.start()
    try:
        result = await runner.process_one()
    finally:
        await runner.stop()

    # Assert
    assert result.processed is False
    assert ack.commit_calls == 1
