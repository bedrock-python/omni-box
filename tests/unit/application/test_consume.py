"""Unit tests for ``omni_box.application.services.consume``."""

from __future__ import annotations

import dataclasses
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest

from omni_box.application.services.consume import (
    AckStrategy,
    CommitOffsetPolicy,
    InboxConsumeResult,
    InboxConsumerRunner,
    InboxMessageProcessor,
)
from omni_box.core.exceptions import InboxPersistError
from omni_box.core.models.entities import InboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.protocols import (
    AckHandle,
    ConsumedMessage,
    EventConsumer,
    InboxEventRepository,
    InboxTransactionProviderProtocol,
    NullAckHandle,
)
from omni_box.core.services.results import (
    EventHandlerResult,
    handler_completed,
    handler_retry,
    handler_skipped,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.unit


# -------- fakes --------


class _FakeAckHandle(AckHandle):
    def __init__(self, fail: bool = False) -> None:
        self.commit_count: int = 0
        self.fail: bool = fail

    async def commit(self) -> None:
        self.commit_count += 1
        if self.fail:
            raise RuntimeError("commit boom")


class _FakeInboxRepo:
    """Minimal in-memory inbox repository satisfying ``InboxEventRepository``."""

    def __init__(
        self,
        *,
        create_returns_completed: bool = False,
        create_raises: BaseException | None = None,
    ) -> None:
        self._create_returns_completed = create_returns_completed
        self._create_raises = create_raises
        self.created: list[InboxEvent] = []
        self.processing: list[tuple[UUID, str]] = []
        self.completed: list[tuple[UUID, str]] = []

    async def create(self, event: InboxEvent) -> InboxEvent:
        if self._create_raises is not None:
            raise self._create_raises
        self.created.append(event)
        if self._create_returns_completed:
            return event.model_copy(update={"status": EventStatus.COMPLETED})
        return event

    async def get_by_id(self, event_id: UUID) -> InboxEvent | None:
        return None

    async def get_by_message_id(self, message_id: str, consumer_group: str) -> InboxEvent | None:
        return None

    async def exists(self, message_id: str, consumer_group: str) -> bool:
        return False

    async def has_completed_sibling_for_inbox_key(
        self, message_id: str, consumer_group: str, exclude_event_id: UUID
    ) -> bool:
        return False

    async def fetch_pending(self, limit: int, **filters: Any) -> list[InboxEvent]:
        return []

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        self.processing.append((event_id, worker_id))
        return True

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        self.completed.append((event_id, worker_id))

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None,
        count_as_attempt: bool = True,
    ) -> None:
        return


class _FakeTxProvider(InboxTransactionProviderProtocol):
    def __init__(self, repo: _FakeInboxRepo) -> None:
        self._repo = repo
        self.opened: int = 0

    def transaction(self) -> Any:
        @asynccontextmanager
        async def _cm() -> AsyncIterator[_FakeInboxRepo]:
            self.opened += 1
            yield self._repo

        return _cm()


class _FakeConsumer(EventConsumer):
    def __init__(self, messages: list[ConsumedMessage] | None = None) -> None:
        self._messages = list(messages or [])
        self.started: int = 0
        self.stopped: int = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def getone(self) -> ConsumedMessage:
        return self._messages.pop(0)


class _FakeInboxMetrics:
    def __init__(self) -> None:
        self.consumed: int = 0
        self.duplicates: list[str | None] = []
        self.processed: list[str | None] = []
        self.failed: list[str | None] = []
        self.committed: int = 0
        self.commit_failed: int = 0
        self.handler_durations: list[tuple[float, str | None]] = []

    def inc_consumed(self, count: int = 1) -> None:
        self.consumed += count

    def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self.duplicates.append(event_type)

    def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self.processed.append(event_type)

    def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self.failed.append(event_type)

    def inc_committed(self, count: int = 1) -> None:
        self.committed += count

    def inc_commit_failed(self, count: int = 1) -> None:
        self.commit_failed += 1

    def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
        self.handler_durations.append((seconds, event_type))


def _make_message(
    message_id: str = "m1",
    *,
    ack_handle: AckHandle | None = None,
    event_type: str = "ev1",
) -> ConsumedMessage:
    return ConsumedMessage(
        message_id=message_id,
        source="src1",
        event_type=event_type,
        payload={"p": 1},
        ack_handle=ack_handle,
    )


# -------- InboxMessageProcessor --------


def test__inbox_message_processor__create_event__copies_message_fields() -> None:
    # Arrange
    repo = _FakeInboxRepo()
    processor = InboxMessageProcessor(transaction_provider=_FakeTxProvider(repo), worker_id="w1", consumer_group="cg1")
    msg = _make_message(message_id="m1")

    # Act
    event = processor.create_event(msg)

    # Assert
    assert event.message_id == "m1"
    assert event.consumer_group == "cg1"
    assert event.source == "src1"
    assert event.event_type == "ev1"
    assert event.payload == {"p": 1}


async def test__inbox_message_processor__handler_success__marks_completed_and_returns_result() -> None:
    # Arrange
    repo = _FakeInboxRepo()
    handler_result = handler_completed()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_result

    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
    )
    event = processor.create_event(_make_message())

    # Act
    stored, result, error, started_at = await processor.process_in_transaction(event)

    # Assert
    assert stored is not None
    assert result is handler_result
    assert error is None
    assert started_at > 0
    assert repo.processing == [(event.id, "w1")]
    assert repo.completed == [(event.id, "w1")]


async def test__inbox_message_processor__handler_returns_none__coerced_and_marks_completed() -> None:
    # Arrange
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> None:
        return None

    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
    )
    event = processor.create_event(_make_message())

    # Act
    stored, result, error, _ = await processor.process_in_transaction(event)

    # Assert
    assert stored is not None
    assert result is not None and result.success is True
    assert error is None
    assert repo.completed == [(event.id, "w1")]


async def test__inbox_message_processor__handler_returns_failure__does_not_mark_completed() -> None:
    # Arrange
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_retry("nope")

    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
    )
    event = processor.create_event(_make_message())

    # Act
    stored, result, error, _ = await processor.process_in_transaction(event)

    # Assert
    assert stored is not None
    assert result is not None and result.success is False
    assert error is None
    assert repo.completed == []


async def test__inbox_message_processor__handler_raises__error_captured_and_no_completion() -> None:
    # Arrange
    repo = _FakeInboxRepo()
    boom = RuntimeError("handler boom")

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        raise boom

    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
    )
    event = processor.create_event(_make_message())

    # Act
    _stored, result, error, _ = await processor.process_in_transaction(event)

    # Assert
    assert error is boom
    assert result is None
    assert repo.completed == []


async def test__inbox_message_processor__handler_timeout__captured_as_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    repo = _FakeInboxRepo()

    async def fake_wait_for(coro: Any, timeout: float) -> Any:
        coro.close()
        raise TimeoutError("timeout")

    monkeypatch.setattr("omni_box.application.services.consume.asyncio.wait_for", fake_wait_for)

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_completed()

    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        process_timeout=0.001,
    )
    event = processor.create_event(_make_message())

    # Act
    _stored, result, error, _ = await processor.process_in_transaction(event)

    # Assert
    assert isinstance(error, TimeoutError)
    assert result is None


async def test__inbox_message_processor__no_handler__skips_lock_and_run() -> None:
    # Arrange
    repo = _FakeInboxRepo()
    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        handler=None,
        worker_id="w1",
        consumer_group="cg1",
    )
    event = processor.create_event(_make_message())

    # Act
    stored, result, error, started_at = await processor.process_in_transaction(event)

    # Assert
    assert stored is not None
    assert result is None
    assert error is None
    assert started_at == 0.0
    assert repo.processing == []
    assert repo.completed == []


async def test__inbox_message_processor__stored_already_completed__skips_handler_invocation() -> None:
    # Arrange
    repo = _FakeInboxRepo(create_returns_completed=True)
    called: list[bool] = []

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        called.append(True)
        return handler_completed()

    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
    )
    event = processor.create_event(_make_message())

    # Act
    stored, result, error, _ = await processor.process_in_transaction(event)

    # Assert
    assert stored is not None and stored.status == EventStatus.COMPLETED
    assert result is None
    assert error is None
    assert called == []
    assert repo.processing == []


async def test__inbox_message_processor__transaction_open_raises__captures_error() -> None:
    # Arrange
    boom = RuntimeError("db gone")
    repo = _FakeInboxRepo(create_raises=boom)
    processor = InboxMessageProcessor(
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
    )
    event = processor.create_event(_make_message())

    # Act
    stored, result, error, _ = await processor.process_in_transaction(event)

    # Assert
    assert stored is None
    assert result is None
    assert error is boom


# -------- InboxConsumerRunner lifecycle --------


async def test__inbox_consumer_runner__start_stop__lifecycle_idempotent() -> None:
    # Arrange
    consumer = _FakeConsumer()
    repo = _FakeInboxRepo()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
    )

    # Act
    await runner.start()
    await runner.start()  # second call: noop
    await runner.stop()
    await runner.stop()  # second call: noop

    # Assert
    assert consumer.started == 1
    assert consumer.stopped == 1


async def test__inbox_consumer_runner__run_forever_when_not_started__raises() -> None:
    # Arrange
    runner = InboxConsumerRunner(
        consumer=_FakeConsumer(),
        transaction_provider=_FakeTxProvider(_FakeInboxRepo()),
        worker_id="w1",
        consumer_group="cg1",
    )

    # Act / Assert
    with pytest.raises(RuntimeError, match="not started"):
        await runner.run_forever()


async def test__inbox_consumer_runner__run_forever_started__loops_until_stopped() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack), _make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_MOST_ONCE,
    )
    await runner.start()
    iterations: list[int] = []

    original = runner._process_one_internal

    async def stop_after_one() -> InboxConsumeResult:
        iterations.append(1)
        if len(iterations) >= 1:
            await runner.stop()
        return await original()

    runner._process_one_internal = stop_after_one  # type: ignore[method-assign]

    # Act
    await runner.run_forever()

    # Assert
    assert iterations == [1]


# -------- InboxConsumerRunner.process_one paths --------


async def test__inbox_consumer_runner__no_handler_exactly_once__commits_after_persist() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()
    metrics = _FakeInboxMetrics()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        metrics=metrics,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is True
    assert result.processed is False
    assert result.duplicate is False
    assert ack.commit_count == 1
    assert metrics.consumed == 1
    assert metrics.committed == 1


async def test__inbox_consumer_runner__at_most_once__commits_before_processing() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_completed()

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_MOST_ONCE,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is True
    assert ack.commit_count == 1


async def test__inbox_consumer_runner__exactly_once_handler_success__commits_after_handler() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack, event_type="ok")])
    repo = _FakeInboxRepo()
    metrics = _FakeInboxMetrics()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_completed()

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        metrics=metrics,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is True
    assert result.processed is True
    assert ack.commit_count == 1
    assert metrics.processed == ["ok"]


async def test__inbox_consumer_runner__exactly_once_handler_skipped__commits_because_not_processed() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_skipped()

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
    )

    # Act
    result = await runner.process_one()

    # Assert: skipped means handler explicitly declined; runner commits to avoid
    # redelivery of an event the application chose not to process.
    assert result.committed is True
    assert ack.commit_count == 1


async def test__inbox_consumer_runner__null_ack_handle_when_message_has_none__no_error() -> None:
    # Arrange
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=None)])
    repo = _FakeInboxRepo()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_MOST_ONCE,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is True
    # NullAckHandle was used and didn't raise
    assert isinstance(NullAckHandle(), AckHandle)


async def test__inbox_consumer_runner__duplicate_message__increments_duplicate_metric() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo(create_returns_completed=True)
    metrics = _FakeInboxMetrics()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        metrics=metrics,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.duplicate is True
    assert result.committed is True
    assert ack.commit_count == 1
    assert metrics.duplicates == ["ev1"]


async def test__inbox_consumer_runner__duplicate_under_at_most_once__not_double_committed() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo(create_returns_completed=True)
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_MOST_ONCE,
    )

    # Act
    result = await runner.process_one()

    # Assert: AT_MOST_ONCE already committed before persisting
    assert result.duplicate is True
    assert ack.commit_count == 1


async def test__inbox_consumer_runner__persist_returns_none__raises_inbox_persist_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
    )

    async def fake_process(event: InboxEvent) -> tuple[None, None, None, float]:
        return None, None, None, 0.0

    monkeypatch.setattr(runner._processor, "process_in_transaction", fake_process)

    # Act / Assert
    with pytest.raises(InboxPersistError, match="Failed to persist"):
        await runner.process_one()


async def test__inbox_consumer_runner__handler_error_exactly_once_default__does_not_commit() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack, event_type="err")])
    repo = _FakeInboxRepo()
    metrics = _FakeInboxMetrics()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        raise RuntimeError("boom")

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        metrics=metrics,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is False
    assert result.processed is False
    assert ack.commit_count == 0
    assert metrics.failed == ["err"]
    assert len(metrics.handler_durations) == 1


async def test__inbox_consumer_runner__handler_error_exactly_once_commit_on_failed__commits() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        raise RuntimeError("boom")

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        exactly_once_commit_on_failed=True,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is True
    assert ack.commit_count == 1


async def test__inbox_consumer_runner__mark_processing_raises__no_duration_observed() -> None:
    """started_at == 0 path inside ``_handle_handler_error``."""
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack, event_type="t")])
    repo = _FakeInboxRepo()
    boom = RuntimeError("lock failed")

    async def bad_mark_processing(event_id: UUID, worker_id: str) -> bool:
        raise boom

    repo.mark_processing = bad_mark_processing  # type: ignore[method-assign]

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_completed()

    metrics = _FakeInboxMetrics()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        metrics=metrics,
    )

    # Act
    result = await runner.process_one()

    # Assert: failure metric incremented but no handler-duration sample
    assert result.committed is False
    assert metrics.failed == ["t"]
    assert metrics.handler_durations == []


async def test__inbox_consumer_runner__handler_error_at_least_once_on_persist__commits_on_error() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        raise RuntimeError("boom")

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_PERSIST,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is True
    assert ack.commit_count == 1


async def test__inbox_consumer_runner__handler_error_at_least_once_on_success__does_not_commit() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        raise RuntimeError("boom")

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_SUCCESS,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is False
    assert ack.commit_count == 0


@pytest.mark.parametrize(
    ("strategy", "policy", "result_factory", "expected_committed"),
    [
        (
            AckStrategy.AT_LEAST_ONCE,
            CommitOffsetPolicy.ON_PERSIST,
            handler_completed,
            True,
        ),
        (
            AckStrategy.AT_LEAST_ONCE,
            CommitOffsetPolicy.ON_SUCCESS,
            handler_completed,
            True,
        ),
        (
            AckStrategy.AT_LEAST_ONCE,
            CommitOffsetPolicy.ON_SUCCESS,
            handler_skipped,
            False,
        ),
    ],
    ids=[
        "at-least-once-on-persist-completed-commits",
        "at-least-once-on-success-completed-commits",
        "at-least-once-on-success-skipped-no-commit",
    ],
)
async def test__inbox_consumer_runner__at_least_once_paths__commit_decision(
    strategy: AckStrategy,
    policy: CommitOffsetPolicy,
    result_factory: Any,
    expected_committed: bool,
) -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return result_factory()

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=strategy,
        commit_offset_policy=policy,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is expected_committed


async def test__inbox_consumer_runner__commit_raises__increments_commit_failed_and_reraises() -> None:
    # Arrange
    ack = _FakeAckHandle(fail=True)
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()
    metrics = _FakeInboxMetrics()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_MOST_ONCE,
        metrics=metrics,
    )

    # Act / Assert
    with pytest.raises(RuntimeError, match="commit boom"):
        await runner.process_one()
    assert metrics.commit_failed == 1
    assert metrics.committed == 0


async def test__inbox_consumer_runner__concurrency_limit__semaphore_path() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack)])
    repo = _FakeInboxRepo()
    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.AT_MOST_ONCE,
        concurrency_limit=1,
    )

    # Act
    result = await runner.process_one()

    # Assert
    assert result.committed is True


async def test__inbox_consumer_runner__handler_returns_failed_processed__updates_failed_metric() -> None:
    # Arrange
    ack = _FakeAckHandle()
    consumer = _FakeConsumer(messages=[_make_message(ack_handle=ack, event_type="t")])
    repo = _FakeInboxRepo()
    metrics = _FakeInboxMetrics()

    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return EventHandlerResult(success=False, processed=True)

    runner = InboxConsumerRunner(
        consumer=consumer,
        transaction_provider=_FakeTxProvider(repo),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
        metrics=metrics,
    )

    # Act
    result = await runner.process_one()

    # Assert
    # processed=True but success=False -> not processed/success branch False; commit decision:
    # handler_result is not None, not handler_result.processed=False, handler_result.success=False ->
    # condition `not processed or success` is False or False -> False => no commit
    assert result.committed is False
    assert metrics.failed == ["t"]


# -------- _should_commit_offset internals --------


def _make_runner(
    *,
    handler: Any = None,
    ack_strategy: AckStrategy = AckStrategy.EXACTLY_ONCE_INBOX,
    commit_offset_policy: CommitOffsetPolicy = CommitOffsetPolicy.ON_PERSIST,
) -> InboxConsumerRunner:
    return InboxConsumerRunner(
        consumer=_FakeConsumer(),
        transaction_provider=_FakeTxProvider(_FakeInboxRepo()),
        handler=handler,
        worker_id="w1",
        consumer_group="cg1",
        ack_strategy=ack_strategy,
        commit_offset_policy=commit_offset_policy,
    )


def test__should_commit_offset__already_committed__returns_false() -> None:
    # Arrange
    runner = _make_runner(ack_strategy=AckStrategy.AT_MOST_ONCE)

    # Act / Assert
    assert runner._should_commit_offset(handler_completed(), True) is False


def test__should_commit_offset__at_least_once_on_persist__always_true() -> None:
    # Arrange
    runner = _make_runner(
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_PERSIST,
    )

    # Act / Assert
    assert runner._should_commit_offset(handler_completed(), False) is True


def test__should_commit_offset__at_least_once_on_success_no_result__commits() -> None:
    # Arrange
    runner = _make_runner(
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_SUCCESS,
    )

    # Act / Assert
    assert runner._should_commit_offset(None, False) is True


def test__should_commit_offset__exactly_once_handler_present_result_none__returns_false() -> None:
    # Arrange
    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_completed()

    runner = _make_runner(handler=handler, ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX)

    # Act / Assert
    assert runner._should_commit_offset(None, False) is False


def test__should_commit_offset__at_least_once_unknown_policy__falls_through_to_false() -> None:
    """Exercise the implicit final-return branch when policy is neither ON_PERSIST nor ON_SUCCESS."""
    # Arrange
    runner = _make_runner(
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_PERSIST,
    )
    runner._commit_offset_policy = "__unknown__"  # type: ignore[assignment]

    # Act / Assert
    assert runner._should_commit_offset(handler_completed(), False) is False


def test__should_commit_offset__unknown_strategy__falls_through_to_false() -> None:
    """Exercise the implicit final-return branch when strategy is unrecognized."""
    # Arrange
    runner = _make_runner(ack_strategy=AckStrategy.AT_LEAST_ONCE)
    runner._ack_strategy = "__unknown__"  # type: ignore[assignment]

    # Act / Assert
    assert runner._should_commit_offset(handler_completed(), False) is False


def test__should_commit_offset__exactly_once_handler_processed_failure__returns_false() -> None:
    # Arrange
    async def handler(event: InboxEvent, r: InboxEventRepository) -> EventHandlerResult:
        return handler_completed()

    runner = _make_runner(handler=handler, ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX)
    failure = EventHandlerResult(success=False, processed=True)

    # Act / Assert
    assert runner._should_commit_offset(failure, False) is False


# -------- InboxConsumeResult dataclass --------


def test__inbox_consume_result__frozen_dataclass__is_immutable() -> None:
    # Arrange
    res = InboxConsumeResult(message_id="m1", event_id=uuid4(), committed=True, processed=False, duplicate=True)

    # Act / Assert
    with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
        res.committed = False  # type: ignore[misc]
