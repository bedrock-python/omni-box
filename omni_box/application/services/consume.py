"""Inbox consumer runner with configurable commit semantics."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from time import perf_counter
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from ...core.constants import DEFAULT_PROCESS_TIMEOUT_SECONDS
from ...core.exceptions import InboxPersistError
from ...core.models.enums import EventStatus
from ...core.protocols import (
    AckHandle,
    ConsumedMessage,
    EventConsumer,
    InboxHandler,
    NullAckHandle,
)
from ...core.services.domain import OmniBoxDomainService
from ...core.services.metrics import NoOpInboxMetrics
from ...core.services.results import EventHandlerResult, coerce_handler_outcome

if TYPE_CHECKING:
    from ...core.models.entities import InboxEvent
    from ...core.protocols import InboxEventRepository
    from ...core.protocols.metrics import InboxMetrics
    from ...core.protocols.transaction import InboxTransactionProviderProtocol

logger = structlog.get_logger(__name__)


class AckStrategy(StrEnum):
    """Commit semantics for consumed broker messages."""

    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE_INBOX = "exactly_once_inbox"


class CommitOffsetPolicy(StrEnum):
    """When to commit (ACK) the broker message in AT_LEAST_ONCE mode."""

    ON_PERSIST = "on_persist"
    ON_SUCCESS = "on_success"


class _HandlerRetryRequestedError(Exception):
    """Rolls back the inbox transaction when a runner's handler returns a failure.

    The runner has no failure to record (it never calls ``mark_failed``), so a
    returned failure is handled as a raised one: the handler's writes and the
    inbox row roll back together and the broker redelivers the message.
    """

    def __init__(self, result: EventHandlerResult) -> None:
        super().__init__(result.error_message or "Handler requested a retry")
        self.result = result


@dataclass(frozen=True, slots=True)
class InboxConsumeResult:
    """Result of one inbox consume cycle."""

    message_id: str
    event_id: UUID | None
    committed: bool
    processed: bool
    duplicate: bool


class InboxMessageProcessor:
    """Logic for processing a single inbox message within a transaction."""

    def __init__(
        self,
        transaction_provider: InboxTransactionProviderProtocol,
        handler: InboxHandler | None = None,
        *,
        worker_id: str,
        consumer_group: str,
        domain_service: OmniBoxDomainService | None = None,
        process_timeout: float = DEFAULT_PROCESS_TIMEOUT_SECONDS,
    ) -> None:
        self._transaction_provider = transaction_provider
        self._handler = handler
        self._worker_id = worker_id
        self._consumer_group = consumer_group
        self._domain_service = domain_service or OmniBoxDomainService()
        self._process_timeout = process_timeout

    @property
    def has_handler(self) -> bool:
        return self._handler is not None

    def create_event(self, message: ConsumedMessage) -> InboxEvent:
        return self._domain_service.create_inbox_event(
            message_id=message.message_id,
            consumer_group=self._consumer_group,
            source=message.source,
            event_type=message.event_type,
            payload=message.payload,
            headers=message.headers,
            trace_id=message.trace_id,
            correlation_id=message.correlation_id,
            causation_id=message.causation_id,
            schema_version=message.schema_version,
        )

    async def process_in_transaction(
        self, event: InboxEvent
    ) -> tuple[InboxEvent | None, EventHandlerResult | None, Exception | None, float]:
        stored: InboxEvent | None = None
        handler_result: EventHandlerResult | None = None
        handler_error: Exception | None = None
        started_at: float = 0.0

        try:
            async with self._transaction_provider.transaction() as repo:
                stored = await repo.create(event)

                logger.info(
                    "Processing inbox event",
                    event_id=str(event.id),
                    message_id=event.message_id,
                    event_type=event.event_type,
                    source=event.source,
                )

                if stored and stored.status != EventStatus.COMPLETED and self._handler is not None:
                    # Only lock if we have a handler to process it now.
                    # Otherwise, leave it as PENDING for the batch processor.
                    if not await repo.mark_processing(stored.id, self._worker_id):
                        # The row is locked elsewhere (or no longer PENDING): running the
                        # handler here would duplicate work the lock holder is doing and
                        # then roll back on ``mark_completed``.
                        logger.info(
                            "Inbox event is not ours to process; leaving it to the lock holder",
                            event_id=str(stored.id),
                            message_id=stored.message_id,
                        )
                    else:
                        started_at = perf_counter()
                        try:
                            raw = await asyncio.wait_for(
                                self._handler(stored, repo),
                                timeout=self._process_timeout,
                            )
                            handler_result = coerce_handler_outcome(raw)
                            await self._settle(repo, stored, handler_result, started_at)
                        except Exception as exc:
                            handler_error = exc
                            raise
        except Exception as e:
            # Distinguish handler failures (already captured above) from
            # transactional/storage failures so the caller can choose how to
            # respond.  If ``handler_error`` is unset we are dealing with a
            # persistence-time error and should surface it explicitly.
            if handler_error is None:
                logger.exception(
                    "Failed to persist inbox event",
                    event_id=str(event.id),
                    message_id=event.message_id,
                )
            elif isinstance(e, _HandlerRetryRequestedError):
                logger.warning(
                    "Inbox handler requested a retry; transaction rolled back",
                    event_id=str(event.id),
                    message_id=event.message_id,
                    reason=str(e),
                )
            else:
                logger.exception(
                    "Inbox handler failed; transaction rolled back",
                    event_id=str(event.id),
                    message_id=event.message_id,
                )
            handler_error = e

        return stored, handler_result, handler_error, started_at

    async def _settle(
        self, repo: InboxEventRepository, stored: InboxEvent, result: EventHandlerResult, started_at: float
    ) -> None:
        """Complete the row on a success or a skip; on a returned failure, roll the transaction back.

        A skip is a decision about the message, so it finishes the row too: a
        redelivery is then a duplicate rather than a row left locked.
        """
        if result.processed and not result.success:
            raise _HandlerRetryRequestedError(result)
        await repo.mark_completed(stored.id, self._worker_id)
        logger.info(
            "Inbox event processed successfully" if result.processed else "Inbox event skipped by its handler",
            event_id=str(stored.id),
            message_id=stored.message_id,
            duration_seconds=round(perf_counter() - started_at, 4),
        )


class InboxConsumerRunner:
    """High-level service for consuming broker messages into the Transactional Inbox."""

    def __init__(
        self,
        consumer: EventConsumer,
        transaction_provider: InboxTransactionProviderProtocol,
        handler: InboxHandler | None = None,
        *,
        worker_id: str,
        consumer_group: str,
        domain_service: OmniBoxDomainService | None = None,
        ack_strategy: AckStrategy = AckStrategy.EXACTLY_ONCE_INBOX,
        commit_offset_policy: CommitOffsetPolicy = CommitOffsetPolicy.ON_PERSIST,
        exactly_once_commit_on_failed: bool = False,
        process_timeout: float = DEFAULT_PROCESS_TIMEOUT_SECONDS,
        concurrency_limit: int | None = None,
        metrics: InboxMetrics | None = None,
    ) -> None:
        self._consumer = consumer
        self._processor = InboxMessageProcessor(
            transaction_provider=transaction_provider,
            handler=handler,
            worker_id=worker_id,
            consumer_group=consumer_group,
            domain_service=domain_service,
            process_timeout=process_timeout,
        )
        self._ack_strategy = ack_strategy
        self._commit_offset_policy = commit_offset_policy
        self._exactly_once_commit_on_failed = exactly_once_commit_on_failed
        self._metrics = metrics or NoOpInboxMetrics()
        self._semaphore = asyncio.Semaphore(concurrency_limit) if concurrency_limit else None
        self._running = False
        self._lifecycle_lock = asyncio.Lock()
        # Backoff used while ``run_forever`` gets a message it cannot commit,
        # whether ``process_one`` raised or returned ``committed=False``. The
        # message was rewound, so the next read is the same message again; the
        # backoff spaces those retries out.
        self._run_forever_min_backoff = 0.1
        self._run_forever_max_backoff = 5.0

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._running:
                return
            await self._consumer.start()
            self._running = True

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if not self._running:
                return
            self._running = False
            await self._consumer.stop()

    async def run_forever(self) -> None:
        """Process messages until ``stop()``.

        A message that is not committed is rewound by ``process_one`` and comes
        back on the next read, so a handler that keeps failing holds its partition
        rather than being read past. The loop backs off exponentially, from 0.1 s
        to 5 s, for as long as messages are not committed.
        """
        if not self._running:
            raise RuntimeError("Runner is not started.")
        backoff = self._run_forever_min_backoff
        while self._running:
            try:
                result = await self.process_one()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("InboxConsumerRunner.process_one failed; backing off")
            else:
                if result.committed:
                    backoff = self._run_forever_min_backoff
                    continue
                logger.warning(
                    "Inbox message not committed; it will be redelivered after a backoff",
                    message_id=result.message_id,
                    backoff_seconds=backoff,
                )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._run_forever_max_backoff)

    async def process_one(self) -> InboxConsumeResult:
        """Read one message, process it and commit it according to ``ack_strategy``.

        A message that is not committed — the result says ``committed=False``, or
        this method raises after reading it — is rewound through
        ``AckHandle.rewind()``, so the next call returns the same message. A
        caller driving this method in its own loop should back off before calling
        it again after such a result, as ``run_forever`` does.
        """
        if self._semaphore:
            async with self._semaphore:
                return await self._process_one_internal()
        return await self._process_one_internal()

    async def _process_one_internal(self) -> InboxConsumeResult:
        message = await self._consumer.getone()
        self._metrics.inc_consumed()
        ack_handle = message.ack_handle or NullAckHandle()
        try:
            result = await self._process_message(message, ack_handle)
        except BaseException:
            await self._rewind(message, ack_handle)
            raise
        if not result.committed:
            await self._rewind(message, ack_handle)
        return result

    async def _rewind(self, message: ConsumedMessage, ack_handle: AckHandle) -> None:
        """Put an uncommitted message back, without masking the outcome that led here."""
        try:
            await ack_handle.rewind()
        except Exception:
            logger.exception("Failed to rewind an uncommitted inbox message", message_id=message.message_id)

    async def _process_message(self, message: ConsumedMessage, ack_handle: AckHandle) -> InboxConsumeResult:
        committed = False

        if self._ack_strategy == AckStrategy.AT_MOST_ONCE:
            await self._commit_with_metrics(ack_handle)
            committed = True

        event = self._processor.create_event(message)
        stored, handler_result, handler_error, started_at = await self._processor.process_in_transaction(event)

        if stored is None:
            raise InboxPersistError(message_id=message.message_id, cause=handler_error)

        if stored.status == EventStatus.COMPLETED:
            return await self._handle_duplicate(message, stored, ack_handle, committed)

        if handler_error is not None:
            return await self._handle_handler_error(message, stored, handler_error, started_at, ack_handle, committed)

        if handler_result is not None:
            self._update_metrics_after_handler(message.event_type, handler_result, started_at)

        if self._should_commit_offset(handler_result, committed):
            await self._commit_with_metrics(ack_handle)
            committed = True

        return InboxConsumeResult(
            message_id=message.message_id,
            event_id=stored.id,
            committed=committed,
            processed=handler_result.processed if handler_result else False,
            duplicate=False,
        )

    async def _handle_duplicate(
        self, message: ConsumedMessage, stored: InboxEvent, ack_handle: AckHandle, committed: bool
    ) -> InboxConsumeResult:
        logger.info(
            "Inbox event already processed (duplicate)",
            event_id=str(stored.id),
            message_id=message.message_id,
            event_type=stored.event_type,
        )
        # Whatever the strategy, the message is done: left uncommitted it would be
        # rewound and redelivered forever.
        if not committed:
            await self._commit_with_metrics(ack_handle)
            committed = True
        self._metrics.inc_duplicate(event_type=stored.event_type)
        return InboxConsumeResult(
            message_id=message.message_id,
            event_id=stored.id,
            committed=committed,
            processed=False,
            duplicate=True,
        )

    async def _handle_handler_error(
        self,
        message: ConsumedMessage,
        stored: InboxEvent,
        error: Exception,
        started_at: float,
        ack_handle: AckHandle,
        committed: bool,
    ) -> InboxConsumeResult:
        if started_at > 0:
            self._metrics.observe_handler_duration(perf_counter() - started_at, event_type=stored.event_type)
        self._metrics.inc_failed(event_type=stored.event_type)

        logger.error(
            "Inbox message handler failed", event_id=str(stored.id), message_id=message.message_id, error=str(error)
        )

        if (
            self._ack_strategy == AckStrategy.AT_LEAST_ONCE
            and self._commit_offset_policy == CommitOffsetPolicy.ON_PERSIST
            and not committed
        ):
            await self._commit_with_metrics(ack_handle)
            committed = True
        if self._ack_strategy == AckStrategy.EXACTLY_ONCE_INBOX and self._exactly_once_commit_on_failed:
            await self._commit_with_metrics(ack_handle)
            committed = True

        return InboxConsumeResult(
            message_id=message.message_id,
            event_id=stored.id,
            committed=committed,
            processed=False,
            duplicate=False,
        )

    def _update_metrics_after_handler(
        self, event_type: str, handler_result: EventHandlerResult, started_at: float
    ) -> None:
        self._metrics.observe_handler_duration(perf_counter() - started_at, event_type=event_type)
        if handler_result.processed:
            if handler_result.success:
                self._metrics.inc_processed(event_type=event_type)
            else:
                self._metrics.inc_failed(event_type=event_type)

    def _should_commit_offset(self, handler_result: EventHandlerResult | None, committed: bool) -> bool:
        if committed:
            return False

        if self._ack_strategy == AckStrategy.AT_LEAST_ONCE:
            if self._commit_offset_policy == CommitOffsetPolicy.ON_PERSIST:
                return True
            if self._commit_offset_policy == CommitOffsetPolicy.ON_SUCCESS:
                return handler_result is None or not handler_result.processed or handler_result.success

        elif self._ack_strategy == AckStrategy.EXACTLY_ONCE_INBOX:
            if not self._processor.has_handler:
                return True
            elif handler_result is None:
                return False
            elif not handler_result.processed or handler_result.success:
                return True

        return False

    async def _commit_with_metrics(self, ack_handle: AckHandle) -> None:
        try:
            await ack_handle.commit()
        except Exception:
            self._metrics.inc_commit_failed()
            raise
        self._metrics.inc_committed()
