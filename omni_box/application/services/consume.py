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
                if stored and stored.status != EventStatus.COMPLETED and self._handler is not None:
                    # Only lock if we have a handler to process it now.
                    # Otherwise, leave it as PENDING for the batch processor.
                    await repo.mark_processing(stored.id, self._worker_id)

                logger.info(
                    "Processing inbox event",
                    event_id=str(event.id),
                    message_id=event.message_id,
                    event_type=event.event_type,
                    source=event.source,
                )

                if stored and stored.status != EventStatus.COMPLETED and self._handler is not None:
                    started_at = perf_counter()
                    try:
                        raw = await asyncio.wait_for(
                            self._handler(stored, repo),
                            timeout=self._process_timeout,
                        )
                        handler_result = coerce_handler_outcome(raw)
                        if handler_result.success:
                            await repo.mark_completed(stored.id, self._worker_id)
                            logger.info(
                                "Inbox event processed successfully",
                                event_id=str(stored.id),
                                message_id=stored.message_id,
                                duration_seconds=round(perf_counter() - started_at, 4),
                            )
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
            else:
                logger.exception(
                    "Inbox handler failed; transaction rolled back",
                    event_id=str(event.id),
                    message_id=event.message_id,
                )
            handler_error = e

        return stored, handler_result, handler_error, started_at


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
        # Backoff used when ``process_one`` raises while ``run_forever`` is
        # active. Caps the loop's retry rate so an external outage can't burn
        # CPU.
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
        if not self._running:
            raise RuntimeError("Runner is not started.")
        backoff = self._run_forever_min_backoff
        while self._running:
            try:
                await self.process_one()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("InboxConsumerRunner.process_one failed; backing off")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._run_forever_max_backoff)
            else:
                backoff = self._run_forever_min_backoff

    async def process_one(self) -> InboxConsumeResult:
        if self._semaphore:
            async with self._semaphore:
                return await self._process_one_internal()
        return await self._process_one_internal()

    async def _process_one_internal(self) -> InboxConsumeResult:
        message = await self._consumer.getone()
        self._metrics.inc_consumed()
        ack_handle = message.ack_handle or NullAckHandle()
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
        if self._ack_strategy == AckStrategy.EXACTLY_ONCE_INBOX and not committed:
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
                return handler_result is None or handler_result.processed

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
