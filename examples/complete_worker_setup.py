"""End-to-end inbox worker setup with deduplication, DLQ, OTel and metrics.

This example wires the canonical inbox consumer pipeline:

1. A concrete ORM model bound to a ``DeclarativeBase``.
2. An ``EventRouter`` with handler classes.
3. A dispatching processor for batch processing.
4. An ``InboxConsumerRunner`` driving a broker consumer message-by-message,
   each message landing in its own DB transaction.

A trivial in-memory ``EventConsumer`` and ``InboxTransactionProviderProtocol``
implementation are provided so the script self-terminates after producing one
fake message. Replace them with your real Kafka consumer (e.g.
``omni_box.infra.brokers.kafka``) and your application's UoW provider.

Run with: ``OMNIBOX_EXAMPLE_DATABASE_URL=... python examples/complete_worker_setup.py``
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from omni_box import (
    AckStrategy,
    BaseEventHandler,
    CommitOffsetPolicy,
    EventRouter,
    InboxConsumerRunner,
    InboxEvent,
    InboxEventRepository,
    create_dispatching_processor,
    event_handler,
    handler_completed,
)
from omni_box.core.protocols import ConsumedMessage, EventConsumer
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol
from omni_box.core.services.results import EventHandlerResult
from omni_box.infra.storage.postgres import InboxEventDBBase, PostgresInboxRepository


class Base(DeclarativeBase):
    pass


class InboxEventDB(Base, InboxEventDBBase):
    pass


class OrderHandlers(BaseEventHandler):
    topic = "orders"

    @event_handler("order.created")
    async def on_order_created(self, event: InboxEvent, repo: InboxEventRepository) -> EventHandlerResult:
        print(f"[handler] order.created payload={event.payload}")
        return handler_completed()


class OneShotConsumer(EventConsumer):
    """Emits one fake message then blocks — useful for local smoke-testing."""

    def __init__(self) -> None:
        self._delivered = False

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def getone(self) -> ConsumedMessage:
        if self._delivered:
            await asyncio.sleep(3600)
        self._delivered = True
        return ConsumedMessage(
            message_id="msg-1",
            source="orders",
            event_type="order.created",
            payload={"order_id": "123"},
        )


class SessionInboxTxProvider(InboxTransactionProviderProtocol):
    """Each :meth:`transaction` call opens a fresh SQLAlchemy session+tx."""

    def __init__(self, session_factory: async_sessionmaker, model_class: type[InboxEventDBBase]) -> None:
        self._session_factory = session_factory
        self._model_class = model_class

    @asynccontextmanager
    async def _open(self) -> AsyncIterator[InboxEventRepository]:
        async with self._session_factory() as session, session.begin():
            yield PostgresInboxRepository(session, model_class=self._model_class)

    def transaction(self) -> AbstractAsyncContextManager[InboxEventRepository]:
        return self._open()


async def main() -> None:
    dsn = os.environ.get("OMNIBOX_EXAMPLE_DATABASE_URL")
    if not dsn:
        print("Set OMNIBOX_EXAMPLE_DATABASE_URL to run this example against Postgres.")
        return

    engine = create_async_engine(dsn)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    router = EventRouter()
    router.register_class(OrderHandlers)

    # The dispatching processor is shown for completeness — it's typically used
    # for poll-based draining of the inbox table; the runner below performs the
    # streaming side of the workflow.
    async with session_factory() as session, session.begin():
        repo = PostgresInboxRepository(session, model_class=InboxEventDB)
        create_dispatching_processor(
            repo=repo,
            router=router,
            enable_otel=False,
            enable_circuit_breaker=True,
            job_name="orders_worker",
        )

    tx_provider = SessionInboxTxProvider(session_factory, InboxEventDB)
    runner = InboxConsumerRunner(
        consumer=OneShotConsumer(),
        transaction_provider=tx_provider,
        worker_id="worker-1",
        consumer_group="orders-cg",
        ack_strategy=AckStrategy.AT_LEAST_ONCE,
        commit_offset_policy=CommitOffsetPolicy.ON_SUCCESS,
    )

    await runner.start()
    try:
        # ``run_forever`` blocks; in this demo we cancel it after one message.
        run_task = asyncio.create_task(runner.run_forever())
        await asyncio.sleep(1.0)
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass
    finally:
        await runner.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
