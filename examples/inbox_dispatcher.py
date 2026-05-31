"""Usage of the Transactional Inbox pattern with ``EventRouter``.

Demonstrates how to register handlers per event type, build a dispatching
processor, and drain a batch of pending inbox events. The processor variant
shown here is suited for *poll-based* inbox consumption — for streaming Kafka
consumption use :class:`InboxConsumerRunner` (see ``complete_worker_setup.py``).

Set ``OMNIBOX_EXAMPLE_DATABASE_URL`` to a reachable PostgreSQL DSN to run it
end-to-end.
"""

from __future__ import annotations

import asyncio
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from omni_box import (
    BaseEventHandler,
    EventRouter,
    InboxEvent,
    InboxEventRepository,
    create_dispatching_processor,
    event_handler,
    handler_completed,
)
from omni_box.core.services.results import EventHandlerResult
from omni_box.infra.storage.postgres import InboxEventDBBase, PostgresInboxRepository


class Base(DeclarativeBase):
    pass


class InboxEventDB(Base, InboxEventDBBase):
    """Concrete ORM model — required because ``InboxEventDBBase`` is abstract."""


class OrderHandlers(BaseEventHandler):
    """Group related event handlers under a single topic."""

    topic = "orders"

    @event_handler("order.created")
    async def on_order_created(self, event: InboxEvent, repo: InboxEventRepository) -> EventHandlerResult:
        # Idempotent business logic goes here.
        print(f"[handler] order.created id={event.id} payload={event.payload}")
        return handler_completed()


async def main() -> None:
    dsn = os.environ.get("OMNIBOX_EXAMPLE_DATABASE_URL")
    if not dsn:
        print("Set OMNIBOX_EXAMPLE_DATABASE_URL to run this example against Postgres.")
        return

    router = EventRouter()
    router.register_class(OrderHandlers)

    engine = create_async_engine(dsn)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session, session.begin():
        repo = PostgresInboxRepository(session, model_class=InboxEventDB)
        processor = create_dispatching_processor(
            repo=repo,
            router=router,
            job_name="orders_inbox",
        )

        result = await processor.process_batch(worker_id="w1", batch_size=20)
        print(
            f"[processor] processed={len(result.processed_event_ids)} "
            f"failed_counted={len(result.failed_counted)} "
            f"remaining={len(result.remaining_event_ids)}"
        )

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
