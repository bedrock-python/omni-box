"""Basic usage of the Transactional Outbox pattern.

This example shows how to:

1. Define a concrete SQLAlchemy ORM model for outbox events.
2. Persist an outbox event inside a business transaction so the row and your
   domain change either both commit or both roll back.
3. Run a background publisher that drains pending rows to a broker.

To run end-to-end, set ``OMNIBOX_EXAMPLE_DATABASE_URL`` to a reachable
PostgreSQL DSN (e.g. ``postgresql+asyncpg://omnibox:omnibox@localhost/omnibox``)
and run ``python examples/outbox_basic.py``. Without the env var the script
just prints the wiring it *would* perform.
"""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from omni_box import OmniBoxDomainService, OutboxEvent, OutboxPublisher
from omni_box.core.protocols import EventPublisher, OutboxEventRepository
from omni_box.infra.storage.postgres import OutboxEventDBBase, PostgresOutboxRepository


class Base(DeclarativeBase):
    pass


class OutboxEventDB(Base, OutboxEventDBBase):
    """Concrete ORM model — required because ``OutboxEventDBBase`` is abstract."""


class StdoutBroker(EventPublisher):
    """Minimal broker that prints events instead of sending them anywhere."""

    async def publish(self, event: OutboxEvent, repo: OutboxEventRepository) -> None:
        print(f"[broker] publish {event.event_type} → topic={event.topic} id={event.id}")


async def main() -> None:
    dsn = os.environ.get("OMNIBOX_EXAMPLE_DATABASE_URL")
    if not dsn:
        print("Set OMNIBOX_EXAMPLE_DATABASE_URL to run this example against Postgres.")
        return

    engine = create_async_engine(dsn)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    domain = OmniBoxDomainService()
    broker = StdoutBroker()

    # 1. Producer side — write the outbox row inside the business transaction.
    async with session_factory() as session, session.begin():
        repo = PostgresOutboxRepository(session, model_class=OutboxEventDB)
        event = domain.create_outbox_event(
            aggregate_type="order",
            aggregate_id=uuid4(),
            event_type="order.created",
            topic="orders",
            partition_key="order-123",
            payload={"order_id": "123", "amount": 100.0},
        )
        await repo.create(event)

    # 2. Background worker side — drain pending rows to the broker.
    async with session_factory() as session, session.begin():
        repo = PostgresOutboxRepository(session, model_class=OutboxEventDB)
        publisher = OutboxPublisher(repo, broker)
        result = await publisher.publish_batch(worker_id="worker-1", batch_size=10)
        print(
            f"[publisher] published={len(result.processed_event_ids)} "
            f"failed_counted={len(result.failed_counted)} "
            f"failed_noncounted={len(result.failed_noncounted)}"
        )

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
