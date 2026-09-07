# Quick start

A condensed walkthrough. For full examples see the [User guide](../user_guide.md).

## 1. Install

```bash
pip install "omni-box[postgres,kafka]"
```

## 2. Define the tables

`omni-box` does not ship a `Base`. Bind the abstract ORM models to your service-owned `DeclarativeBase`:

```python
from sqlalchemy.orm import DeclarativeBase
from omni_box.infra.storage.postgres import InboxEventDBBase, OutboxEventDBBase


class Base(DeclarativeBase):
    pass


class OutboxEventDB(Base, OutboxEventDBBase):
    pass


class InboxEventDB(Base, InboxEventDBBase):
    pass
```

Then generate a migration. See [migrations.md](../migrations.md) for the exact DDL.

## 3. Persist outbox rows in your business transaction

```python
from omni_box import OmniBoxDomainService

domain = OmniBoxDomainService()
event = domain.create_outbox_event(
    aggregate_type="user",
    aggregate_id=user_id,
    event_type="user.created",
    topic="users.events",
    partition_key=str(user_id),
    payload={"email": "user@example.com"},
)

async with uow.transaction() as tx:
    await tx.users.create(user)
    await tx.outbox.create(event)
```

## 4. Run the publisher

The publisher runs in a transaction of its own, and — like every other repository call —
it is the caller who opens and commits it. Fetch, lock, publish and status update are one
unit of work.

```python
from omni_box import OutboxPublisher
from omni_box.core.converters import EnvelopeEventConverter
from omni_box.infra.brokers.kafka import KafkaEventPublisher
from omni_box.infra.storage.postgres import PostgresOutboxRepository

broker = KafkaEventPublisher(producer=producer, converter=EnvelopeEventConverter())

while not shutdown:
    async with session_factory() as session, session.begin():   # the commit is yours
        repo = PostgresOutboxRepository(session, model_class=OutboxEventDB)
        result = await OutboxPublisher(repo, broker).publish_batch(
            worker_id="publisher-1",
            batch_size=100,
        )
    if not result.processed_event_ids:
        await asyncio.sleep(1.0)
```

Without the transaction the lock and the completion are rolled back with the session: the
rows stay `pending` and the next cycle republishes them.

## 5. Consume into the inbox

The runner opens a transaction through your provider, inserts the inbox row, runs the
handler inside that same transaction, and commits the broker offset according to the
`AckStrategy`. `repo.session` is that transaction: write the side effect through it and it
commits with the inbox row, or rolls back with it when the handler raises. A session the
handler opens itself is a second transaction and does not get that.

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from omni_box import AckStrategy, InboxConsumerRunner, InboxEvent, InboxEventRepository
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol
from omni_box.infra.storage.postgres import PostgresInboxRepository


class InboxTxProvider(InboxTransactionProviderProtocol):
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[InboxEventRepository]:
        async with session_factory() as session, session.begin():   # the commit is yours
            yield PostgresInboxRepository(session, model_class=InboxEventDB)


async def handle(event: InboxEvent, repo: InboxEventRepository) -> None:
    await repo.session.execute(             # the transaction the inbox row is in
        profiles.insert().values(email=event.payload["email"])
    )


runner = InboxConsumerRunner(
    consumer=kafka_consumer_adapter,
    transaction_provider=InboxTxProvider(),
    handler=handle,
    worker_id="worker-1",
    consumer_group="identity-service",
    ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
)

await runner.start()
try:
    await runner.run_forever()
finally:
    await runner.stop()
```
