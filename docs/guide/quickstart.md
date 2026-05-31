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

```python
from omni_box import OutboxPublisher
from omni_box.core.converters import EnvelopeEventConverter
from omni_box.infra.brokers.kafka import KafkaEventPublisher

broker = KafkaEventPublisher(producer=producer, converter=EnvelopeEventConverter())
publisher = OutboxPublisher(repo=outbox_repo, broker=broker)

while not shutdown:
    result = await publisher.publish_batch(worker_id="publisher-1", batch_size=100)
    if not result.processed_event_ids:
        await asyncio.sleep(1.0)
```

## 5. Consume into the inbox

```python
from omni_box import AckStrategy, InboxConsumerRunner

runner = InboxConsumerRunner(
    consumer=kafka_consumer_adapter,
    transaction_provider=my_inbox_tx_provider,
    handler=my_handler,
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

The `transaction_provider` must implement `InboxTransactionProviderProtocol` from `omni_box.core.protocols.transaction`.
