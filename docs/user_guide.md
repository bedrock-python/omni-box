# User Guide

`omni-box` provides production-ready primitives for **Transactional Outbox** and **Transactional Inbox** patterns in async Python services.

## Installation

```bash
uv add omni-box
# or
pip install omni-box
```

Optional extras (see [`README.md`](../README.md#installation) for the full list): `postgres`, `kafka`, `metrics`, `opentelemetry`, `settings`, `dishka`.

## Basic concepts

- **Outbox** — guarantees that an event reaches the broker *iff* the originating DB transaction committed. The publisher is a separate background job that reads pending rows.
- **Inbox** — guarantees that each incoming broker message is handled at most once per `(message_id, consumer_group)` thanks to the inbox unique index, regardless of broker redelivery.

## Transactional Outbox

### 1. Define your payload schema (optional)

```python
from pydantic import BaseModel

class UserCreated(BaseModel):
    user_id: str
    email: str
```

### 2. Persist events inside your business transaction

```python
from omni_box import OmniBoxDomainService

domain = OmniBoxDomainService()

async def create_user(uow, email: str):
    async with uow.transaction() as tx:
        user = await tx.users.create(email=email)
        event = domain.create_outbox_event(
            event_type="user.created",
            topic="users",
            partition_key=str(user.id),
            payload={"user_id": str(user.id), "email": email},
            aggregate_type="user",
            aggregate_id=user.id,
        )
        await tx.outbox.create(event)
```

`tx.outbox` is your service-owned repository — typically `PostgresOutboxRepository` bound to the same `AsyncSession` as the rest of your UoW.

### 3. Run the publisher

```python
import asyncio

from omni_box import OutboxPublisher
from omni_box.core.converters import EnvelopeEventConverter
from omni_box.infra.brokers.kafka import KafkaEventPublisher

broker = KafkaEventPublisher(
    producer=kafka_producer,            # caller-owned AIOKafkaProducer
    converter=EnvelopeEventConverter(),
)
publisher = OutboxPublisher(repo=outbox_repo, broker=broker)

while not shutdown:
    result = await publisher.publish_batch(worker_id="publisher-1", batch_size=100)
    if not result.processed_event_ids:
        await asyncio.sleep(1.0)
```

## Transactional Inbox

### Option A — drive consumption with `InboxConsumerRunner`

This is the typical "one Kafka message per transaction" loop with configurable commit semantics.

```python
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from omni_box import AckStrategy, InboxConsumerRunner
from omni_box.core.protocols import InboxEventRepository
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol


class InboxTxProvider(InboxTransactionProviderProtocol):
    def __init__(self, session_factory, repo_factory) -> None:
        self._session_factory = session_factory
        self._repo_factory = repo_factory

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[InboxEventRepository]:
        async with self._session_factory() as session, session.begin():
            yield self._repo_factory(session)


runner = InboxConsumerRunner(
    consumer=kafka_consumer_adapter,
    transaction_provider=InboxTxProvider(session_factory, lambda s: PostgresInboxRepository(s, model_class=InboxEventDB)),
    handler=handle_inbox_event,     # optional in-tx handler
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

### Option B — batch processing already-stored inbox rows

Use `create_inbox_processor` when you want to ingest messages quickly (commit on persist) and process them in a separate workload.

```python
from omni_box import InboxEvent, create_inbox_processor
from omni_box.core.protocols import InboxEventRepository

async def my_handler(event: InboxEvent, repo: InboxEventRepository):
    print(f"Processing {event.event_type} ({event.message_id})")

processor = create_inbox_processor(
    repo=inbox_repo,
    handler=my_handler,
    job_name="my_inbox_job",
)

await processor.process_batch(worker_id="worker-1", batch_size=50)
```

### Option C — automated event routing

```python
from omni_box import EventRouter, create_dispatching_processor, event_handler, InboxEvent
from omni_box.core.protocols import InboxEventRepository

router = EventRouter()

@event_handler(event_type="user.created", source="users")
async def handle_user_created(event: InboxEvent, repo: InboxEventRepository, uow):
    ...

processor = create_dispatching_processor(
    repo=inbox_repo,
    router=router,
    dependencies={"uow": uow},
)
```

## Customising the pipeline

Need full control? Build the processor yourself.

```python
from omni_box import EventProcessorBuilder
from omni_box.core.pipeline.steps import (
    CircuitBreakerStep,
    DLQStep,
    HandlerExecutionStep,
    OpenTelemetryStep,
    SiblingDeduplicationStep,
)
from omni_box.core.pipeline.strategies import (
    BulkCommitStrategy,
    DistributedLockingFetchStrategy,
)

builder = (
    EventProcessorBuilder(inbox_repo)
    .add_step(OpenTelemetryStep(service_name="my-service"))
    .add_step(CircuitBreakerStep(failure_threshold=5, recovery_timeout_seconds=60))
    .add_step(DLQStep(dlq_storage))
    .add_step(SiblingDeduplicationStep())
    .add_step(HandlerExecutionStep(my_handler, timeout=30))
    .with_fetch_strategy(DistributedLockingFetchStrategy())
    .with_commit_strategy(BulkCommitStrategy())
)

processor = builder.build()
```

The builder auto-picks `DistributedLockingFetchStrategy` + `BulkCommitStrategy` when the repository advertises matching capabilities, so the `with_*` calls above are usually optional.

## Outbox payload envelopes

`OutboxPublisher` delegates the body shape to a converter:

| Converter | Body |
| :--- | :--- |
| `RawEventConverter` | Raw `event.payload`. |
| `SchemaVersionedConverter` | `{"schema_version": …, "payload": …}`. |
| `EnvelopeEventConverter` | Full envelope: payload + tracing identifiers. |

```python
from omni_box import EnvelopeEventConverter
from omni_box.infra.brokers.kafka import KafkaEventPublisher

broker = KafkaEventPublisher(producer=p, converter=EnvelopeEventConverter())
```

## Observability

### Structured logging

`omni-box` emits structured events through `structlog`. Configure renderers in your application bootstrap — for example:

```python
import structlog

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
```

`omni-box` does not ship a PII sanitiser; redact sensitive fields in your own `structlog` processor if needed.

### Metrics

Pass `metrics=` to any factory or to `MetricsStep`. Either implement `InboxMetrics` / `OutboxMetrics` yourself or use the Prometheus adapters from `omni_box.infra.metrics` (extra: `metrics`).

### OpenTelemetry

Add `OpenTelemetryStep(service_name="my-service")` to your pipeline (extra: `opentelemetry`).

### Dead Letter Queue

```python
from omni_box.core.pipeline.steps import DLQStep, DLQStorage

class MyDLQStorage(DLQStorage):
    async def move_to_dlq(self, event, error: str) -> None:
        await db.save_to_dlq(event, error)

builder.add_step(DLQStep(MyDLQStorage()))
```

`DLQStep` is **best-effort and non-transactional**. Transient failures (`count_as_attempt=False`) are never routed to DLQ.

### Circuit breaker

```python
from omni_box.core.pipeline.steps import CircuitBreakerStep

builder.add_step(CircuitBreakerStep(failure_threshold=5, recovery_timeout_seconds=60))
```

The breaker state is held **in-process** — it does not survive restarts and is not shared between replicas. For multi-worker deployments add an external coordination layer (e.g. Redis-backed counters) on top of this step.

## Maintenance

Run periodically against the same repository:

```python
from omni_box import OmniBoxMaintenanceService

m = OmniBoxMaintenanceService(repo=outbox_repo)
await m.release_stale_locks(stale_timeout_seconds=300)
await m.cleanup_old_events(retention_days=14)
```

Both methods require the repository to implement `SupportsRetentionPolicies`. `PostgresOutboxRepository` and `PostgresInboxRepository` do.
