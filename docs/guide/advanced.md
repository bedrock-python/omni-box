# Advanced

## Custom `ProcessingStep`

```python
from omni_box.core.pipeline.step import BaseProcessingStep, StepResult


class AuditStep(BaseProcessingStep):
    async def execute(self, event, context):
        await audit_log.write(event)
        return StepResult.next()
```

Add it to the builder before `HandlerExecutionStep`.

## Custom `FetchStrategy`

Implement `omni_box.core.pipeline.strategies.fetch.FetchStrategy[T]`. The strategy receives an `EventRepository[T]` and a `worker_id`, and must return a list of events with appropriate locking semantics.

`FilteredFetchStrategy(sources=[...])` is a quick way to scope a worker to a subset of `source` values; it composes with the default strategies.

## Custom `CommitStrategy`

Implement `omni_box.core.pipeline.strategies.commit.CommitStrategy[T]`. It receives the final `ProcessingContext` and is responsible for persisting completion / failure / skipped transitions.

## Partitioned tables

Use `OutboxEventPartitionedDBBase` / `InboxEventPartitionedDBBase`. PostgreSQL requires the partition key (`created_at`) to appear in every unique index; the partitioned bases handle this for you via `__inbox_dedup_index_columns__` and `__outbox_conflict_index_*__`.

Create child partitions per time window (typically RANGE by month or day) and attach them with `pg_partman` or your own DDL. The repositories use `INSERT ... ON CONFLICT DO NOTHING` on the partitioned table, which propagates to the correct child partition.

## Multi-region / multi-worker

- Use `DistributedLockingFetchStrategy` (auto-picked when the repository implements `SupportsDistributedLocking`).
- Tune `EventProcessorBuilder.with_lease_ttl(seconds)` to cover the worst-case handler runtime plus a safety margin.
- Run `OmniBoxMaintenanceService.release_stale_locks` periodically with a `stale_timeout_seconds` strictly larger than the lease TTL.
- Remember that `CircuitBreakerStep` is **process-local** — replicas trip independently. Add Redis-backed counters if you need cross-replica coordination.

## Custom DLQ sink

```python
from omni_box.core.pipeline.steps import DLQStep, DLQStorage


class KafkaDLQ(DLQStorage):
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic

    async def move_to_dlq(self, event, error: str) -> None:
        await self._producer.send_and_wait(
            self._topic,
            key=str(event.id).encode(),       # idempotent key — avoids dup on replay
            value=event.model_dump_json().encode(),
            headers=[("dlq-error", error.encode())],
        )

builder.add_step(DLQStep(KafkaDLQ(producer, "events.dlq")))
```

Because `DLQStep` is best-effort, the unique key keeps replays from inflating the DLQ.

## Replacing the broker

`OutboxPublisher` and `InboxConsumerRunner` operate against the broker protocols:

- `EventPublisher` — `async def publish(event, repo)`.
- `EventConsumer` — `start`, `stop`, `async def getone() -> ConsumedMessage`.

Implement those for RabbitMQ, Pulsar, NATS, etc. The Kafka adapter is just a reference implementation; nothing about the core pipeline assumes Kafka.
