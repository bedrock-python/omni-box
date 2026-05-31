# API Reference

This page documents the public surface of `omni-box` — everything re-exported from the top-level package and the most useful submodules. For an auto-generated, source-of-truth listing run:

```python
import omni_box
print(sorted(omni_box.__all__))
print(omni_box.__version__)
```

## Domain models

Imported from `omni_box` (defined in `omni_box.core.models`).

- **`BaseEvent`** — common fields: `id`, `event_type`, `payload`, `headers`, `status`, `attempts_made`, `max_attempts`, `last_error`, timing (`scheduled_at`, `completed_at`, `created_at`, `updated_at`), locking (`locked_at`, `locked_by`), tracing (`trace_id`, `correlation_id`, `causation_id`, `idempotency_key`, `schema_version`).
- **`OutboxEvent`** — adds `aggregate_type`, `aggregate_id`, `topic`, `partition_key`.
- **`InboxEvent`** — adds `message_id`, `consumer_group`, `source`.
- **`BaseEventSchema`** — Pydantic schema base used by ad-hoc payload models.
- **`EventStatus`** (`StrEnum`) — `PENDING = "pending"`, `COMPLETED = "completed"`, `FAILED = "failed"`.

## Domain services

### `OmniBoxDomainService`

Factory for validated event entities and the source of truth for lock/transition rules.

- `create_outbox_event(aggregate_type, aggregate_id, event_type, topic, partition_key, payload, *, headers=None, idempotency_key=None, trace_id=None, correlation_id=None, causation_id=None, schema_version=None, max_attempts=None, scheduled_at=None) -> OutboxEvent`
- `create_inbox_event(message_id, consumer_group, source, event_type, payload, *, headers=None, trace_id=None, correlation_id=None, causation_id=None, schema_version=None) -> InboxEvent`
- `lock_event(event, worker_id, locked_at)`, `refresh_event_lock`, `unlock_event`, `force_unlock_event`
- `mark_event_completed`, `mark_event_failed` (supports `count_as_attempt`, `next_retry_at`)
- `is_lock_stale(event, now, stale_timeout_seconds=None) -> bool`

### `OmniBoxMaintenanceService`

Operational helpers. Requires a repository that implements `SupportsRetentionPolicies`.

- `release_stale_locks(stale_timeout_seconds) -> int`
- `cleanup_old_events(retention_days, batch_size=..., max_iterations=...) -> int`

## Protocols

From `omni_box.core.protocols`.

- **Repositories**: `EventRepository[T]`, `OutboxEventRepository`, `InboxEventRepository`, `FetchFilters`, `RepositoryCapabilities`.
- **Capabilities**: `SupportsBulkOperations[T]`, `SupportsDistributedLocking[T]`, `SupportsRetentionPolicies`.
- **Broker**: `EventPublisher`, `EventConsumer`, `ConsumedMessage`, `AckHandle` / `NullAckHandle`, `EnvelopeParser`, `EnvelopeData`, `InboxHandler`.
- **Transaction providers**: `InboxTransactionProviderProtocol`, `OutboxTransactionProviderProtocol` (under `omni_box.core.protocols.transaction`).
- **Metrics**: `InboxMetrics`, `OutboxMetrics`, `ProcessingMetrics` (under `omni_box.core.protocols.metrics`).

## Pipeline

From `omni_box.core.pipeline`.

### `EventProcessorBuilder[T: BaseEvent]`

Fluent builder. Picks `DistributedLockingFetchStrategy` + `BulkCommitStrategy` automatically when the repository advertises the matching capabilities; falls back to `OptimisticLockingFetchStrategy` + `SingleCommitStrategy`.

| Method | Description |
| :--- | :--- |
| `add_step(step)` | Append a `ProcessingStep[T]`. |
| `with_fetch_strategy(strategy)` | Override the auto-picked fetch strategy. |
| `with_commit_strategy(strategy)` | Override the auto-picked commit strategy. |
| `with_metrics(metrics)` | Wire a `ProcessingMetrics` collector. |
| `with_lease_ttl(seconds)` | Lock TTL used by `DistributedLockingFetchStrategy`. |
| `with_job_name(name)` | Logging/metrics label. |
| `build()` | Returns an `EventBatchProcessor[T]`. |

### `EventBatchProcessor[T: BaseEvent]`

- `process_batch(worker_id, batch_size, shutdown_requested_func=None, **fetch_filters) -> BatchProcessingResult`
- `BatchProcessingResult` carries `processed_event_ids`, `failed_counted`, `failed_noncounted`, `remaining_event_ids`, `commit_failed`.

### Built-in steps (`omni_box.core.pipeline.steps`)

| Step | Purpose | Notes |
| :--- | :--- | :--- |
| `HandlerExecutionStep` | Runs the user handler inside the pipeline with a timeout. | Required terminal step in every processor. |
| `SiblingDeduplicationStep` | Skips an `InboxEvent` if a sibling row with the same `(message_id, consumer_group)` is already `completed`. | Calls `InboxEventRepository.has_completed_sibling_for_inbox_key`. |
| `MetricsStep` | Pushes batch lifecycle counters into an `InboxMetrics` / `OutboxMetrics` sink. | |
| `OpenTelemetryStep(service_name=...)` | Creates spans for each batch/event. | Requires `opentelemetry` extra. |
| `CircuitBreakerStep(failure_threshold, recovery_timeout_seconds)` | Stops batch processing after consecutive failures. | **State is process-local; not distributed.** Add Redis-backed coordination if you need cross-replica behaviour. |
| `DLQStep(dlq_storage)` | Routes failed events to a `DLQStorage[T]` after exhausting retries. | **Best-effort, non-transactional.** Transient (`count_as_attempt=False`) failures are **never** routed to DLQ. |

### Strategies (`omni_box.core.pipeline.strategies`)

- Fetch: `DistributedLockingFetchStrategy`, `OptimisticLockingFetchStrategy`, `FilteredFetchStrategy`.
- Commit: `BulkCommitStrategy`, `SingleCommitStrategy`.

## Application-layer services

### `OutboxPublisher` (`omni_box.application.services.publish`)

```python
OutboxPublisher(
    repo: OutboxEventRepository,
    broker: EventPublisher,
    metrics: OutboxMetrics | None = None,
    publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    concurrency_limit: int | None = None,
)
```

- `publish_batch(worker_id, batch_size, shutdown_requested_func=None, **fetch_filters)`

### `InboxConsumerRunner` (`omni_box.application.services.consume`)

```python
InboxConsumerRunner(
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
)
```

- `start()`, `stop()`
- `run_forever()` — loops calling `process_one`, applies exponential backoff on errors.
- `process_one() -> InboxConsumeResult` (`message_id`, `event_id`, `committed`, `processed`, `duplicate`).

Enums:

- `AckStrategy` — `AT_MOST_ONCE`, `AT_LEAST_ONCE`, `EXACTLY_ONCE_INBOX`.
- `CommitOffsetPolicy` — `ON_PERSIST`, `ON_SUCCESS` (only used by `AT_LEAST_ONCE`).

### Factories (`omni_box.application.factories`)

All three return `EventBatchProcessor[T]`.

- `create_outbox_processor(repo, publisher, *, publish_timeout, metrics=None, dlq_storage=None, enable_otel=False, enable_circuit_breaker=False, ...)`
- `create_inbox_processor(repo, handler, *, skip_duplicate_siblings=True, filter_sources=None, process_timeout=..., ...)`
- `create_dispatching_processor(repo, router, *, dependencies=None, ...)` — uses an `EventRouter`.

## Dispatch (`omni_box.core.dispatch`)

- `EventRouter` — registry of handlers keyed by `(event_type, source)`.
- `BaseEventHandler` — base class for class-based handlers.
- `event_handler(event_type, source=...)` — decorator.

## Handler results

From `omni_box.core.services.results`, re-exported at the top level.

- `EventHandlerStatus`, `EventHandlerResult`, `BatchProcessingResult`.
- Helpers: `handler_completed()`, `handler_retry(error)`, `handler_skipped(reason)`.

## Converters (`omni_box.core.converters`)

- `EventConverter` (protocol)
- `RawEventConverter` — body is the raw `payload`.
- `SchemaVersionedConverter` — body is `{"schema_version": ..., "payload": ...}`.
- `EnvelopeEventConverter` — full envelope with tracing identifiers (re-exported at the top level).

## Exceptions

All inherit from `OmniBoxError`.

- Storage: `StorageError`, `StorageConnectionError`, `StorageTimeoutError`, `StorageTransactionError`, `StorageIntegrityError`.
- Domain / locking: `EventNotLockedError`, `EventLockedByAnotherWorkerError`, `EventAlreadyLockedError`, `InvalidEventStateError`, `EventConcurrentUpdateError`.
- Misc: `UnsupportedCapabilityError`, `InboxPersistError`.

## Infrastructure adapters

### PostgreSQL (extra: `postgres`)

`omni_box.infra.storage.postgres`:

- ORM bases: `OutboxEventDBBase`, `InboxEventDBBase`, `OutboxEventPartitionedDBBase`, `InboxEventPartitionedDBBase`, plus the underlying `EventMixin`, `OutboxColumnsMixin`, `InboxColumnsMixin`.
- Repositories: `PostgresOutboxRepository`, `PostgresInboxRepository`, `PostgresEventRepository` (shared base).
- Helpers: `UnConstrainedEnum`, `get_event_constraints(table_name, include_created_at_in_unique=False)`.

### Kafka (extra: `kafka`)

`omni_box.infra.brokers.kafka`:

- `KafkaEventPublisher(producer, converter, *, max_infra_retries=3)` — built on top of `aiokafka.AIOKafkaProducer`. The caller owns the producer lifecycle (`start`/`stop`).
- `KafkaEventConsumer` — wraps `aiokafka.AIOKafkaConsumer` and exposes per-record `AckHandle`s. Use `DefaultEnvelopeParser` or provide your own `EnvelopeParser`.

Neither adapter depends on any external "kit" package; only `aiokafka` is required.

### Prometheus (extra: `metrics`)

`omni_box.infra.metrics` provides Prometheus implementations of `InboxMetrics`, `OutboxMetrics`, and `ProcessingMetrics`. Wire them into the factories or pass to `MetricsStep` directly.

## Version

```python
from omni_box import __version__
```
