# omni-box for AI agents

> One page holding everything a coding assistant needs to wire and drive omni-box
> correctly, plus a map of where the rest of the documentation keeps the details it leaves
> out. Give an agent this page rather than the whole site.

| | |
|---|---|
| Package | `omni-box` on PyPI, import root `omni_box` |
| Requires | Python 3.12+ (PEP 695 generics). Core depends on `pydantic` 2, `orjson`, `structlog` only |
| Install | `pip install omni-box` · extras: `postgres`, `kafka`, `metrics`, `opentelemetry`, `settings`, `dishka` |
| Async | Everything. There is no sync API and no sync mirror module |
| Storage | PostgreSQL via SQLAlchemy 2 + asyncpg (`postgres` extra), tested on 17; anything else by implementing the repository protocols |
| Source | <https://github.com/bedrock-python/omni-box> |

## How to read this page

Every page of this site is also served as raw Markdown at its own URL with `.md` in place
of the trailing slash — this page is `/agents.md`, the quick start is
`/guide/quickstart.md` — so anything the map below points at can be fetched as plain text
rather than scraped out of HTML. The **Copy page** control at the top of a page does the
same thing for a human with a chat window open. The one exception is the generated API
reference: its Markdown is a directive to a docstring renderer rather than the API, so it
carries neither the control nor a `.md` twin — read it as HTML, or read the docstrings in
the source.

Top to bottom before writing code. [Rules that hold or break the code](#rules-that-hold-or-break-the-code)
is the section correctness lives in — those are the things the library will not save you
from. Every name used below is in the public API; if you need something not listed here,
fetch the page the [documentation map](#documentation-map) points at rather than guessing
a method that sounds plausible.

## Scope

**It does** give you the two halves of transactional messaging as primitives. The
**outbox**: a validated event entity you insert in the same transaction as your business
state, and a relay that locks a batch of pending rows, hands each to a broker, and writes
the outcome back. The **inbox**: a row keyed by `(message_id, consumer_group)` that a
redelivered message collides with, a consumer runner that lands one message per
transaction with configurable commit semantics, and a batch processor for rows already
landed. Around both: a composable pipeline (metrics, OpenTelemetry, DLQ, circuit breaker,
sibling deduplication), a PostgreSQL repository that speaks `FOR UPDATE SKIP LOCKED`, and
an `aiokafka` adapter.

**It does not** own a transaction. There is no Unit of Work, no session factory, no
`commit()` anywhere in the library except the broker offset commit — the transactional
boundary that makes an outbox an outbox is yours, and it is the one thing you must get
right. It creates no tables and ships no migrations: the ORM bases are abstract and you
bind them to your own `DeclarativeBase`. It runs no scheduler and no daemon; the publisher
loop is a `while` in your worker. It does not deliver exactly once — nothing does — and it
does not make your handler idempotent.

## Mental model

**Outbox.** `OmniBoxDomainService.create_outbox_event(...)` builds an `OutboxEvent` — a
frozen Pydantic model with `status=PENDING`. You insert it through an
`OutboxEventRepository` in the same transaction as the business change; either both land
or neither does. Later, in a different transaction, `OutboxPublisher.publish_batch(...)`
runs one cycle: **fetch and lock** pending rows, **publish** each through an
`EventPublisher`, **commit** the outcomes back to the rows. All three are one database
transaction, and it is yours to commit.

**Inbox.** `InboxConsumerRunner` pulls one message from an `EventConsumer`, opens a
transaction through your `InboxTransactionProviderProtocol`, inserts an `InboxEvent`, runs
your handler inside that same transaction, and commits the broker offset according to the
`AckStrategy`. A message already in the table does not insert a second row: the repository
returns the existing one, and if it is `completed` the runner reports a duplicate and moves
on. Alternatively, land rows fast with no handler and drain them later with
`create_inbox_processor` — the same fetch/lock/process/commit cycle the outbox uses.

**The cycle** underneath both batch paths is `EventBatchProcessor.process_batch`:

* a **fetch strategy** returns a locked batch — `DistributedLockingFetchStrategy`
  (`SELECT … FOR UPDATE SKIP LOCKED` then `UPDATE … SET locked_by`) when the repository
  reports `supports_distributed_locking`, `OptimisticLockingFetchStrategy` otherwise;
* a **pipeline** of `ProcessingStep`s runs per event against a `ProcessingContext` that
  accumulates completed, failed-counted, failed-non-counted and skipped ids;
* a **commit strategy** writes those back — `BulkCommitStrategy` when the repository
  reports `supports_bulk`, `SingleCommitStrategy` otherwise;
* a `BatchProcessingResult` comes out. `EventProcessorBuilder.build()` picks both
  strategies from `repo.capabilities`; you only call `with_*` to override.

**State** is three values. `pending` → `completed` on success; `pending` → `pending` with
`attempts_made + 1` on a counted failure; `pending` → `failed` when `attempts_made` reaches
`max_attempts`. `failed` is terminal — nothing fetches it again.

**Locking** is a `(locked_at, locked_by)` pair on the row plus, on PostgreSQL, the row lock
the fetching transaction holds. Two workers do not collide because of `SKIP LOCKED`; the
column pair is what survives a crash, and `OmniBoxMaintenanceService.release_stale_locks`
is what clears it afterwards.

## Wiring

### The outbox, end to end

```python
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from omni_box import OmniBoxDomainService, OutboxPublisher
from omni_box.core.converters import EnvelopeEventConverter
from omni_box.infra.brokers.kafka import KafkaEventPublisher
from omni_box.infra.storage.postgres import OutboxEventDBBase, PostgresOutboxRepository


class Base(DeclarativeBase):
    pass


class OutboxEventDB(Base, OutboxEventDBBase):      # your table, your migration
    pass


engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
domain = OmniBoxDomainService()

# 1. Write the event with the business state, in one transaction.
async with session_factory() as session, session.begin():
    session.add(User(id=user_id, email=email))
    await PostgresOutboxRepository(session, model_class=OutboxEventDB).create(
        domain.create_outbox_event(
            aggregate_type="user",
            aggregate_id=user_id,
            event_type="user.created",
            topic="users.events",
            partition_key=str(user_id),
            payload={"email": email},          # non-empty, JSON-serializable
            idempotency_key=f"user.created:{user_id}",
        )
    )

# 2. Relay the rows, in a different transaction, in a different process if you like.
broker = KafkaEventPublisher(producer=producer, converter=EnvelopeEventConverter())

while not shutdown:
    async with session_factory() as session, session.begin():   # the commit is yours
        repo = PostgresOutboxRepository(session, model_class=OutboxEventDB)
        result = await OutboxPublisher(repo, broker).publish_batch(
            worker_id="publisher-1",           # [A-Za-z0-9.-/:] only
            batch_size=100,
        )
    if not result.processed_event_ids:
        await asyncio.sleep(1.0)
```

### The inbox, end to end

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from omni_box import AckStrategy, InboxConsumerRunner, InboxEvent, InboxEventRepository
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol
from omni_box.infra.storage.postgres import InboxEventDBBase, PostgresInboxRepository


class InboxTxProvider(InboxTransactionProviderProtocol):
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[InboxEventRepository]:
        async with self._session_factory() as session, session.begin():
            yield PostgresInboxRepository(session, model_class=InboxEventDB)


async def handle(event: InboxEvent, repo: InboxEventRepository) -> None:
    await do_the_work(event.payload)        # same transaction as the inbox row


runner = InboxConsumerRunner(
    consumer=kafka_consumer,                # any EventConsumer
    transaction_provider=InboxTxProvider(session_factory),
    handler=handle,                         # optional; omit to land rows and drain later
    worker_id="worker-1",
    consumer_group="identity-service",      # part of the deduplication key
    ack_strategy=AckStrategy.EXACTLY_ONCE_INBOX,
)

await runner.start()
try:
    await runner.run_forever()
finally:
    await runner.stop()
```

## The API

### Entities

`BaseEvent` is frozen (`model_config = ConfigDict(frozen=True)`); every transition returns a
new object.

| Field | Type | Default |
|---|---|---|
| `id` | `UUID` | `uuid4()` |
| `event_type` | `str`, ≤100 chars, stripped, non-empty | required |
| `payload` | `dict[str, JsonValue]` | required, non-empty, ≤1 000 000 bytes of JSON |
| `headers` | `dict[str, str] \| None` | `None`; ≤100 entries, key ≤64, value ≤512 |
| `status` | `EventStatus` | `PENDING` |
| `attempts_made` / `max_attempts` | `int` | `0` / `6` |
| `last_error` | `str \| None` | `None`, truncated to 2000 bytes |
| `created_at` / `scheduled_at` | aware `datetime`, normalised to UTC | now |
| `completed_at` / `locked_at` / `locked_by` | | `None` |
| `trace_id` `correlation_id` `causation_id` | `str \| None`, ≤64 | `None` |
| `idempotency_key` | `str \| None`, ≤128 | `None` |
| `schema_version` | `str \| None`, ≤50 | `None` |

`OutboxEvent` adds `aggregate_type` (≤50), `aggregate_id` (`UUID`), `topic` (≤255),
`partition_key` (≤255) — all required. `InboxEvent` adds `message_id` (≤255),
`consumer_group` (≤255), `source` (≤255), plus `processed_at` (alias for `completed_at`),
`get_context_value(key)` and `get_payload_as(schema_cls=None)`.

Properties on both: `is_locked`, `can_retry`, `attempts_left`, `failure_count`.
There is no `updated_at` on the entity — that column exists only in the ORM row.

`EventStatus` is a `StrEnum`: `PENDING = "pending"`, `COMPLETED = "completed"`,
`FAILED = "failed"`. The DDL uses the lowercase strings.

### `OmniBoxDomainService`

Constructor keywords, all optional: `max_attempts=6`, `scheduled_at_skew_seconds=60`,
`scheduled_at_max_future_seconds=31536000`, `payload_max_bytes=1_000_000`,
`headers_max_count=100`, `header_key_max_length=64`, `header_value_max_length=512`,
`last_error_max_length=2000`, `truncation_suffix="... [TRUNCATED]"`.

| Method | Signature | Returns |
|---|---|---|
| `create_outbox_event` | `(aggregate_type, aggregate_id, event_type, topic, partition_key, payload, *, headers=None, max_attempts=None, trace_id=None, idempotency_key=None, correlation_id=None, causation_id=None, schema_version=None, scheduled_at=None)` | `OutboxEvent` |
| `create_inbox_event` | `(message_id, consumer_group, source, event_type, payload, *, headers=None, trace_id=None, correlation_id=None, causation_id=None, schema_version=None)` | `InboxEvent` |
| `lock_event` | `(event, worker_id, locked_at)` | a new event |
| `refresh_event_lock` | `(event, worker_id, now)` | a new event |
| `unlock_event` | `(event, worker_id)` | a new event |
| `force_unlock_event` | `(event, reason)` | a new event; reason ≤255 chars |
| `mark_event_completed` | `(event, completed_at, worker_id)` | a new event |
| `mark_event_failed` | `(event, error, worker_id, count_as_attempt=True, next_retry_at=None)` | a new event |
| `is_lock_stale` | `(event, now, stale_timeout_seconds=None)` — default 300 s | `bool` |
| `assert_locked_by` | `(event, worker_id)` | raises or returns `None` |

`create_inbox_event` takes no `idempotency_key` and no `max_attempts`: the inbox key is
`(message_id, consumer_group)` and the budget comes from the service.

### Repository protocols (`omni_box.core.protocols`)

| Method | On | Returns |
|---|---|---|
| `capabilities` (property) | `EventRepository[T]` | `RepositoryCapabilities(supports_bulk, supports_distributed_locking, supports_retention)` |
| `create(event)` | `EventRepository[T]` | the row **that is in the table** — the existing one on a duplicate |
| `get_by_id(event_id)` | `EventRepository[T]` | `T \| None` |
| `fetch_pending(limit, **filters)` | `EventRepository[T]` | `list[T]`, `pending`, unlocked, due, with budget left |
| `mark_processing(event_id, worker_id)` | `EventRepository[T]` | `bool` — `False` when someone else holds it |
| `mark_completed(event_id, worker_id)` | `EventRepository[T]` | `None`; raises `EventConcurrentUpdateError` |
| `mark_failed(event_id, error, worker_id, next_retry_at, count_as_attempt=True)` | `EventRepository[T]` | `None` |
| `get_by_message_id(message_id, consumer_group)` | `InboxEventRepository` | `InboxEvent \| None` |
| `exists(message_id, consumer_group)` | `InboxEventRepository` | `bool` |
| `has_completed_sibling_for_inbox_key(message_id, consumer_group, exclude_event_id)` | `InboxEventRepository` | `bool` |

`FetchFilters` is a `TypedDict`: `source`, `topic`, `aggregate_type`, `aggregate_id`, each
a value or a list. Capability protocols in `omni_box.core.protocols.features`:
`SupportsBulkOperations` (`bulk_create`, `bulk_mark_completed`, `bulk_mark_failed`,
`bulk_release_locks`), `SupportsDistributedLocking` (`fetch_and_lock_pending`,
`refresh_lock`, `release_lock`, `force_unlock`), `SupportsRetentionPolicies`
(`delete_old_completed`, `release_stale_locks`). The builder accepts either the structural
match or the `capabilities` flags.

### High-level services

| Class | Constructor | Methods |
|---|---|---|
| `OutboxPublisher` | `(repo, broker, metrics=None, publish_timeout=30.0, concurrency_limit=None)` | `publish_batch(worker_id, batch_size, shutdown_requested_func=None, **fetch_filters) -> BatchProcessingResult` |
| `InboxConsumerRunner` | `(consumer, transaction_provider, handler=None, *, worker_id, consumer_group, domain_service=None, ack_strategy=EXACTLY_ONCE_INBOX, commit_offset_policy=ON_PERSIST, exactly_once_commit_on_failed=False, process_timeout=30.0, concurrency_limit=None, metrics=None)` | `start()`, `stop()`, `run_forever()`, `process_one() -> InboxConsumeResult` |
| `OmniBoxMaintenanceService` | `(repo)` | `release_stale_locks(stale_timeout_seconds) -> int`, `cleanup_old_events(retention_days, batch_size=1000, max_iterations=10000) -> int` |
| `EventBatchProcessor` | built by the factories or the builder | `process_batch(worker_id, batch_size, shutdown_requested_func=None, **fetch_filters)` |

`InboxConsumeResult` is `(message_id, event_id, committed, processed, duplicate)`.
`BatchProcessingResult` is `(processed_event_ids, failed_counted, failed_noncounted,
remaining_event_ids, commit_failed)`; the two failure lists hold
`EventFailureUpdate(event_id, error, next_retry_at)` tuples.

Commit semantics, exactly as the runner implements them:

| `ack_strategy` | The offset is committed |
|---|---|
| `AT_MOST_ONCE` | before the transaction is even attempted |
| `AT_LEAST_ONCE` + `ON_PERSIST` | after the transaction, success or failure |
| `AT_LEAST_ONCE` + `ON_SUCCESS` | only when the handler returned a result with `processed=True` |
| `EXACTLY_ONCE_INBOX` | when there is no handler, when the handler succeeded, when it skipped, or when the row was already `completed` — and on a handler exception only if `exactly_once_commit_on_failed=True` |

### Factories (`omni_box.application.factories`)

All three return an `EventBatchProcessor` and share `metrics=None`, `dlq_storage=None`,
`enable_otel=False`, `enable_circuit_breaker=False`,
`circuit_breaker_failure_threshold=5`, `circuit_breaker_recovery_timeout=60`, `job_name`,
`additional_steps_before=None`, `additional_steps_after=None`.

* `create_outbox_processor(repo, publisher, *, publish_timeout=30.0, …)`
* `create_inbox_processor(repo, handler, *, skip_duplicate_siblings=True, filter_sources=None, process_timeout=30.0, …)`
* `create_dispatching_processor(repo, router, *, filter_sources=None, skip_duplicate_siblings=True, process_timeout=30.0, dependencies=None, …)`

### Pipeline (`omni_box.core.pipeline`)

`EventProcessorBuilder(repo)` — `add_step(step)`, `with_fetch_strategy(s)`,
`with_commit_strategy(s)`, `with_metrics(m)`, `with_lease_ttl(seconds)` (default 300),
`with_job_name(name)`, `build()`. Every `with_*` returns `self`.

A step implements `async def execute(event, context) -> StepResult` and may implement
`on_batch_start` / `on_batch_end` / `on_event_start` / `on_event_end`; subclass
`BaseProcessingStep` to get no-op hooks. `StepResult.next()` continues,
`StepResult.skip()` drops the rest of the pipeline for this event, `StepResult.stop()`
ends the whole batch.

| Step (`omni_box.core.pipeline.steps`) | What it does |
|---|---|
| `HandlerExecutionStep(handler, timeout=30.0)` | awaits the handler, turns the outcome into `mark_completed` / `mark_failed` / `mark_skipped` on the context. Every processor needs one |
| `SiblingDeduplicationStep(enabled=True)` | skips the event when a *completed* row shares its `(message_id, consumer_group)`. Inbox only |
| `MetricsStep(metrics)` | emits counters and durations at `on_batch_end` |
| `OpenTelemetryStep(service_name="omni-box")` | one span per event; needs the `opentelemetry` extra, silently inert without it |
| `CircuitBreakerStep(failure_threshold=5, recovery_timeout_seconds=60)` | stops the batch after N consecutive failures. In-process state |
| `DLQStep(dlq_storage)` | on the last counted attempt, calls `dlq_storage.move_to_dlq(event, error)` |

Fetch strategies: `DistributedLockingFetchStrategy(ttl=300)`,
`OptimisticLockingFetchStrategy()`, `FilteredFetchStrategy(sources=None, ttl=300)`.
Commit strategies: `BulkCommitStrategy()`, `SingleCommitStrategy()`.

### Handler results (`omni_box.core.services.results`)

A handler returns `None` (treated as success) or an `EventHandlerResult(success, processed=True,
status=None, error_message=None, count_as_attempt=True, next_retry_at=None)`. Three
constructors are re-exported at the top level:

| Call | Effect on the row |
|---|---|
| `handler_completed(status=COMPLETED)` | `completed` |
| `handler_skipped(status=SKIPPED)` | skipped — not completed, not failed, `attempts_made` untouched |
| `handler_retry(message, *, count_as_attempt=True, next_retry_at=None, status=RETRY)` | failure; `attempts_made + 1` unless you say otherwise |

`EventHandlerStatus` is `COMPLETED`, `STALE`, `SKIPPED`, `FAILED`, `RETRY`.

### Routing (`omni_box.core.dispatch`)

`EventRouter(normalize_topic=None)` keys handlers by `(topic, event_type, schema_version)`.
`register_handler(event_type, topic, handler, schema_version=None, handler_name=None)`
registers a function; `register_class(cls, topic=None)` / `register_instance(obj, topic=None)`
sweep a `BaseEventHandler` subclass for methods marked with
`@event_handler(event_type, topic=None, schema_version=None)`. `dispatch(event, topic, repo,
**dependencies)` tries the exact `schema_version`, then a registered migration
(`BaseEventSchema.register_migration`), then the version-agnostic entry, and returns a
failed `EventHandlerResult` when nothing matches. `create_dispatching_processor` passes
`event.source` as the topic.

### PostgreSQL (`omni_box.infra.storage.postgres`, extra `postgres`)

Abstract ORM bases — bind them to your own `DeclarativeBase`: `OutboxEventDBBase`,
`InboxEventDBBase`, `OutboxEventPartitionedDBBase`, `InboxEventPartitionedDBBase`, the
mixin `EventMixin`, and the helpers `get_event_constraints(table_name,
include_created_at_in_unique=False)` and `UnConstrainedEnum`.

`PostgresOutboxRepository(session, *, model_class, conflict_index_id=None,
conflict_index_idempotency=None, batch_size=1000, error_max_length=2000,
truncation_suffix=…, scheduled_at_skew_seconds=60)` and `PostgresInboxRepository(...)` with
the same keywords. Both implement every capability protocol, and both expose
`requeue_failed(event_id) -> bool` from the shared base — the only way to move a `failed`
row back to `pending`.

### Kafka (`omni_box.infra.brokers.kafka`, extra `kafka`)

`KafkaEventPublisher(producer, converter, *, max_infra_retries=3)` — you own the
`AIOKafkaProducer` lifecycle; set `enable_idempotence=True` and `acks="all"` on it.
`KafkaEventConsumer(consumer, *, payload_loader=None, message_id_getter=None,
event_type_getter=None, source_getter=None, envelope_parser=None)` — you own the
`AIOKafkaConsumer` and should set `enable_auto_commit=False`. Without a
`message_id_getter` the message id is the `message_id` or `event_id` header, falling back
to `"{topic}:{partition}:{offset}"`.

Converters (`omni_box.core.converters`): `RawEventConverter` (the payload as-is),
`SchemaVersionedConverter` (`schema_version` + `payload`), `EnvelopeEventConverter(
default_schema_version="1.0.0")` (adds `event_type`, aggregate identity, timestamp,
tracing ids).

### Elsewhere

`omni_box.infra.metrics` (extra `metrics`) — `PrometheusOutboxMetrics(prefix=None)`,
`PrometheusInboxMetrics(prefix=None)`. `omni_box.contrib.settings` (extra `settings`) —
`BaseOutboxSettings` / `BaseInboxSettings`, reading `OMNI_OUTBOX_` / `OMNI_INBOX_` with
`__` as the nesting delimiter. `omni_box.contrib.dishka` (extra `dishka`) —
`EventDispatcherProvider`, `DIAwareEventRouter`, `create_di_router`.
`omni_box.testing` — `assert_outbox_event_created`. `omni_box.utils` — `utc_now`,
`calculate_backoff_with_jitter`, `ErrorClassifier`.

## Rules that hold or break the code

1. **The library never opens or commits a database transaction.** Every repository call
   runs in the session you handed it. `publish_batch` and `process_batch` must be wrapped
   in a transaction *you* commit — the fetch, the lock, the publish and the status update
   are one unit. Forget the commit and nothing happened: the rows are still `pending` and
   the next cycle publishes them again.
2. **Delivery is at-least-once in both directions.** The broker send happens before the
   commit that marks the row `completed`, so a crash in between republishes. That is the
   point of the inbox on the other side; it is not a defect to work around.
3. **The inbox deduplication key is `(message_id, consumer_group)`, and the window is the
   lifetime of the row.** It is a unique index, not a time window: it holds for as long as
   the row is in the table and not one moment longer. `cleanup_old_events(retention_days=N)`
   or dropping a partition removes the row, and a message redelivered after that is new
   again. Pick `retention_days` from your broker's retention, not from disk pressure.
   Change `consumer_group` and every message in flight is new too.
4. **A handler passed to `InboxConsumerRunner` runs inside the transaction that inserts the
   inbox row.** If it raises, the insert rolls back with it — there is no `pending` row left
   behind to retry from, and the retry has to come from the broker. That is the exactly-once
   *effect* the pattern gives you: the side effect and the record of it commit together.
5. **The runner never records a failure.** It calls neither `mark_failed` nor anything that
   increments `attempts_made`, so `max_attempts` does nothing on that path. The retry budget
   only exists for the batch processors (`create_*_processor`, `EventBatchProcessor`).
6. **`worker_id` is validated** by the PostgreSQL fetch-and-lock path: `[A-Za-z0-9.\-/:]`
   only, at most 255 characters. An underscore or a space raises `ValueError` at fetch time,
   not at construction. `worker-1` is fine, `worker_1` is not.
7. **`payload` must be a non-empty JSON object** of at most 1 000 000 bytes, with no `NaN`
   and no `Infinity`. `payload={}` raises. Everything is normalised through `orjson`, so what
   you read back is what will go on the wire.
8. **Entities are frozen.** Every `OmniBoxDomainService` method returns a *new* event; none
   of them touch the database. They are for services that keep events in memory. The
   repository methods (`mark_completed`, `mark_failed`, …) are what changes a row, and they
   take an id, not an entity.
9. **`attempts_made == max_attempts` means `failed`, and `failed` is terminal.** The default
   budget is 6. `fetch_pending` and `fetch_and_lock_pending` both filter on
   `attempts_made < max_attempts`, so a failed row is never picked up again;
   `PostgresEventRepository.requeue_failed(event_id)` resets it.
10. **`create()` returns the row that is in the table.** On a duplicate — the same
    `(message_id, consumer_group)`, or the same `idempotency_key` — you get the *existing*
    row back, with its own `id` and status, and no exception. Compare
    `returned.id == event.id` if you need to know whether you were the writer.
11. **`SiblingDeduplicationStep` is a no-op on a non-partitioned table.**
    `PostgresInboxRepository.has_completed_sibling_for_inbox_key` returns `False` unless the
    dedup key carries `created_at`, because on a plain table the unique index already makes a
    sibling impossible. It earns its keep only on the partitioned bases.
12. **On a partitioned table the unique index cannot enforce the logical key** — PostgreSQL
    requires the partition key in it, and two retries never share a `created_at`. Both
    repositories therefore serialize the identity with `pg_advisory_xact_lock` and look it up
    before inserting: one advisory lock and one `SELECT` per insert, held until *your*
    transaction ends. One more reason to keep those transactions short.
13. **Nothing classifies errors for you.** `ErrorClassifier` exists in `omni_box.utils` and
    is used only inside `KafkaEventPublisher`'s own infrastructure retry. In the pipeline,
    every exception out of a handler or a publisher is a counted failure. To spend no budget
    on a transient error, return `handler_retry(msg, count_as_attempt=False,
    next_retry_at=…)` — and `next_retry_at` is mandatory when `count_as_attempt=False`.
14. **`scheduled_at` is validated against `created_at`**: no more than 60 seconds before it,
    no more than 365 days after. A retry scheduled beyond that is rejected, not clamped.
15. **`release_stale_locks(stale_timeout_seconds)` must use a timeout comfortably larger
    than `with_lease_ttl(...)`** and than the worst-case handler runtime. Set it too low and
    maintenance unlocks rows a healthy worker is still holding.
16. **`CircuitBreakerStep` state is per process** — replicas trip independently and a
    restart forgets everything. **`DLQStep` is best-effort**: it runs outside the commit, a
    failure in `move_to_dlq` is logged and swallowed, and the event goes to `failed` anyway.
    Give the sink an idempotent key.
17. **The tables are yours.** No `Base`, no migrations, no DDL. Bind the abstract bases to
    your `DeclarativeBase` and generate the migration yourself; the repositories depend on the
    column names, so keep them.
18. **`shutdown_requested_func` is accepted and ignored.** It is forwarded from
    `publish_batch` to `process_batch` and never consulted. Handle shutdown between batches.
19. **There is no scheduler.** The relay loop, its sleep, its shutdown and the maintenance
    cadence are yours to write.

## Common mistakes

```python
# WRONG — a session with no transaction: the lock and the completion never land
session = session_factory()
repo = PostgresOutboxRepository(session, model_class=OutboxEventDB)
await OutboxPublisher(repo, broker).publish_batch(worker_id="pub-1", batch_size=100)

# RIGHT — one transaction per cycle, committed by the caller
async with session_factory() as session, session.begin():
    repo = PostgresOutboxRepository(session, model_class=OutboxEventDB)
    result = await OutboxPublisher(repo, broker).publish_batch(worker_id="pub-1", batch_size=100)
```

```python
# WRONG — underscores are not allowed in worker_id; this raises at fetch time
await publisher.publish_batch(worker_id="outbox_worker_1", batch_size=50)

# RIGHT
await publisher.publish_batch(worker_id="outbox-worker-1", batch_size=50)
```

```python
# WRONG — an empty payload, and a mutation of a frozen model
event = domain.create_outbox_event(..., payload={})     # ValidationError: payload cannot be empty
event.status = EventStatus.COMPLETED                    # ValidationError: instance is frozen

# RIGHT
event = domain.create_outbox_event(..., payload={"user_id": str(user_id)})
event = domain.mark_event_completed(event, completed_at=utc_now(), worker_id="worker-1")
```

```python
# WRONG — the decorator alone registers nothing, and it has no `source` argument
@event_handler(event_type="user.created", source="users")
async def handle_user_created(event, repo): ...

# RIGHT — either a plain function registered explicitly …
router.register_handler(event_type="user.created", topic="users", handler=handle_user_created)

# … or a class the router sweeps
class UserHandlers(BaseEventHandler):
    topic = "users"

    @event_handler("user.created")
    async def on_created(self, event: InboxEvent, repo: InboxEventRepository) -> None: ...

router.register_instance(UserHandlers())
```

```python
# WRONG — assuming the new row is yours, and that a duplicate raises
stored = await repo.create(event)
await do_side_effect(event.id)          # `event.id` may be nowhere in the table

# RIGHT — the returned row is the one that exists
stored = await repo.create(event)
if stored.status is EventStatus.COMPLETED:
    return                              # a redelivery of work already done
await do_side_effect(stored.id)
```

```python
# WRONG — expecting the runner to retry a failing handler out of the table
runner = InboxConsumerRunner(..., handler=flaky_handler)   # max_attempts does nothing here

# RIGHT — land the message, drain it with a processor that owns the retry budget
runner = InboxConsumerRunner(..., handler=None, ack_strategy=AckStrategy.AT_LEAST_ONCE)
processor = create_inbox_processor(repo=inbox_repo, handler=flaky_handler)
async with session_factory() as session, session.begin():
    await processor.process_batch(worker_id="inbox-1", batch_size=50)
```

## Errors

Everything derives from `OmniBoxError`.

| Exception | Means |
|---|---|
| `StorageError` | any backend failure; the base of the storage family |
| `StorageConnectionError` / `StorageTimeoutError` / `StorageTransactionError` / `StorageIntegrityError` | connection lost, statement or lock timeout, transaction aborted, constraint violated |
| `EventNotLockedError` | an operation needing a lock ran on an unlocked event |
| `EventLockedByAnotherWorkerError` | the lock belongs to a different `worker_id` |
| `EventAlreadyLockedError` | locking an event that is already locked |
| `InvalidEventStateError` | a transition from a status that does not allow it — carries `current_status` and `expected_statuses` |
| `EventConcurrentUpdateError` | an update touched fewer rows than expected: another worker got there first, or the row is gone. Carries `expected`, `actual`, `missing_ids` |
| `UnsupportedCapabilityError` | a maintenance call on a repository without `SupportsRetentionPolicies` |
| `InboxPersistError` | the per-message inbox transaction rolled back; the offset was deliberately not committed. The underlying failure is on `.cause` |

Outside the top-level namespace: `SchemaResolutionError` (`omni_box.core.models.schemas`,
also a `ValueError`) when `BaseEventSchema.resolve` finds no schema;
`HandlerAlreadyRegisteredError` and its base `DispatcherError`
(`omni_box.core.dispatch.exceptions`); `PipelineStoppedError` and its base `PipelineError`
(`omni_box.core.pipeline.exceptions`), raised internally when a step returns
`StepResult.stop()` and caught by the pipeline.

## Documentation map

Fetch a page when the task is the one named beside it.

| Page | Read it when |
|---|---|
| [Home](index.md) | picking extras, or a one-screen summary of what the library is |
| [Quick start](guide/quickstart.md) | wiring the first integration end to end |
| [User guide](user_guide.md) | the three ways to consume, converters, observability, maintenance |
| [Configuration](guide/configuration.md) | the knobs on every component, and the `pydantic-settings` helpers |
| [Advanced](guide/advanced.md) | custom steps and strategies, partitioned tables, a DLQ sink, another broker |
| [Architecture](architecture.md) | the layer boundaries, the batch cycle, commit semantics |
| [Storage adapters](storage_adapters.md) | writing a repository for something that is not PostgreSQL |
| [Migrations & DDL](migrations.md) | the exact schema, indexes, constraints, and the partitioned variants |
| [Troubleshooting](troubleshooting.md) | stale locks, duplicates, a tripped breaker, offsets that will not move |
| [API reference](api_reference.md) | the hand-written listing of the public surface |
| [Generated API](reference/index.md) | an exact signature or docstring — HTML only, see above |
| [Changelog](changelog.md) | what changed between versions |
