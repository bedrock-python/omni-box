# Troubleshooting Guide

Common issues and how to diagnose them.

## 1. Stale locks

### Symptom

Rows stay in `pending` with `locked_at` / `locked_by` populated; the batch processor keeps skipping them.

### Cause

A worker crashed or was killed before clearing its lock (e.g. OOM, container kill, lost DB connection while a row was in flight).

### Resolution

Run `OmniBoxMaintenanceService.release_stale_locks` on a schedule. It requires a repository that implements `SupportsRetentionPolicies` — `PostgresOutboxRepository` and `PostgresInboxRepository` both do.

```python
from omni_box import OmniBoxMaintenanceService

maintenance = OmniBoxMaintenanceService(repo=outbox_repo)
released = await maintenance.release_stale_locks(stale_timeout_seconds=300)
```

Pick `stale_timeout_seconds` so it is comfortably larger than your handler timeout plus the broker rebalance window. Five minutes is a safe default.

## 2. Duplicate processing

### Outbox

Outbox rows are deduplicated by `idempotency_key` when it is set (the partial unique index `idx_outbox_events_idempotency_key`); otherwise by primary key. Use a stable `idempotency_key` per business action when you need at-most-once semantics on the producing side.

On the partitioned variant the unique index has to include the partition key — `(idempotency_key, created_at)` — which cannot reject a retry on its own. `PostgresOutboxRepository` therefore serializes idempotent inserts there the way the inbox does for its business key: a transaction-scoped advisory lock on the key, a lookup (earliest row wins), and only then the insert. The guarantee is the same as on a plain table, including under concurrent writers; the cost is one lock and one `SELECT` per insert that carries an `idempotency_key`. A table that already holds duplicates from before this behaviour keeps working: `create()` returns the earliest row instead of raising `MultipleResultsFound`.

### Inbox

Inbox rows are deduplicated by **`(message_id, consumer_group)`** (the unique index `idx_inbox_deduplication`). On the partitioned variant the tuple is extended with `created_at` because PostgreSQL requires the partition key in unique indexes — `PostgresInboxRepository` reads the actual columns from `__inbox_dedup_index_columns__`.

`SiblingDeduplicationStep` additionally short-circuits processing when a *completed* sibling row already exists for the same `(message_id, consumer_group)` — useful when you retry a partially failed batch.

### Operational checklist

- Confirm the unique index is present (`\d+ inbox_events`).
- Use `AckStrategy.EXACTLY_ONCE_INBOX` on `InboxConsumerRunner` if your handler is not idempotent.
- For high-fanout consumers, share a single `consumer_group` per logical subscriber — splitting groups invalidates the dedup window.

## 3. Failed events / retries

`mark_failed` accepts `count_as_attempt`:

- `count_as_attempt=True` (default) — bumps `attempts_made`. Once it reaches `max_attempts`, the row transitions to `failed` and is no longer fetched.
- `count_as_attempt=False` — does **not** bump the counter; intended for transient infrastructure errors (broker hiccup, DB timeout). You must provide `next_retry_at`.

**When a counted retry happens.** A counted failure that does not say when to retry — an exception, a handler timeout, a `handler_retry()` without `next_retry_at` — is put back with an exponential backoff: `retry_backoff_base_seconds * 2 ** attempts_made`, capped at `retry_backoff_cap_seconds`, with equal jitter (somewhere between half of that and all of it). With the defaults (10 s, 3600 s, `max_attempts=6`) the five retries come after 5–10 s, 10–20 s, 20–40 s, 40–80 s and 80–160 s, so a row reaches `failed` 2.5 to 5 minutes after its first failure, however busy the relay is. An explicit `next_retry_at` wins. `retry_backoff_base_seconds=0` on the factory or on `OutboxPublisher` retries on the next cycle instead. While it waits the row is `pending` with a `scheduled_at` in the future:

```sql
SELECT id, attempts_made, scheduled_at - now() AS retry_in, last_error
FROM outbox_events
WHERE status = 'pending' AND attempts_made > 0
ORDER BY scheduled_at;
```

On the outbox, the events written after it with the same `topic` and `partition_key` wait with it (see "Event order" below).

The `DLQStep` only considers counted failures: a transient error never routes a row to DLQ even if it superficially looks like it crossed the threshold. The DLQ move itself is **best-effort** — it runs outside the commit transaction and a failure during `move_to_dlq` is logged and swallowed. Pair the step with an idempotent sink (e.g. Kafka with a unique key) to avoid duplicates on replay.

### The attempt budget is for the row, not for the outage

An attempt is spent when the failure is about the event: a payload the broker rejects, a topic it does not have, a serialization error. A broker or a downstream that is **not there** is a property of the cycle, and spending the budget on it would turn a long outage into a backlog of terminal `failed` rows that only an operator could bring back.

So a publisher or a handler says so by raising `TransientError`. The pipeline records it without bumping `attempts_made` and reschedules the row a second ahead; `KafkaEventPublisher` raises it once `max_infra_retries` are spent on a broker that does not answer. What you see during an outage is rows sitting in `pending` with `attempts_made` unchanged and `last_error` saying the broker is unreachable — and, because the outbox step stops publishing for the rest of the cycle after the first such failure, one probe per cycle rather than one per row:

```sql
SELECT status, attempts_made, count(*), max(last_error)
FROM outbox_events
GROUP BY 1, 2 ORDER BY 1, 2;
```

When the broker answers again the next cycle publishes the backlog. `requeue_failed` stays what it always was: the operator's tool for rows that genuinely poisoned themselves, not the way out of an outage.

### Event order

`PostgresOutboxRepository.fetch_and_lock_pending` hands out the events of one `(topic, partition_key)` in the order they were written (`created_at`), one per cycle, and across any number of relays: it only takes the *head* of each key, the earliest pending event that nothing in front of it is waiting for. A key's next event waits while the one before it is due, is held by a relay, or failed and waits for its retry — a failed row holds its whole key back until it is published. Keys are independent, and a batch still carries the heads of up to `batch_size` keys.

Two exceptions, both deliberate. An event scheduled for later (`scheduled_at` in the future) that has never been tried does not hold back the events written after it — delaying an event is not a reason to delay its aggregate. And a `failed` row is terminal: its successors go out without it, and `requeue_failed` sends it later, out of order. Alert on `failed` rows if that matters to your consumers.

Symptom: one aggregate's events stop while others flow, and the oldest pending row of that key has `attempts_made > 0` and a future `scheduled_at`. That is the order guarantee at work; fix the cause of the failure, or let it run out its budget.

The fetch looks up a key's earlier pending events through the partial index `idx_outbox_events_pending_key_order` on `(topic, partition_key, created_at) WHERE status = 'pending'` (see [Migrations](migrations.md)). Without it every fetch scans the table.

Inspect genuinely failed rows directly:

```sql
SELECT id, event_type, attempts_made, max_attempts, last_error
FROM outbox_events
WHERE status = 'failed'
ORDER BY updated_at DESC
LIMIT 50;
```

## 4. Circuit breaker tripped

`CircuitBreakerStep` opens after `failure_threshold` consecutive failures and stays open for `recovery_timeout_seconds`. While open the pipeline returns `StepResult.stop()` for every event in the current batch.

**Important caveats** (see the docstring of `CircuitBreakerStep`):

- State is held **in-process**. It does not survive a restart.
- State is **not shared between workers / replicas**. Each replica trips independently.
- For multi-worker deployments add an external coordination layer (e.g. Redis-backed counters) on top of this step.

A consecutive success resets the breaker.

## 5. Inbox commit anomalies

- **Symptom**: Kafka offsets advance but `inbox_events` is empty.
  Likely cause: `ack_strategy=AT_MOST_ONCE` combined with a persistence failure. Switch to `AT_LEAST_ONCE` (with `commit_offset_policy=ON_PERSIST`) or `EXACTLY_ONCE_INBOX`.

- **Symptom**: Kafka offsets do not advance even though rows are written.
  Likely cause: `EXACTLY_ONCE_INBOX` + a failing handler. The runner only commits after success unless `exactly_once_commit_on_failed=True`. Decide whether the failure is recoverable and pick the appropriate flag.

- **Symptom**: the handler's side effect is in the database but the inbox row is not — or the effect appears twice after a redelivery.
  Likely cause: the handler opened a session of its own. Write through `repo.session`, the transaction the inbox row is in; the two then commit together, or roll back together when the handler raises.

- **`InboxPersistError` thrown by `process_one`**.
  Indicates the per-message transaction was rolled back (DB outage, integrity violation, lock conflict). The offset is intentionally not committed so the broker can redeliver. Check the cause via `error.cause`.

## 6. Performance tuning

- **Batch size**. Defaults are conservative. For PostgreSQL repositories that implement `SupportsBulkOperations`, the builder picks `BulkCommitStrategy` automatically — increasing `batch_size` becomes nearly linear.
- **Fetch strategy**. Use `DistributedLockingFetchStrategy` (`SELECT ... FOR UPDATE SKIP LOCKED`) when you run >1 worker against the same partition. The builder auto-selects it when the repository implements `SupportsDistributedLocking`.
- **Filters**. `FilteredFetchStrategy` injects fixed `source` / `topic` filters so each worker only scans its slice — combine it with partial indexes for cheap routing.
- **Lock TTL**. Set `EventProcessorBuilder.with_lease_ttl(seconds)` to cover the worst-case handler runtime and your maintenance cadence; otherwise `release_stale_locks` will start fighting healthy workers.

## 7. Observability

- Metrics: register `Prometheus*Metrics` adapters from `omni_box.infra.metrics`, or implement `InboxMetrics` / `OutboxMetrics` / `ProcessingMetrics` yourself. `MetricsStep` is auto-added by the factories when `metrics=...` is passed.
- Tracing: add `OpenTelemetryStep(service_name=...)` (requires the `opentelemetry` extra).
- Logging: the library uses `structlog`. Apply your own renderer/processors in app bootstrap.
