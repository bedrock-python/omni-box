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

The `DLQStep` only considers counted failures: a transient error never routes a row to DLQ even if it superficially looks like it crossed the threshold. The DLQ move itself is **best-effort** — it runs outside the commit transaction and a failure during `move_to_dlq` is logged and swallowed. Pair the step with an idempotent sink (e.g. Kafka with a unique key) to avoid duplicates on replay.

Inspect failed rows directly:

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
