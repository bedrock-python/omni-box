# Configuration

`omni-box` is configured per component — there is no single global settings object. The main knobs:

## `OmniBoxDomainService`

Constructor flags (all optional, defaults sourced from `omni_box.core.constants`):

- `max_attempts` — default retry budget for new events.
- `scheduled_at_skew_seconds` / `scheduled_at_max_future_seconds` — validation window for `scheduled_at`.
- `payload_max_bytes` — refuse oversized payloads.
- `headers_max_count`, `header_key_max_length`, `header_value_max_length` — header sanity limits.
- `last_error_max_length`, `truncation_suffix` — how `mark_event_failed` truncates long error strings.

## `OutboxPublisher`

```python
OutboxPublisher(
    repo,
    broker,
    metrics=None,
    publish_timeout=DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    concurrency_limit=None,
)
```

- `publish_timeout` — hard timeout for a single `broker.publish` call.
- `concurrency_limit` — wraps `publish_batch` in an `asyncio.Semaphore`.

## `InboxConsumerRunner`

Key parameters:

- `ack_strategy` — `AT_MOST_ONCE`, `AT_LEAST_ONCE`, `EXACTLY_ONCE_INBOX`.
- `commit_offset_policy` — `ON_PERSIST` (default) or `ON_SUCCESS`. Only used by `AT_LEAST_ONCE`.
- `exactly_once_commit_on_failed` — when `True`, also commit on handler failure under `EXACTLY_ONCE_INBOX` (typically combined with DLQ).
- `process_timeout` — per-message handler timeout.
- `concurrency_limit` — outer semaphore around `process_one`.

## `EventProcessorBuilder`

- `with_lease_ttl(seconds)` — lock TTL forwarded to `DistributedLockingFetchStrategy`.
- `with_job_name(name)` — used in log records and metrics labels.
- `with_metrics(metrics)` — wires `ProcessingMetrics`.
- `with_fetch_strategy` / `with_commit_strategy` — override defaults (auto-picked from repository capabilities).

## Factories

`create_outbox_processor`, `create_inbox_processor`, `create_dispatching_processor` all accept the same observability / safety options:

- `metrics`
- `dlq_storage`
- `enable_otel`
- `enable_circuit_breaker`, `circuit_breaker_failure_threshold`, `circuit_breaker_recovery_timeout`
- `additional_steps_before`, `additional_steps_after` — splice extra steps into the pipeline.

## Optional `pydantic-settings` integration

With the `settings` extra installed, `omni_box.contrib.settings` exposes ready-to-use `BaseSettings` classes for outbox/inbox runners. Inspect that module for the exact field set — these are convenience helpers, not a required entry point.

## DI integration (Dishka)

With the `dishka` extra installed, `omni_box.contrib.dishka` provides ready-made `Provider` classes. A minimal example lives in [`docs/examples/dishka_integration.py`](../examples/dishka_integration.py).
