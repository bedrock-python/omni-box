# Omni-Box Documentation

`omni-box` is a Python 3.12+ library that implements the **Transactional Outbox** and **Transactional Inbox** patterns for async services.

## Contents

- [Architecture](architecture.md) — layers, pipeline, commit semantics.
- [User guide](user_guide.md) — quick start, customisation, observability.
- [Storage adapters](storage_adapters.md) — protocols and how to write a new backend.
- [Migrations & DDL](migrations.md) — PostgreSQL schema details.
- [Troubleshooting](troubleshooting.md) — common issues and tuning tips.
- [API reference](api_reference.md) — public surface re-exported from `omni_box`.

## Installation

```bash
pip install omni-box
```

### Optional extras

| Extra | Purpose |
| :--- | :--- |
| `postgres` | SQLAlchemy 2 + asyncpg adapter (`omni_box.infra.storage.postgres`). |
| `kafka` | Pure `aiokafka` publisher/consumer (`omni_box.infra.brokers.kafka`). |
| `metrics` | Prometheus implementations of the metrics protocols. |
| `opentelemetry` | Enables `OpenTelemetryStep`. |
| `settings` | `pydantic-settings` helpers in `omni_box.contrib.settings`. |
| `dishka` | DI providers in `omni_box.contrib.dishka`. |

The Kafka adapter talks to `aiokafka` directly — no third-party publisher/consumer wrapper is required.

## Key features

- **Transactional Outbox** — guarantee reliable event publishing tied to your DB transaction.
- **Transactional Inbox** — effectively-once processing via the unique `(message_id, consumer_group)` index plus `EXACTLY_ONCE_INBOX` commit semantics.
- **Pipeline architecture** — `EventProcessorBuilder` composes built-in or custom steps and strategies.
- **No UoW lock-in** — the library never opens transactions; the caller (or the `InboxTransactionProviderProtocol`) owns the session.
- **Observability hooks** — `InboxMetrics`, `OutboxMetrics`, `ProcessingMetrics`, OpenTelemetry, structured logging.
