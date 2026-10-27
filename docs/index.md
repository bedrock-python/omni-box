# Omni-Box Documentation

`omni-box` is a Python 3.12+ library that implements the **Transactional Outbox** and **Transactional Inbox** patterns for async services.

## Contents

- [Architecture](architecture.md) — layers, pipeline, commit semantics.
- [User guide](user_guide.md) — quick start, customisation, observability.
- [Storage adapters](storage_adapters.md) — protocols and how to write a new backend.
- [Migrations & DDL](migrations.md) — PostgreSQL schema details.
- [Troubleshooting](troubleshooting.md) — common issues and tuning tips.
- [API reference](api_reference.md) — public surface re-exported from `omni_box`.

## When to use it, and when not

omni-box fits an async Python 3.12+ service that writes to PostgreSQL through SQLAlchemy and talks to Kafka, and wants both halves of the pattern from one library: an outbox relay that keeps each key's events in order across any number of relay processes, and an inbox that deduplicates on the consumer side, with retries, a DLQ, a circuit breaker, Prometheus and OpenTelemetry as pipeline steps.

Something else is probably the better fit when:

- **You are on Django.** [django-outbox-pattern](https://github.com/juntossomosmais/django-outbox-pattern) and [django-jaiminho](https://github.com/loadsmart/django-jaiminho) are built around Django's ORM and transactions. omni-box is async SQLAlchemy only.
- **You already use FastStream.** [faststream-outbox](https://github.com/modern-python/faststream-outbox) keeps the outbox in a PostgreSQL table and relays through FastStream to Kafka, RabbitMQ, NATS or Redis. omni-box ships a Kafka adapter only; another broker means implementing `EventPublisher` and `EventConsumer` yourself.
- **You need Python below 3.12, or a database other than PostgreSQL.** omni-box uses PEP 695 generics, and the only repository it ships is for PostgreSQL. The repository protocols let you write your own ([storage adapters](storage_adapters.md)).
- **CDC is already in your stack.** If Debezium or a similar tool reads your WAL, its outbox routing replaces the polling relay. The inbox half can still be useful on the consumer side.
- **You want event sourcing**, not just reliable publishing. [eventsourcing](https://github.com/pyeventsourcing/eventsourcing) is built for that.

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
