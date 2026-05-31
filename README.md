# omni-box

Unified transactional messaging primitives for the **Transactional Outbox** and **Transactional Inbox** patterns in async Python services.

[![PyPI](https://img.shields.io/pypi/v/omni-box?color=blue)](https://pypi.org/project/omni-box/)
[![Python](https://img.shields.io/pypi/pyversions/omni-box)](https://pypi.org/project/omni-box/)
[![License](https://img.shields.io/github/license/bedrock-python/omni-box)](LICENSE)
[![CI](https://github.com/bedrock-python/omni-box/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/bedrock-python/omni-box/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bedrock-python/omni-box/graph/badge.svg)](https://codecov.io/gh/bedrock-python/omni-box)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://bedrock-python.github.io/omni-box/)

The library ships:

- Domain entities (`OutboxEvent`, `InboxEvent`) and a `OmniBoxDomainService` factory.
- Storage-agnostic repository protocols and a production-ready PostgreSQL implementation (SQLAlchemy 2 + asyncpg).
- A composable pipeline (`EventProcessorBuilder`, `EventBatchProcessor`) with built-in steps for metrics, OpenTelemetry, DLQ, circuit breaker and inbox deduplication.
- High-level orchestrators: `OutboxPublisher` (background publisher) and `InboxConsumerRunner` (broker consumer with configurable commit semantics).
- A pure-`aiokafka` broker adapter (publisher and consumer).

The library does **not** provide a Unit-of-Work implementation. The transactional boundary that ties business state with the outbox row (or with the inbox row plus side effects) is owned by the calling service.

## Requirements

- Python **3.12+** (uses PEP 695 generics).
- The core package only depends on `pydantic`, `orjson`, and `structlog`.

## Installation

```bash
pip install omni-box
```

Optional extras (declared under `[project.optional-dependencies]` in `pyproject.toml`):

| Extra | Pulls in | Used by |
| :--- | :--- | :--- |
| `postgres` | `sqlalchemy[asyncio]`, `asyncpg` | `omni_box.infra.storage.postgres` |
| `kafka` | `aiokafka` | `omni_box.infra.brokers.kafka` |
| `metrics` | `prometheus-client` | `omni_box.infra.metrics` |
| `opentelemetry` | `opentelemetry-api`, `opentelemetry-sdk` | `OpenTelemetryStep` |
| `settings` | `pydantic-settings` | `omni_box.contrib.settings` |
| `dishka` | `dishka` | `omni_box.contrib.dishka` |

Combine as needed, e.g. `pip install "omni-box[postgres,kafka,metrics]"`.

## Outbox Quick Start

```python
from omni_box import OmniBoxDomainService, OutboxPublisher
from omni_box.core.converters import EnvelopeEventConverter
from omni_box.infra.brokers.kafka import KafkaEventPublisher

# 1. Persist the event in the same DB transaction as your business state.
domain = OmniBoxDomainService()
event = domain.create_outbox_event(
    aggregate_type="user",
    aggregate_id=user_id,
    event_type="user.created",
    topic="users.events",
    partition_key=str(user_id),
    payload={"email": "user@example.com"},
)

async with uow.transaction() as tx:        # your own UoW, not part of omni-box
    await tx.users.create(user)
    await tx.outbox.create(event)

# 2. A background worker reads pending rows and publishes them.
broker = KafkaEventPublisher(producer=kafka_producer, converter=EnvelopeEventConverter())
publisher = OutboxPublisher(repo=outbox_repo, broker=broker)

while not shutdown:
    result = await publisher.publish_batch(worker_id="publisher-1", batch_size=100)
    if not result.processed_event_ids:
        await asyncio.sleep(1.0)
```

`OutboxPublisher` is defined in `omni_box.application.services.publish`. Under the hood it builds an `EventBatchProcessor` via `create_outbox_processor`, so you get fetch, lock, retry, metrics and (optionally) DLQ for free.

## Inbox Quick Start

`InboxConsumerRunner` consumes from a broker and lands every message in the inbox table inside a transaction. The transaction is opened via a user-supplied `InboxTransactionProviderProtocol`, which yields an `InboxEventRepository` bound to the open session — this keeps the library free of any UoW.

```python
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from omni_box import AckStrategy, InboxConsumerRunner
from omni_box.core.protocols import InboxEventRepository
from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol


class InboxTxProvider(InboxTransactionProviderProtocol):
    """Bridges your session/UoW to the runner."""

    def __init__(self, session_factory, repo_factory) -> None:
        self._session_factory = session_factory
        self._repo_factory = repo_factory

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[InboxEventRepository]:
        async with self._session_factory() as session, session.begin():
            yield self._repo_factory(session)


runner = InboxConsumerRunner(
    consumer=kafka_inbox_consumer,            # your EventConsumer adapter
    transaction_provider=InboxTxProvider(...),
    handler=handle_inbox_event,               # optional: process within the same tx
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

Commit semantics are configurable via `ack_strategy` (`AT_MOST_ONCE`, `AT_LEAST_ONCE`, `EXACTLY_ONCE_INBOX`) and `commit_offset_policy` (`ON_PERSIST`, `ON_SUCCESS`). See `omni_box.application.services.consume` for the full contract.

## Public API

The package re-exports its stable surface from `omni_box`:

```python
import omni_box
print(omni_box.__all__)
print(omni_box.__version__)
```

For detailed component reference see [`docs/api_reference.md`](docs/api_reference.md).

## Documentation

- [Architecture](docs/architecture.md)
- [User guide](docs/user_guide.md)
- [Migrations & DDL](docs/migrations.md)
- [Custom storage adapters](docs/storage_adapters.md)
- [Troubleshooting](docs/troubleshooting.md)
- [API reference](docs/api_reference.md)

## License

Apache 2.0 — see [`LICENSE`](LICENSE).
