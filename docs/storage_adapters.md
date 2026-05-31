# Storage Adapters

`omni-box` keeps storage out of the core via a small set of `typing.Protocol`s. The bundled PostgreSQL adapter is one implementation; you can add others (e.g. MongoDB, ClickHouse, an in-memory test double) by satisfying the same protocols.

## Protocol hierarchy

All protocols live in `omni_box.core.protocols.repository`.

```python
from omni_box.core.protocols import (
    EventRepository,
    OutboxEventRepository,
    InboxEventRepository,
    FetchFilters,
    RepositoryCapabilities,
)
```

### `EventRepository[T: BaseEvent]`

The common base used by every batch processor.

| Method | Purpose |
| :--- | :--- |
| `capabilities` (property) | Returns `RepositoryCapabilities` flags (`supports_bulk`, `supports_distributed_locking`, `supports_retention`). |
| `create(event)` | Persist a new event row. |
| `get_by_id(event_id)` | Fetch by primary key. |
| `fetch_pending(limit, **filters)` | Return PENDING rows ready to process. `FetchFilters` is a typed dict with `source`, `topic`, `aggregate_type`, `aggregate_id`. |
| `mark_processing(event_id, worker_id)` | Take an optimistic lock. |
| `mark_completed(event_id, worker_id)` | Transition to COMPLETED. |
| `mark_failed(event_id, error, worker_id, next_retry_at, count_as_attempt)` | Transition to FAILED or reschedule. `count_as_attempt=False` skips bumping `attempts_made` for transient errors. |

### `OutboxEventRepository`

Equivalent to `EventRepository[OutboxEvent]`. No extra methods are required.

### `InboxEventRepository`

Extends `EventRepository[InboxEvent]` with three methods that the inbox runner and `SiblingDeduplicationStep` rely on:

```python
async def get_by_message_id(self, message_id: str, consumer_group: str) -> InboxEvent | None: ...
async def exists(self, message_id: str, consumer_group: str) -> bool: ...
async def has_completed_sibling_for_inbox_key(
    self, message_id: str, consumer_group: str, exclude_event_id: UUID,
) -> bool: ...
```

### Capability protocols

Live in `omni_box.core.protocols.features`. Implement only what your storage can do efficiently.

- **`SupportsBulkOperations[T]`** — `bulk_create`, `bulk_mark_completed`, `bulk_mark_failed`, `bulk_release_locks`. Enables `BulkCommitStrategy` (auto-picked by the builder).
- **`SupportsDistributedLocking[T]`** — `fetch_and_lock_pending`, `refresh_lock`, `release_lock`, `force_unlock`. Enables `DistributedLockingFetchStrategy` (e.g. `SELECT ... FOR UPDATE SKIP LOCKED`).
- **`SupportsRetentionPolicies`** — `delete_old_completed`, `release_stale_locks`. Required by `OmniBoxMaintenanceService`.

Either implement the protocol structurally, *or* return a `RepositoryCapabilities` value from the `capabilities` property — the builder checks both. The structural check uses `runtime_checkable`, so a concrete class is detected automatically as long as method signatures match.

## Built-in PostgreSQL adapter

Located in `omni_box.infra.storage.postgres` (extra: `pip install "omni-box[postgres]"`).

| Module | Contents |
| :--- | :--- |
| `omni_box.infra.storage.postgres.orm` | Abstract ORM bases and mixins (no `Base` is provided — bring your own `DeclarativeBase`). |
| `omni_box.infra.storage.postgres.repositories.outbox` | `PostgresOutboxRepository` (implements bulk, distributed locking, retention). |
| `omni_box.infra.storage.postgres.repositories.inbox` | `PostgresInboxRepository` (adds inbox-specific deduplication via `INSERT ... ON CONFLICT DO NOTHING`). |
| `omni_box.infra.storage.postgres.repositories.base` | `PostgresEventRepository` — shared logic used by the two above. |

Concrete table classes are owned by the consuming service so it stays in charge of metadata, naming, and Alembic registration. See [`migrations.md`](migrations.md) for example DDL/SQLAlchemy snippets.

## Writing your own adapter

1. **Pick the protocols.** At minimum implement `EventRepository[T]` (or `OutboxEventRepository` / `InboxEventRepository`). Add capability protocols you can implement cheaply.
2. **Return domain entities.** Public methods must return `OutboxEvent` / `InboxEvent`, never your underlying ORM rows. Use a private `_to_entity` mapper.
3. **Map errors.** Wrap backend-specific exceptions into `StorageError` and its subclasses from `omni_box.core.exceptions`:
   - `StorageConnectionError` — connection refused / dropped.
   - `StorageTimeoutError` — statement / lock timeout.
   - `StorageTransactionError` — transaction abort / serialization failure.
   - `StorageIntegrityError` — unique/PK/FK violations.
4. **Do not start transactions.** Repository methods run inside a session/connection owned by the caller (a UoW, the `InboxTransactionProviderProtocol`, or the test harness). Never call `commit()` yourself.
5. **Surface capabilities.** Either expose them structurally or via the `capabilities` property — `EventProcessorBuilder.build()` inspects both.

### Skeleton example

```python
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Unpack
from uuid import UUID

from omni_box.core.exceptions import StorageError, StorageIntegrityError
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.protocols import (
    FetchFilters,
    OutboxEventRepository,
    RepositoryCapabilities,
)


class MyOutboxRepository(OutboxEventRepository):
    def __init__(self, conn) -> None:
        self._conn = conn

    @property
    def capabilities(self) -> RepositoryCapabilities:
        return RepositoryCapabilities(
            supports_bulk=False,
            supports_distributed_locking=False,
            supports_retention=False,
        )

    def _to_entity(self, row) -> OutboxEvent:
        return OutboxEvent.model_validate(row)

    async def create(self, event: OutboxEvent) -> OutboxEvent:
        try:
            row = await self._conn.insert_outbox(event.model_dump(mode="json"))
        except DuplicateKey as exc:                              # backend-specific
            raise StorageIntegrityError(str(exc)) from exc
        except ConnectionLost as exc:
            raise StorageError(str(exc)) from exc
        return self._to_entity(row)

    async def get_by_id(self, event_id: UUID) -> OutboxEvent | None: ...
    async def fetch_pending(self, limit: int, **filters: Unpack[FetchFilters]) -> list[OutboxEvent]: ...
    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool: ...
    async def mark_completed(self, event_id: UUID, worker_id: str) -> None: ...
    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None,
        count_as_attempt: bool = True,
    ) -> None: ...
```

### Testing your adapter

`omni_box.testing` and the in-tree `tests/` package contain reusable fake repositories (`_FakeInboxRepo`, `_FakeOutboxRepo`) that demonstrate the minimum behavioural contract — useful when stubbing storage in service tests.
