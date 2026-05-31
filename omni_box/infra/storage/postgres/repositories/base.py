"""Base PostgreSQL event repository implementation."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Unpack
from uuid import UUID

import sqlalchemy as sa
import structlog
from sqlalchemy import case, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.constants import (
    DEFAULT_SCHEDULE_AT_SKEW_SECONDS,
    DEFAULT_TRUNCATION_SUFFIX,
    FORCE_UNLOCK_REASON_MAX_LENGTH,
    LAST_ERROR_MAX_LENGTH,
    REASON_IDEMPOTENT_DUPLICATE,
)
from omni_box.core.exceptions import (
    EventConcurrentUpdateError,
    StorageError,
    StorageIntegrityError,
)
from omni_box.core.models.entities import BaseEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.models.types import EventFailureUpdate, PositiveInt
from omni_box.core.protocols import (
    EventRepository,
    FetchFilters,
    RepositoryCapabilities,
    SupportsBulkOperations,
    SupportsDistributedLocking,
    SupportsRetentionPolicies,
)

from ..constants import MAX_BATCH_SIZE, REPO_BATCH_SIZE
from ..orm import EventMixin

logger = structlog.get_logger(__name__)


def _as_list(value: list[str] | tuple[str, ...] | None) -> list[str] | None:
    """Normalize conflict index."""
    if value is None:
        return None
    lst = list(value)
    return lst if lst else None


class PostgresEventRepository[T: BaseEvent, M: EventMixin](
    EventRepository[T],
    SupportsBulkOperations[T],
    SupportsDistributedLocking[T],
    SupportsRetentionPolicies,
):
    """Generic PostgreSQL implementation of EventRepository."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        model_class: type[M],
        conflict_index_id: list[str] | None = None,
        conflict_index_idempotency: list[str] | None = None,
        batch_size: int = REPO_BATCH_SIZE,
        error_max_length: int = LAST_ERROR_MAX_LENGTH,
        truncation_suffix: str = DEFAULT_TRUNCATION_SUFFIX,
        scheduled_at_skew_seconds: int = DEFAULT_SCHEDULE_AT_SKEW_SECONDS,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if batch_size > MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be <= {MAX_BATCH_SIZE}, got {batch_size}")
        if error_max_length < 1:
            raise ValueError(f"error_max_length must be >= 1, got {error_max_length}")
        if scheduled_at_skew_seconds < 0:
            raise ValueError(f"scheduled_at_skew_seconds must be >= 0, got {scheduled_at_skew_seconds}")
        if len(truncation_suffix.encode("utf-8")) >= error_max_length:
            raise ValueError(
                f"truncation_suffix length ({len(truncation_suffix.encode('utf-8'))} bytes) "
                f"must be < error_max_length ({error_max_length})"
            )

        self._session = session
        self._model_class = model_class
        self._conflict_index_id = _as_list(
            conflict_index_id or getattr(model_class, "__outbox_conflict_index_id__", None)
        ) or ["id"]
        self._conflict_index_idempotency = _as_list(
            conflict_index_idempotency or getattr(model_class, "__outbox_conflict_index_idempotency__", None)
        ) or ["idempotency_key"]
        self._batch_size = batch_size
        self._error_max_length = error_max_length
        self._truncation_suffix = truncation_suffix
        self._scheduled_at_skew_seconds = scheduled_at_skew_seconds

    @property
    def capabilities(self) -> RepositoryCapabilities:
        return RepositoryCapabilities(
            supports_bulk=True,
            supports_distributed_locking=True,
            supports_retention=True,
        )

    async def create(self, event: T) -> T:
        try:
            values = self._prepare_insert_values(event)
            stmt = self._build_insert_stmt(event, values)
            db_event = await self._execute_insert(stmt, event.id)

            if db_event is not None:
                self._log_event_created(event)
                return self._to_entity(db_event)

            existing_db_event = await self._fetch_existing_event(event)
            if existing_db_event:
                self._log_event_skipped(existing_db_event, event.idempotency_key)
                return self._to_entity(existing_db_event)

            raise EventConcurrentUpdateError(
                expected=1,
                actual=0,
                message=f"Failed to create or retrieve event {event.id}",
                missing_ids=[event.id],
            )
        except IntegrityError as e:
            raise StorageIntegrityError(f"Postgres: constraint violation: {e}") from e
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: database error: {e}") from e

    async def bulk_create(self, events: list[T]) -> list[T]:
        if not events:
            return []

        with_idem = [e for e in events if e.idempotency_key]
        without_idem = [e for e in events if not e.idempotency_key]
        created_entities: list[T] = []

        async def _process_group(group: list[T], conflict_index: list[str] | None) -> None:
            if not group:
                return

            for i in range(0, len(group), self._batch_size):
                batch = group[i : i + self._batch_size]
                values = [self._prepare_insert_values(e) for e in batch]
                insert_stmt = insert(self._model_class).values(values)
                if conflict_index:
                    insert_stmt = insert_stmt.on_conflict_do_nothing(
                        index_elements=conflict_index,
                        index_where=(self._model_class.idempotency_key.is_not(None))
                        if conflict_index == self._conflict_index_idempotency
                        else None,
                    )
                else:  # pragma: no cover
                    insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=self._conflict_index_id)

                stmt = insert_stmt.returning(self._model_class)
                result = await self._session.execute(stmt)
                db_events = result.scalars().all()
                created_entities.extend([self._to_entity(db_e) for db_e in db_events])

                if len(db_events) < len(batch):
                    created_ids = {db_e.id for db_e in db_events}
                    skipped_events = [e for e in batch if e.id not in created_ids]
                    for skipped in skipped_events:
                        existing = await self._fetch_existing_event(skipped)
                        if existing:
                            created_entities.append(self._to_entity(existing))

        try:
            await _process_group(with_idem, self._conflict_index_idempotency)
            await _process_group(without_idem, self._conflict_index_id)
        except IntegrityError as e:
            raise StorageIntegrityError(f"Postgres: bulk creation constraint violation: {e}") from e
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: bulk creation database error: {e}") from e
        else:
            return created_entities

    async def get_by_id(self, event_id: UUID) -> T | None:
        try:
            result = await self._session.execute(select(self._model_class).where(self._model_class.id == event_id))
            db_event = result.scalar_one_or_none()
            return self._to_entity(db_event) if db_event else None
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error fetching event {event_id}: {e}") from e

    async def fetch_pending(self, limit: PositiveInt, **filters: Unpack[FetchFilters]) -> list[T]:
        try:
            now = func.now()
            stmt = (
                select(self._model_class)
                .where(self._model_class.status == EventStatus.PENDING)
                .where(self._model_class.locked_at.is_(None))
                .where(self._model_class.scheduled_at <= now)
                .where(self._model_class.attempts_made < self._model_class.max_attempts)
            )
            stmt = self._apply_filters(stmt, filters)
            stmt = stmt.order_by(
                self._model_class.scheduled_at,
                self._model_class.created_at,
                self._model_class.id,
            ).limit(limit)
            result = await self._session.execute(stmt)
            db_events = result.scalars().all()
            return [self._to_entity(event) for event in db_events]
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error fetching pending: {e}") from e

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        try:
            now = func.now()
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == event_id)
                .where(self._model_class.status == EventStatus.PENDING)
                .where(self._model_class.locked_at.is_(None))
                .values(locked_at=now, locked_by=worker_id)
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
            return result.scalar_one_or_none() is not None
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error marking processing: {e}") from e

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        try:
            now = func.now()
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == event_id)
                .where(self._model_class.status == EventStatus.PENDING)
                .where(self._model_class.locked_at.is_not(None))
                .where(self._model_class.locked_by == worker_id)
                .values(
                    status=EventStatus.COMPLETED,
                    completed_at=now,
                    locked_at=None,
                    locked_by=None,
                )
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
            if result.scalar_one_or_none() is None:
                raise EventConcurrentUpdateError(
                    expected=1, actual=0, message=f"Failed to mark completed {event_id}", missing_ids=[event_id]
                )
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error marking completed: {e}") from e

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None = None,
        count_as_attempt: bool = True,
    ) -> None:
        try:
            truncated_err = self._truncate_error(error)
            now = func.now()
            values: dict[str, Any] = {
                "last_error": truncated_err,
                "scheduled_at": next_retry_at or now,
                "locked_at": None,
                "locked_by": None,
            }
            if count_as_attempt:
                new_attempts = self._model_class.attempts_made + 1
                values["attempts_made"] = new_attempts
                values["status"] = case(
                    (new_attempts >= self._model_class.max_attempts, EventStatus.FAILED),
                    else_=EventStatus.PENDING,
                )
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == event_id)
                .where(self._model_class.status == EventStatus.PENDING)
                .where(self._model_class.locked_at.is_not(None))
                .where(self._model_class.locked_by == worker_id)
                .values(**values)
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
            if result.scalar_one_or_none() is None:
                raise EventConcurrentUpdateError(
                    expected=1, actual=0, message=f"Failed to mark failed {event_id}", missing_ids=[event_id]
                )
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error marking failed: {e}") from e

    async def bulk_mark_completed(self, event_ids: list[UUID], worker_id: str) -> int:
        if not event_ids:
            return 0
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("event_ids must be unique")

        sorted_event_ids = sorted(set(event_ids))
        try:
            now = func.now()
            total_updated = 0
            for i in range(0, len(sorted_event_ids), self._batch_size):
                batch = sorted_event_ids[i : i + self._batch_size]
                stmt = (
                    update(self._model_class)
                    .where(self._model_class.id.in_(batch))
                    .where(self._model_class.status == EventStatus.PENDING)
                    .where(self._model_class.locked_at.is_not(None))
                    .where(self._model_class.locked_by == worker_id)
                    .values(status=EventStatus.COMPLETED, completed_at=now, locked_at=None, locked_by=None)
                    .returning(self._model_class.id)
                )
                result = await self._session.execute(stmt)
                total_updated += len(list(result.scalars().all()))
            if total_updated != len(sorted_event_ids):
                existing_ids = await self._get_existing_ids(sorted_event_ids)
                missing_ids = sorted(set(sorted_event_ids) - set(existing_ids))
                raise EventConcurrentUpdateError(
                    expected=len(event_ids),
                    actual=total_updated,
                    message="Bulk mark completed failed",
                    missing_ids=missing_ids,
                )
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: bulk mark completed error: {e}") from e
        else:
            return total_updated

    async def bulk_mark_failed(
        self, failures: list[EventFailureUpdate], worker_id: str, count_as_attempt: bool = True
    ) -> int:
        if not failures:
            return 0
        if len(failures) != len({f.event_id for f in failures}):
            raise ValueError("event_ids in failures must be unique")

        sorted_failures = sorted(failures, key=lambda f: f.event_id)
        try:
            total_updated = 0
            for i in range(0, len(sorted_failures), self._batch_size):
                batch = sorted_failures[i : i + self._batch_size]
                values_data = [(f.event_id, self._truncate_error(f.error), f.next_retry_at) for f in batch]
                data_src = (
                    sa.values(
                        sa.column("id", sa.Uuid),
                        sa.column("error", sa.String),
                        sa.column("next_retry", sa.DateTime(timezone=True)),
                    )
                    .data(values_data)
                    .alias("data_src")
                )
                if count_as_attempt:
                    new_attempts = self._model_class.attempts_made + 1
                    stmt = (
                        update(self._model_class)
                        .where(self._model_class.id == data_src.c.id)
                        .where(self._model_class.status == EventStatus.PENDING)
                        .where(self._model_class.locked_by == worker_id)
                        .values(
                            attempts_made=new_attempts,
                            status=case(
                                (new_attempts >= self._model_class.max_attempts, EventStatus.FAILED),
                                else_=EventStatus.PENDING,
                            ),
                            scheduled_at=case(
                                (
                                    data_src.c.next_retry.is_not(None),
                                    sa.cast(data_src.c.next_retry, sa.DateTime(timezone=True)),
                                ),
                                else_=self._model_class.scheduled_at,
                            ),
                            last_error=data_src.c.error,
                            locked_at=None,
                            locked_by=None,
                        )
                        .returning(self._model_class.id)
                    )
                else:
                    stmt = (
                        update(self._model_class)
                        .where(self._model_class.id == data_src.c.id)
                        .where(self._model_class.status == EventStatus.PENDING)
                        .where(self._model_class.locked_by == worker_id)
                        .values(
                            scheduled_at=case(
                                (
                                    data_src.c.next_retry.is_not(None),
                                    sa.cast(data_src.c.next_retry, sa.DateTime(timezone=True)),
                                ),
                                else_=self._model_class.scheduled_at,
                            ),
                            last_error=data_src.c.error,
                            locked_at=None,
                            locked_by=None,
                        )
                        .returning(self._model_class.id)
                    )
                result = await self._session.execute(stmt)
                total_updated += len(list(result.scalars().all()))

            if total_updated != len(sorted_failures):
                all_ids = [f.event_id for f in sorted_failures]
                existing_ids = await self._get_existing_ids(all_ids)
                missing_ids = sorted(set(all_ids) - set(existing_ids))
                raise EventConcurrentUpdateError(
                    expected=len(failures),
                    actual=total_updated,
                    message="Bulk mark failed: some events were not updated (missing or locked by another worker)",
                    missing_ids=missing_ids,
                )
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: bulk mark failed error: {e}") from e
        else:
            return total_updated

    async def force_unlock(self, event_id: UUID, reason: str) -> bool:
        """Forcefully release a lock regardless of owner, with audit reason."""
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("Reason for force unlock cannot be empty")
        if len(normalized_reason) > FORCE_UNLOCK_REASON_MAX_LENGTH:
            raise ValueError(
                f"Reason for force unlock is too long: {len(normalized_reason)} "
                f"(max {FORCE_UNLOCK_REASON_MAX_LENGTH} chars)"
            )

        try:
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == event_id)
                .where(self._model_class.locked_at.is_not(None))
                .values(locked_at=None, locked_by=None, last_error=f"Force unlock: {normalized_reason}")
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
            updated_id = result.scalar_one_or_none()
            if updated_id is None:
                # Might already be unlocked or not exist
                raise EventConcurrentUpdateError(
                    expected=1,
                    actual=0,
                    message=f"Failed to force unlock {event_id}: not found or not locked",
                    missing_ids=[event_id],
                )
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: force unlock error: {e}") from e
        else:
            return True

    async def fetch_and_lock_pending(
        self, limit: int, worker_id: str, ttl: int | None = None, **filters: Unpack[FetchFilters]
    ) -> list[T]:
        if limit < 1:
            raise ValueError("limit must be greater than 0")
        worker_id = self._normalize_worker_id(worker_id)
        if not worker_id:  # pragma: no cover
            raise ValueError("worker_id cannot be empty")

        try:
            now = func.now()
            lock_expired_threshold = now - timedelta(seconds=ttl) if ttl is not None else None
            lock_filter = (
                or_(self._model_class.locked_at.is_(None), self._model_class.locked_at < lock_expired_threshold)
                if ttl
                else self._model_class.locked_at.is_(None)
            )
            subq_stmt = (
                select(self._model_class.id)
                .where(self._model_class.status == EventStatus.PENDING)
                .where(lock_filter)
                .where(self._model_class.scheduled_at <= now)
                .where(self._model_class.attempts_made < self._model_class.max_attempts)
            )
            subq_stmt = self._apply_filters(subq_stmt, filters)
            subq = (
                subq_stmt.order_by(self._model_class.scheduled_at, self._model_class.created_at, self._model_class.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
                .cte("pending_events")
            )
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == subq.c.id)
                .values(locked_at=now, locked_by=worker_id)
                .returning(self._model_class)
            )
            result = await self._session.execute(stmt)
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: fetch-and-lock error: {e}") from e
        else:
            return [self._to_entity(event) for event in result.scalars().all()]

    async def refresh_lock(self, event_id: UUID, worker_id: str) -> bool:
        try:
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == event_id)
                .where(self._model_class.locked_by == worker_id)
                .where(self._model_class.locked_at.is_not(None))
                .values(locked_at=func.now())
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: lock refresh error: {e}") from e
        else:
            return result.scalar_one_or_none() is not None

    async def release_lock(self, event_id: UUID, worker_id: str) -> bool:
        try:
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == event_id)
                .where(self._model_class.locked_by == worker_id)
                .where(self._model_class.locked_at.is_not(None))
                .values(locked_at=None, locked_by=None)
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: lock release error: {e}") from e
        else:
            return result.scalar_one_or_none() is not None

    async def bulk_release_locks(self, event_ids: list[UUID], worker_id: str) -> int:
        if not event_ids:
            return 0
        try:
            total = 0
            for i in range(0, len(event_ids), self._batch_size):
                batch = event_ids[i : i + self._batch_size]
                stmt = (
                    update(self._model_class)
                    .where(self._model_class.id.in_(batch))
                    .where(self._model_class.locked_at.is_not(None))
                    .where(self._model_class.locked_by == worker_id)
                    .values(locked_at=None, locked_by=None)
                    .returning(self._model_class.id)
                )
                result = await self._session.execute(stmt)
                total += len(result.scalars().all())

            if total != len(event_ids):
                raise EventConcurrentUpdateError(
                    expected=len(event_ids),
                    actual=total,
                    message=(
                        "Bulk release locks failed: some events were not updated (missing or not locked by this worker)"
                    ),
                )
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: bulk lock release error: {e}") from e
        else:
            return total

    async def delete_old_completed(self, retention_days: int, batch_size: int = 1000) -> int:
        if retention_days < 1:
            raise ValueError("retention_days must be greater than 0")
        try:
            retention_threshold = func.now() - timedelta(days=retention_days)
            to_delete_cte = (
                select(self._model_class.id)
                .where(self._model_class.status == EventStatus.COMPLETED)
                .where(self._model_class.completed_at < retention_threshold)
                .limit(batch_size)
                .cte("to_delete")
            )
            stmt = (
                delete(self._model_class)
                .where(self._model_class.id.in_(select(to_delete_cte.c.id)))
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: cleanup error: {e}") from e
        else:
            return len(result.scalars().all())

    async def release_stale_locks(self, stale_timeout_seconds: int, max_iterations: int = 1000) -> int:
        if stale_timeout_seconds < 1:
            raise ValueError("stale_timeout_seconds must be greater than 0")
        if max_iterations < 1:
            raise ValueError("max_iterations must be greater than 0")
        try:
            threshold = func.now() - timedelta(seconds=stale_timeout_seconds)
            total = 0
            for _ in range(max_iterations):
                subq = (
                    select(self._model_class.id)
                    .where(self._model_class.status == EventStatus.PENDING)
                    .where(self._model_class.locked_at < threshold)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                    .cte("stale_events")
                )
                stmt = (
                    update(self._model_class)
                    .where(self._model_class.id == subq.c.id)
                    .values(locked_at=None, locked_by=None)
                    .returning(self._model_class.id)
                )
                result = await self._session.execute(stmt)
                count = len(result.scalars().all())
                total += count
                if count < self._batch_size:
                    break
            else:
                logger.warning(
                    "release_stale_locks hit max_iterations cap; remaining stale locks may exist",
                    max_iterations=max_iterations,
                    total_released=total,
                )
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: stale lock release error: {e}") from e
        else:
            return total

    async def requeue_failed(self, event_id: UUID) -> bool:
        """Requeue a specific failed event for retry."""
        try:
            stmt = (
                update(self._model_class)
                .where(self._model_class.id == event_id)
                .where(self._model_class.status == EventStatus.FAILED)
                .values(status=EventStatus.PENDING, attempts_made=0, last_error=None, scheduled_at=func.now())
                .returning(self._model_class.id)
            )
            result = await self._session.execute(stmt)
            return result.scalar_one_or_none() is not None
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: requeue failed error: {e}") from e

    def _apply_filters(self, stmt: sa.Select, filters: FetchFilters) -> sa.Select:
        for key, value in filters.items():
            if value is None:
                continue
            col = getattr(self._model_class, key)
            stmt = stmt.where(col.in_(value)) if isinstance(value, (list, tuple, set)) else stmt.where(col == value)
        return stmt

    def _prepare_insert_values(self, event: T) -> dict[str, Any]:
        return {
            "id": event.id,
            "event_type": event.event_type,
            "payload": event.payload,
            "headers": event.headers,
            "status": event.status,
            "attempts_made": event.attempts_made,
            "max_attempts": event.max_attempts,
            "last_error": event.last_error,
            "trace_id": event.trace_id,
            "idempotency_key": event.idempotency_key,
            "correlation_id": event.correlation_id,
            "causation_id": event.causation_id,
            "schema_version": event.schema_version,
            "created_at": event.created_at,
            "scheduled_at": event.scheduled_at,
            "completed_at": event.completed_at,
            "locked_at": event.locked_at,
            "locked_by": event.locked_by,
        }

    def _build_insert_stmt(self, event: T, values: dict[str, Any]) -> Any:
        idx = self._conflict_index_idempotency if event.idempotency_key else self._conflict_index_id
        where = (
            (self._model_class.idempotency_key.is_not(None))
            if event.idempotency_key and idx == self._conflict_index_idempotency
            else None
        )
        return (
            insert(self._model_class)
            .values(**values)
            .on_conflict_do_nothing(index_elements=idx, index_where=where)
            .returning(self._model_class)
        )

    async def _execute_insert(self, stmt: sa.Executable, event_id: UUID) -> M | None:
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def _fetch_existing_event(self, event: T) -> M | None:
        col = self._model_class.idempotency_key if event.idempotency_key else self._model_class.id
        val = event.idempotency_key if event.idempotency_key else event.id
        return (await self._session.execute(select(self._model_class).where(col == val))).scalar_one_or_none()

    async def _get_existing_ids(self, event_ids: list[UUID]) -> list[UUID]:
        return list(
            (await self._session.execute(select(self._model_class.id).where(self._model_class.id.in_(event_ids))))
            .scalars()
            .all()
        )

    def _log_event_created(self, event: T) -> None:
        logger.debug("Created event", event_id=str(event.id), event_type=event.event_type)

    def _log_event_skipped(self, db_event: EventMixin, idempotency_key: str | None) -> None:
        logger.info(
            "Event skipped: idempotent duplicate",
            event_id=str(db_event.id),
            idempotency_key=idempotency_key,
            reason=REASON_IDEMPOTENT_DUPLICATE,
        )

    # Permitted worker_id characters: letters, digits, dash, dot (k8s FQDN),
    # slash (Nomad alloc), colon (process ports). Forbidden: SQL LIKE wildcards
    # (% _), backslash, NUL byte, whitespace.
    _WORKER_ID_PATTERN = re.compile(r"^[A-Za-z0-9.\-/:]+$")
    _WORKER_ID_MAX_LENGTH = 255

    def _normalize_worker_id(self, worker_id: str) -> str:
        s = worker_id.strip()
        if not s:
            raise ValueError("worker_id cannot be empty")
        if len(s) > self._WORKER_ID_MAX_LENGTH:
            raise ValueError(f"worker_id is too long: {len(s)} > {self._WORKER_ID_MAX_LENGTH}")
        if not self._WORKER_ID_PATTERN.match(s):
            raise ValueError(f"Invalid worker_id format: {worker_id!r}")
        return s

    def _to_entity(self, db_event: M) -> T:
        raise NotImplementedError

    def _base_to_entity_dict(self, db_event: M) -> dict[str, Any]:
        return {
            "id": db_event.id,
            "event_type": db_event.event_type,
            "payload": db_event.payload,
            "headers": db_event.headers,
            "status": db_event.status,
            "attempts_made": db_event.attempts_made,
            "max_attempts": db_event.max_attempts,
            "last_error": db_event.last_error,
            "trace_id": db_event.trace_id,
            "idempotency_key": db_event.idempotency_key,
            "correlation_id": db_event.correlation_id,
            "causation_id": db_event.causation_id,
            "schema_version": db_event.schema_version,
            "created_at": db_event.created_at,
            "scheduled_at": db_event.scheduled_at,
            "completed_at": db_event.completed_at,
            "locked_at": db_event.locked_at,
            "locked_by": db_event.locked_by,
        }

    def _truncate_error(self, error: str) -> str:
        return str(BaseEvent.truncate_error(error, max_bytes=self._error_max_length, suffix=self._truncation_suffix))
