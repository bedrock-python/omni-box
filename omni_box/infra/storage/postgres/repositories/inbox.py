"""PostgreSQL inbox event repository implementation."""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

import structlog
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.exceptions import EventConcurrentUpdateError, StorageError, StorageIntegrityError
from omni_box.core.models.entities import InboxEvent
from omni_box.core.models.enums import EventStatus
from omni_box.core.protocols import InboxEventRepository

from ..orm import InboxEventDBBase, InboxEventPartitionedDBBase
from .base import PostgresEventRepository

_InboxDBType = InboxEventDBBase | InboxEventPartitionedDBBase

logger = structlog.get_logger(__name__)


class PostgresInboxRepository(
    PostgresEventRepository[InboxEvent, InboxEventDBBase | InboxEventPartitionedDBBase],
    InboxEventRepository,
):
    """PostgreSQL implementation of InboxEventRepository."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        model_class: type[InboxEventDBBase] | type[InboxEventPartitionedDBBase],
        **kwargs: Any,
    ) -> None:
        super().__init__(session, model_class=model_class, **kwargs)

    def _inbox_dedup_column_tuple(self) -> tuple[str, ...]:
        return tuple(getattr(self._model_class, "__inbox_dedup_index_columns__", ("message_id", "consumer_group")))

    async def _acquire_inbox_business_key_lock(self, message_id: str, consumer_group: str) -> None:
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(CAST(:msg AS text)), hashtext(CAST(:cg AS text)))"),
            {"msg": message_id, "cg": consumer_group},
        )

    async def create(self, event: InboxEvent) -> InboxEvent:
        if "created_at" not in self._inbox_dedup_column_tuple():
            return await super().create(event)
        try:
            await self._acquire_inbox_business_key_lock(event.message_id, event.consumer_group)
            stmt = (
                select(self._model_class)
                .where(
                    self._model_class.message_id == event.message_id,
                    self._model_class.consumer_group == event.consumer_group,
                )
                .order_by(self._model_class.created_at.asc())
                .limit(1)
            )
            existing = cast("_InboxDBType | None", (await self._session.execute(stmt)).scalar_one_or_none())
            if existing is not None:
                self._log_event_skipped(existing, event.idempotency_key)
                return self._to_entity(existing)
            db_event = await self._execute_insert(
                self._build_insert_stmt(event, self._prepare_insert_values(event)), event.id
            )
            if db_event is not None:
                self._log_event_created(event)
                return self._to_entity(db_event)
            existing = await self._fetch_existing_event(event)
            if existing:
                self._log_event_skipped(existing, event.idempotency_key)
                return self._to_entity(existing)
            raise EventConcurrentUpdateError(
                expected=1, actual=0, message=f"Failed to create/retrieve {event.id}", missing_ids=[event.id]
            )
        except IntegrityError as e:
            raise StorageIntegrityError(f"Postgres: constraint violation: {e}") from e
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: database error: {e}") from e

    async def _fetch_existing_event(self, event: InboxEvent) -> _InboxDBType | None:
        dedup_cols = list(self._inbox_dedup_column_tuple())
        stmt = select(self._model_class).where(
            self._model_class.message_id == event.message_id, self._model_class.consumer_group == event.consumer_group
        )
        if "created_at" in dedup_cols:
            stmt = stmt.where(self._model_class.created_at == event.created_at)
        result = await self._session.execute(stmt)
        return cast("_InboxDBType | None", result.scalar_one_or_none())

    async def get_by_message_id(self, message_id: str, consumer_group: str) -> InboxEvent | None:
        try:
            stmt = (
                select(self._model_class)
                .where(self._model_class.message_id == message_id, self._model_class.consumer_group == consumer_group)
                .order_by(self._model_class.created_at.desc())
                .limit(1)
            )
            result = await self._session.execute(stmt)
            db_event = cast("_InboxDBType | None", result.scalar_one_or_none())
            return self._to_entity(db_event) if db_event else None
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error fetching inbox event {message_id}: {e}") from e

    async def exists(self, message_id: str, consumer_group: str) -> bool:
        try:
            stmt = (
                select(self._model_class.id)
                .where(self._model_class.message_id == message_id, self._model_class.consumer_group == consumer_group)
                .limit(1)
            )
            return (await self._session.execute(stmt)).scalar() is not None
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error checking inbox event {message_id}: {e}") from e

    async def has_completed_sibling_for_inbox_key(
        self, message_id: str, consumer_group: str, exclude_event_id: UUID
    ) -> bool:
        if "created_at" not in self._inbox_dedup_column_tuple():
            return False
        try:
            stmt = (
                select(self._model_class.id)
                .where(
                    self._model_class.message_id == message_id,
                    self._model_class.consumer_group == consumer_group,
                    self._model_class.status == EventStatus.COMPLETED,
                    self._model_class.id != exclude_event_id,
                )
                .limit(1)
            )
            return (await self._session.execute(stmt)).scalar() is not None
        except SQLAlchemyError as e:
            raise StorageError(f"Postgres: error checking sibling for inbox {message_id}: {e}") from e

    def _prepare_insert_values(self, event: InboxEvent) -> dict[str, Any]:
        vals = super()._prepare_insert_values(event)
        vals.update({"message_id": event.message_id, "consumer_group": event.consumer_group, "source": event.source})
        return vals

    def _build_insert_stmt(self, event: InboxEvent, values: dict[str, Any]) -> Any:
        return (
            insert(self._model_class)
            .values(**values)
            .on_conflict_do_nothing(index_elements=list(self._inbox_dedup_column_tuple()))
            .returning(self._model_class)
        )

    def _to_entity(self, db_event: InboxEventDBBase | InboxEventPartitionedDBBase) -> InboxEvent:
        data = self._base_to_entity_dict(db_event)
        data.update(
            {"message_id": db_event.message_id, "consumer_group": db_event.consumer_group, "source": db_event.source}
        )
        return InboxEvent.model_validate(
            data, context={"scheduled_at_skew_seconds": self._scheduled_at_skew_seconds, "payload_trusted": True}
        )
