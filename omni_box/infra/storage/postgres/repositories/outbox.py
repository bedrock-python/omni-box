"""PostgreSQL outbox event repository implementation."""

from __future__ import annotations

from typing import Any

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.protocols import OutboxEventRepository

from ..orm import OutboxEventDBBase, OutboxEventPartitionedDBBase
from .base import PostgresEventRepository


class PostgresOutboxRepository(
    PostgresEventRepository[OutboxEvent, OutboxEventDBBase | OutboxEventPartitionedDBBase],
    OutboxEventRepository,
):
    """PostgreSQL implementation of OutboxEventRepository.

    ``fetch_and_lock_pending`` keeps the order in which events were written per
    ``(topic, partition_key)``: it hands out only the head of each key, a row no
    earlier pending row of the same key is waiting in front of.
    """

    _ordering_key_columns = ("topic", "partition_key")

    def _prepare_insert_values(self, event: OutboxEvent) -> dict[str, Any]:
        vals = super()._prepare_insert_values(event)
        vals.update(
            {
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "topic": event.topic,
                "partition_key": event.partition_key,
            }
        )
        return vals

    def _to_entity(self, db_event: OutboxEventDBBase | OutboxEventPartitionedDBBase) -> OutboxEvent:
        data = self._base_to_entity_dict(db_event)
        data.update(
            {
                "aggregate_type": db_event.aggregate_type,
                "aggregate_id": db_event.aggregate_id,
                "topic": db_event.topic,
                "partition_key": db_event.partition_key,
            }
        )
        return OutboxEvent.model_validate(
            data, context={"scheduled_at_skew_seconds": self._scheduled_at_skew_seconds, "payload_trusted": True}
        )
