"""Choreography-based Saga pattern implementation using omni-box."""

from __future__ import annotations

from uuid import uuid4

from omni_box import BaseEventHandler, InboxEvent, OmniBoxDomainService, OutboxEvent, event_handler
from omni_box.infra.storage.postgres import PostgresInboxRepository, PostgresOutboxRepository

# --- Domain Events (Outbox) ---


class CreateOrderCommand(OutboxEvent):
    pass


class ReserveStockCommand(OutboxEvent):
    pass


class OrderCreatedEvent(OutboxEvent):
    pass


class StockReservedEvent(OutboxEvent):
    pass


class StockReservationFailedEvent(OutboxEvent):
    pass


class OrderCancelledEvent(OutboxEvent):
    pass


# --- Saga Participant: Order Service ---


class OrderSagaParticipant(BaseEventHandler):
    """Handles order lifecycle and compensations."""

    topic = "orders"

    def __init__(self, outbox_repo: PostgresOutboxRepository):
        self.outbox = outbox_repo
        self.domain = OmniBoxDomainService()

    @event_handler("order.create_requested")
    async def on_create_requested(self, event: InboxEvent, repo: PostgresInboxRepository) -> None:
        """Step 1: Create Order in PENDING state and request stock reservation."""
        order_id = str(event.payload["order_id"])

        # 1. Save order to DB (local transaction)

        # 2. Emit command for next participant via Outbox (same transaction)
        reservation_cmd = self.domain.create_outbox_event(
            aggregate_type="order",
            aggregate_id=uuid4(),
            event_type="stock.reserve_requested",
            topic="stock",
            partition_key=order_id,
            payload={"order_id": order_id, "items": event.payload["items"]},
            correlation_id=event.correlation_id or str(event.id),
        )
        await self.outbox.create(reservation_cmd)

    @event_handler("stock.reserved")
    async def on_stock_reserved(self, event: InboxEvent, repo: PostgresInboxRepository) -> None:
        """Step 2 (Success): Stock reserved, move order to CREATED state."""
        # ...

    @event_handler("stock.reservation_failed")
    async def on_stock_failed(self, event: InboxEvent, repo: PostgresInboxRepository) -> None:
        """Step 2 (Failure): Stock failed, move order to CANCELLED state (Compensation)."""
        # ...


# --- Saga Participant: Stock Service ---


class StockSagaParticipant(BaseEventHandler):
    topic = "stock"

    def __init__(self, outbox_repo: PostgresOutboxRepository):
        self.outbox = outbox_repo
        self.domain = OmniBoxDomainService()

    @event_handler("stock.reserve_requested")
    async def on_reserve_requested(self, event: InboxEvent, repo: PostgresInboxRepository) -> None:
        """Step 2: Try to reserve stock."""
        order_id = str(event.payload["order_id"])

        success = True  # or perform stock check

        if success:
            res = self.domain.create_outbox_event(
                aggregate_type="stock",
                aggregate_id=uuid4(),
                event_type="stock.reserved",
                topic="orders",
                partition_key=order_id,
                payload={"order_id": order_id},
                correlation_id=event.correlation_id,
            )
        else:
            res = self.domain.create_outbox_event(
                aggregate_type="stock",
                aggregate_id=uuid4(),
                event_type="stock.reservation_failed",
                topic="orders",
                partition_key=order_id,
                payload={"order_id": order_id, "reason": "out of stock"},
                correlation_id=event.correlation_id,
            )

        await self.outbox.create(res)
