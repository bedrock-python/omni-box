"""Core domain logic for Omni-box."""

from .exceptions import (
    EventAlreadyLockedError,
    EventConcurrentUpdateError,
    EventLockedByAnotherWorkerError,
    EventNotLockedError,
    InvalidEventStateError,
    OmniBoxError,
    StorageConnectionError,
    StorageError,
    StorageIntegrityError,
    StorageTimeoutError,
    StorageTransactionError,
    UnsupportedCapabilityError,
)
from .models.entities import BaseEvent, InboxEvent, OutboxEvent
from .models.enums import EventStatus
from .models.schemas import BaseEventSchema
from .protocols import InboxEventRepository, OutboxEventRepository
from .services.domain import OmniBoxDomainService
from .services.maintenance import OmniBoxMaintenanceService

__all__ = [
    "BaseEvent",
    "BaseEventSchema",
    "EventAlreadyLockedError",
    "EventConcurrentUpdateError",
    "EventLockedByAnotherWorkerError",
    "EventNotLockedError",
    "EventStatus",
    "InboxEvent",
    "InboxEventRepository",
    "InvalidEventStateError",
    "OmniBoxDomainService",
    "OmniBoxError",
    "OmniBoxMaintenanceService",
    "OutboxEvent",
    "OutboxEventRepository",
    "StorageConnectionError",
    "StorageError",
    "StorageIntegrityError",
    "StorageTimeoutError",
    "StorageTransactionError",
    "UnsupportedCapabilityError",
]
