"""Omni-box implementation (Outbox & Inbox) for AIOps platform."""

from .__version__ import __version__
from .application.factories import (
    create_dispatching_processor,
    create_inbox_processor,
    create_outbox_processor,
)
from .application.services.consume import (
    AckStrategy,
    CommitOffsetPolicy,
    InboxConsumeResult,
    InboxConsumerRunner,
)
from .application.services.publish import OutboxPublisher
from .core import (
    BaseEvent,
    BaseEventSchema,
    EventAlreadyLockedError,
    EventConcurrentUpdateError,
    EventLockedByAnotherWorkerError,
    EventNotLockedError,
    EventStatus,
    InboxEvent,
    InboxEventRepository,
    InvalidEventStateError,
    OmniBoxDomainService,
    OmniBoxError,
    OmniBoxMaintenanceService,
    OutboxEvent,
    OutboxEventRepository,
    StorageConnectionError,
    StorageError,
    StorageIntegrityError,
    StorageTimeoutError,
    StorageTransactionError,
    UnsupportedCapabilityError,
)
from .core.converters import EnvelopeEventConverter
from .core.dispatch import (
    BaseEventHandler,
    EventRouter,
    event_handler,
)
from .core.exceptions import InboxPersistError
from .core.pipeline.builder import EventProcessorBuilder
from .core.pipeline.strategies import FilteredFetchStrategy
from .core.protocols import (
    AckHandle,
    ConsumedMessage,
    EnvelopeData,
    EnvelopeParser,
    EventConsumer,
    EventPublisher,
    FetchFilters,
    InboxHandler,
    NullAckHandle,
    RepositoryCapabilities,
)
from .core.protocols.metrics import InboxMetrics, OutboxMetrics
from .core.services import (
    EventBatchProcessor,
    EventHandlerResult,
    EventHandlerStatus,
    handler_completed,
    handler_retry,
    handler_skipped,
)
from .core.services.results import BatchProcessingResult

__all__ = [
    "AckHandle",
    "AckStrategy",
    "BaseEvent",
    "BaseEventHandler",
    "BaseEventSchema",
    "BatchProcessingResult",
    "CommitOffsetPolicy",
    "ConsumedMessage",
    "EnvelopeData",
    "EnvelopeEventConverter",
    "EnvelopeParser",
    "EventAlreadyLockedError",
    "EventBatchProcessor",
    "EventConcurrentUpdateError",
    "EventConsumer",
    "EventHandlerResult",
    "EventHandlerStatus",
    "EventLockedByAnotherWorkerError",
    "EventNotLockedError",
    "EventProcessorBuilder",
    "EventPublisher",
    "EventRouter",
    "EventStatus",
    "FetchFilters",
    "FilteredFetchStrategy",
    "InboxConsumeResult",
    "InboxConsumerRunner",
    "InboxEvent",
    "InboxEventRepository",
    "InboxHandler",
    "InboxMetrics",
    "InboxPersistError",
    "InvalidEventStateError",
    "NullAckHandle",
    "OmniBoxDomainService",
    "OmniBoxError",
    "OmniBoxMaintenanceService",
    "OutboxEvent",
    "OutboxEventRepository",
    "OutboxMetrics",
    "OutboxPublisher",
    "RepositoryCapabilities",
    "StorageConnectionError",
    "StorageError",
    "StorageIntegrityError",
    "StorageTimeoutError",
    "StorageTransactionError",
    "UnsupportedCapabilityError",
    "__version__",
    "create_dispatching_processor",
    "create_inbox_processor",
    "create_outbox_processor",
    "event_handler",
    "handler_completed",
    "handler_retry",
    "handler_skipped",
]
