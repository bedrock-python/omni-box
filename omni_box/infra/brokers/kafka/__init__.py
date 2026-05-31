"""Kafka-based broker implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .consumer import KafkaEventConsumer
    from .publisher import KafkaEventPublisher

__all__ = ["KafkaEventConsumer", "KafkaEventPublisher"]

try:
    from .consumer import KafkaEventConsumer
except ImportError:
    pass

try:
    from .publisher import KafkaEventPublisher
except ImportError:
    pass
