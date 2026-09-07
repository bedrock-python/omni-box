"""Kafka event publisher implementation using aiokafka."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import orjson
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import (
    KafkaConnectionError,
    KafkaTimeoutError,
    NodeNotReadyError,
    RequestTimedOutError,
    UnknownTopicOrPartitionError,
)

from ....core.converters.event import EventConverter
from ....core.exceptions import TransientError
from ....core.models.entities import OutboxEvent
from ....core.protocols import EventPublisher
from ....utils.backoff import ErrorClassifier, calculate_backoff_with_jitter

if TYPE_CHECKING:
    from ....core.protocols import EventRepository

logger = structlog.get_logger(__name__)

# The aiokafka errors that mean the broker is not answering. aiokafka's own
# ``retriable`` flag is wider than this: it also covers a topic that does not
# exist, which is about the row and not about the broker.
_BROKER_UNREACHABLE: tuple[type[BaseException], ...] = (
    KafkaConnectionError,
    KafkaTimeoutError,
    NodeNotReadyError,
    RequestTimedOutError,
)


def _describe(exc: BaseException) -> str:
    """``TypeName: message``, without repeating a name the exception already prints.

    aiokafka's errors stringify as ``NodeNotReadyError: node 1 is not ready``
    on their own, and one of them carrying no message stringifies as the name
    alone; the builtins print the message only.
    """
    name = type(exc).__name__
    detail = str(exc)
    if not detail or detail == name:
        return name
    if detail.startswith(f"{name}: "):
        return detail
    return f"{name}: {detail}"


class KafkaEventPublisher(EventPublisher):
    """Kafka publisher that converts OutboxEvent and sends via aiokafka.

    Notes:
        Caller is responsible for ``AIOKafkaProducer`` lifecycle (``start``/``stop``).
        For at-least-once delivery configure the producer with
        ``enable_idempotence=True`` and ``acks="all"``.

        A broker that does not answer -- a connection or node error, a request
        or client timeout, or a topic it cannot fetch metadata for while it
        ignores a metadata request as well -- is retried ``max_infra_retries``
        times and then raised as ``TransientError``, which the outbox pipeline
        records without spending an attempt. Everything else, a payload or a
        topic the broker rejects included, is raised as is and counts.
    """

    def __init__(
        self,
        producer: AIOKafkaProducer,
        converter: EventConverter,
        *,
        max_infra_retries: int = 3,
    ) -> None:
        self._producer = producer
        self._converter = converter
        self._max_infra_retries = max_infra_retries

    async def publish(self, event: OutboxEvent, repo: EventRepository[OutboxEvent]) -> None:
        # ``repo`` is required by the publisher protocol so it stays
        # signature-compatible with handler steps. Kafka publishing does not
        # need repository access.
        value_dict = self._converter.convert(event)
        value_bytes = orjson.dumps(value_dict)
        key_bytes = event.partition_key.encode("utf-8") if event.partition_key else None
        headers = self._build_headers(event)
        encoded_headers: list[tuple[str, bytes]] = [(k, v.encode("utf-8")) for k, v in headers.items()]

        for attempt in range(self._max_infra_retries + 1):
            try:
                await self._producer.send_and_wait(
                    topic=event.topic,
                    value=value_bytes,
                    key=key_bytes,
                    headers=encoded_headers,
                )
            except Exception as e:
                reason = await self._unreachable_reason(e, event.topic)
                if reason is None:
                    raise
                if attempt < self._max_infra_retries:
                    delay = calculate_backoff_with_jitter(attempt)
                    logger.warning(
                        "Kafka retry", event_id=str(event.id), attempt=attempt + 1, delay=delay, error=reason
                    )
                    await asyncio.sleep(delay)
                    continue
                raise TransientError(reason) from e
            else:
                return

    async def _unreachable_reason(self, exc: Exception, topic: str) -> str | None:
        """Describe ``exc`` when it means the broker is not answering; ``None`` when it is about the event."""
        if ErrorClassifier.classify(exc, additional_transient=_BROKER_UNREACHABLE).is_transient:
            return f"Kafka broker unreachable: {_describe(exc)}"
        # aiokafka reports a topic it could not fetch metadata for the same way
        # whether the topic is missing or the broker is silent; a metadata
        # refresh that fails as well tells the two apart.
        if isinstance(exc, UnknownTopicOrPartitionError) and not await self._producer.client.force_metadata_update():
            return f"Kafka broker unreachable: no metadata for topic {topic!r} and no answer to a metadata request"
        return None

    def _build_headers(self, event: OutboxEvent) -> dict[str, str]:
        headers = dict(event.headers or {})
        headers["event_id"] = str(event.id)
        headers["event_type"] = event.event_type
        if event.schema_version:
            headers["schema_version"] = event.schema_version
        if event.trace_id:
            headers["trace_id"] = event.trace_id
        if event.correlation_id:
            headers["correlation_id"] = event.correlation_id
        if event.causation_id:
            headers["causation_id"] = event.causation_id
        return headers


__all__ = ["KafkaEventPublisher"]
