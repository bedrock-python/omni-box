"""Kafka event publisher implementation using aiokafka."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import orjson
import structlog
from aiokafka import AIOKafkaProducer

from ....core.converters.event import EventConverter
from ....core.models.entities import OutboxEvent
from ....core.protocols import EventPublisher
from ....utils.backoff import ErrorClassifier, calculate_backoff_with_jitter

if TYPE_CHECKING:
    from ....core.protocols import EventRepository

logger = structlog.get_logger(__name__)


class KafkaEventPublisher(EventPublisher):
    """Kafka publisher that converts OutboxEvent and sends via aiokafka.

    Notes:
        Caller is responsible for ``AIOKafkaProducer`` lifecycle (``start``/``stop``).
        For at-least-once delivery configure the producer with
        ``enable_idempotence=True`` and ``acks="all"``.
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
                classification = ErrorClassifier.classify(e)
                if classification.is_transient and attempt < self._max_infra_retries:
                    delay = calculate_backoff_with_jitter(attempt)
                    logger.warning(
                        "Kafka retry", event_id=str(event.id), attempt=attempt + 1, delay=delay, error=str(e)
                    )
                    await asyncio.sleep(delay)
                    continue
                raise
            else:
                return

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
