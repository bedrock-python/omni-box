"""Kafka consumer adapter for omni-box Inbox runner (pure aiokafka)."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import orjson
from aiokafka import AIOKafkaConsumer, TopicPartition
from pydantic import JsonValue

from ....core.protocols import (
    AckHandle,
    ConsumedMessage,
    EnvelopeData,
    EnvelopeParser,
    EventConsumer,
)

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord


class DefaultEnvelopeParser(EnvelopeParser):
    """Default envelope unwrapper for omni-box."""

    def parse(self, raw_payload: dict[str, JsonValue], headers: dict[str, str] | None) -> EnvelopeData:
        payload = raw_payload
        schema_version = (headers or {}).get("schema_version")
        trace_id = (headers or {}).get("trace_id")
        correlation_id = (headers or {}).get("correlation_id")
        causation_id = (headers or {}).get("causation_id")

        if isinstance(raw_payload, dict) and "payload" in raw_payload:
            if not schema_version:
                schema_version = cast(str | None, raw_payload.get("schema_version"))
            if not trace_id:
                trace_id = cast(str | None, raw_payload.get("trace_id"))
            if not correlation_id:
                correlation_id = cast(str | None, raw_payload.get("correlation_id"))
            if not causation_id:
                causation_id = cast(str | None, raw_payload.get("causation_id"))
            payload = cast(dict[str, JsonValue], raw_payload["payload"])

        return EnvelopeData(
            payload=payload,
            schema_version=schema_version,
            trace_id=trace_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )


class _KafkaCommitAckHandle(AckHandle):
    """Per-record commit handle (commits ``record.offset + 1`` for its topic-partition)."""

    def __init__(self, consumer: AIOKafkaConsumer, record: ConsumerRecord) -> None:
        self._consumer = consumer
        self._record = record

    async def commit(self) -> None:
        tp = TopicPartition(self._record.topic, self._record.partition)
        await self._consumer.commit({tp: self._record.offset + 1})


class KafkaEventConsumer(EventConsumer):
    """Adapter around a pre-configured ``aiokafka.AIOKafkaConsumer``.

    The caller is responsible for instantiating the consumer with the correct
    bootstrap servers, ``group_id``, topic subscriptions and (typically)
    ``enable_auto_commit=False``. This adapter only normalizes records into
    :class:`ConsumedMessage` and exposes a per-record commit ack handle.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        *,
        payload_loader: Callable[[ConsumerRecord], dict[str, JsonValue]] | None = None,
        message_id_getter: Callable[[ConsumerRecord, dict[str, str] | None], str] | None = None,
        event_type_getter: Callable[[ConsumerRecord, dict[str, JsonValue], dict[str, str] | None], str] | None = None,
        source_getter: Callable[[ConsumerRecord, dict[str, str] | None], str] | None = None,
        envelope_parser: EnvelopeParser | None = None,
    ) -> None:
        self._consumer = consumer
        self._payload_loader = payload_loader or self._default_payload_loader
        self._message_id_getter = message_id_getter or self._default_message_id_getter
        self._event_type_getter = event_type_getter or self._default_event_type_getter
        self._source_getter = source_getter or self._default_source_getter
        self._envelope_parser = envelope_parser or DefaultEnvelopeParser()

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def getone(self) -> ConsumedMessage:
        record = await self._consumer.getone()
        headers = self._decode_headers(record)
        raw_payload = self._payload_loader(record)
        message_id = self._message_id_getter(record, headers)
        event_type = self._event_type_getter(record, raw_payload, headers)
        source = self._source_getter(record, headers)
        envelope = self._envelope_parser.parse(raw_payload, headers)

        return ConsumedMessage(
            message_id=message_id,
            source=source,
            event_type=event_type,
            payload=envelope.payload,
            headers=headers,
            trace_id=envelope.trace_id,
            correlation_id=envelope.correlation_id,
            causation_id=envelope.causation_id,
            schema_version=envelope.schema_version,
            ack_handle=_KafkaCommitAckHandle(self._consumer, record),
            raw_message=record,
        )

    @staticmethod
    def _decode_headers(record: ConsumerRecord) -> dict[str, str] | None:
        if not record.headers:
            return None
        decoded: dict[str, str] = {}
        for k, v in record.headers:
            if v is not None:
                decoded[k] = v.decode("utf-8", errors="replace")
        return decoded or None

    @staticmethod
    def _default_payload_loader(record: ConsumerRecord) -> dict[str, JsonValue]:
        v = record.value
        if v is None:
            raise ValueError("Empty payload")
        payload: Any = orjson.loads(v) if isinstance(v, bytes) else v
        if not isinstance(payload, dict):
            raise TypeError("Payload must be dict")
        return cast(dict[str, JsonValue], payload)

    @staticmethod
    def _default_message_id_getter(record: ConsumerRecord, headers: dict[str, str] | None) -> str:
        if headers:
            mid = headers.get("message_id") or headers.get("event_id")
            if mid:
                return mid
        return f"{record.topic}:{record.partition}:{record.offset}"

    @staticmethod
    def _default_event_type_getter(
        record: ConsumerRecord, payload: dict[str, JsonValue], headers: dict[str, str] | None
    ) -> str:
        if headers and headers.get("event_type"):
            return headers["event_type"]
        pet = payload.get("event_type")
        if isinstance(pet, str) and pet.strip():
            return pet
        return str(record.topic)

    @staticmethod
    def _default_source_getter(record: ConsumerRecord, headers: dict[str, str] | None) -> str:
        if headers:
            src = headers.get("source") or headers.get("source_service")
            if src:
                return src
        return str(record.topic)


__all__ = ["DefaultEnvelopeParser", "KafkaEventConsumer"]
