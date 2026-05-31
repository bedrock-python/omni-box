"""Integration tests for ``KafkaEventConsumer`` against a real Kafka broker."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import cast
from uuid import uuid4

import orjson
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from pydantic import JsonValue

from omni_box.core.protocols import ConsumedMessage, EnvelopeData, EnvelopeParser
from omni_box.infra.brokers.kafka.consumer import DefaultEnvelopeParser, KafkaEventConsumer

pytestmark = pytest.mark.integration

KafkaConsumerFactory = Callable[..., Awaitable[AIOKafkaConsumer]]
_RECV_TIMEOUT = 10.0


async def _send_raw(
    producer: AIOKafkaProducer,
    *,
    topic: str,
    value: dict[str, object],
    key: bytes | None = None,
    headers: list[tuple[str, bytes]] | None = None,
) -> None:
    await producer.send_and_wait(
        topic=topic,
        value=orjson.dumps(value),
        key=key,
        headers=headers or [],
    )


async def _await_message(adapter: KafkaEventConsumer) -> ConsumedMessage:
    return await asyncio.wait_for(adapter.getone(), timeout=_RECV_TIMEOUT)


# ---------- start / stop lifecycle ----------


async def test__kafka_consumer__started_adapter__delegates_lifecycle_to_underlying_consumer(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(kafka_producer, topic=kafka_topic, value={"x": 1})

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert isinstance(msg, ConsumedMessage)


# ---------- mapping ----------


async def test__kafka_consumer__envelope_with_payload_field__unwraps_into_consumed_payload(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    body: dict[str, object] = {
        "schema_version": "1.0.0",
        "trace_id": "tr-1",
        "correlation_id": "co-1",
        "causation_id": "ca-1",
        "payload": {"user_id": "u-1", "email": "u@example.com"},
    }
    await _send_raw(kafka_producer, topic=kafka_topic, value=body, headers=[("event_type", b"user.created")])

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.payload == {"user_id": "u-1", "email": "u@example.com"}
    assert msg.schema_version == "1.0.0"
    assert msg.trace_id == "tr-1"
    assert msg.correlation_id == "co-1"
    assert msg.causation_id == "ca-1"
    assert msg.event_type == "user.created"


async def test__kafka_consumer__envelope_metadata_in_headers__overrides_envelope_body_fields(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    body: dict[str, object] = {
        "schema_version": "9.9.9",
        "trace_id": "from-body",
        "payload": {"k": "v"},
    }
    await _send_raw(
        kafka_producer,
        topic=kafka_topic,
        value=body,
        headers=[
            ("schema_version", b"1.0.0"),
            ("trace_id", b"from-header"),
        ],
    )

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.schema_version == "1.0.0"
    assert msg.trace_id == "from-header"


async def test__kafka_consumer__no_envelope_payload_field__treats_whole_body_as_payload(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    body: dict[str, object] = {"flat": "yes", "value": 42}
    await _send_raw(kafka_producer, topic=kafka_topic, value=body)

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.payload == body


# ---------- message_id resolution ----------


async def test__kafka_consumer__header_message_id__used_as_message_id(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(
        kafka_producer,
        topic=kafka_topic,
        value={"x": 1},
        headers=[("message_id", b"msg-123")],
    )

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.message_id == "msg-123"


async def test__kafka_consumer__header_event_id_only__used_as_fallback_message_id(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(
        kafka_producer,
        topic=kafka_topic,
        value={"x": 1},
        headers=[("event_id", b"evt-7")],
    )

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.message_id == "evt-7"


async def test__kafka_consumer__no_id_headers__falls_back_to_topic_partition_offset(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(kafka_producer, topic=kafka_topic, value={"x": 1})

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.message_id == f"{kafka_topic}:0:0"


# ---------- source resolution ----------


async def test__kafka_consumer__header_source__used_as_source(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(
        kafka_producer,
        topic=kafka_topic,
        value={"x": 1},
        headers=[("source", b"identity-service")],
    )

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.source == "identity-service"


async def test__kafka_consumer__header_source_service_only__used_as_fallback_source(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(
        kafka_producer,
        topic=kafka_topic,
        value={"x": 1},
        headers=[("source_service", b"billing")],
    )

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.source == "billing"


# ---------- event_type resolution ----------


async def test__kafka_consumer__no_event_type_in_headers_or_payload__falls_back_to_topic(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(kafka_producer, topic=kafka_topic, value={"k": "v"})

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.event_type == kafka_topic


async def test__kafka_consumer__event_type_in_payload_only__used_as_event_type(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await _send_raw(kafka_producer, topic=kafka_topic, value={"event_type": "user.updated", "k": "v"})

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.event_type == "user.updated"


# ---------- commit ----------


async def test__kafka_consumer__ack_handle_commit__advances_committed_offset_for_partition(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    group = f"g-{uuid4().hex[:6]}"
    inner = await make_kafka_consumer(kafka_topic, group_id=group)
    adapter = KafkaEventConsumer(inner)
    await _send_raw(kafka_producer, topic=kafka_topic, value={"n": 1})
    msg = await _await_message(adapter)
    assert msg.ack_handle is not None
    tp = TopicPartition(kafka_topic, 0)

    # Act
    await msg.ack_handle.commit()

    # Assert
    committed = await inner.committed(tp)
    assert committed == 1


# ---------- empty payload ----------


def test__default_payload_loader__none_value__raises_value_error() -> None:
    # Arrange
    @dataclass
    class _FakeRecord:
        topic: str = "t"
        partition: int = 0
        offset: int = 0
        value: bytes | None = None
        headers: list[tuple[str, bytes | None]] | None = None
        key: bytes | None = None

    # Act / Assert
    with pytest.raises(ValueError):
        KafkaEventConsumer._default_payload_loader(_FakeRecord())  # type: ignore[arg-type]


async def test__kafka_consumer__non_dict_payload__raises_type_error(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    await kafka_producer.send_and_wait(topic=kafka_topic, value=orjson.dumps([1, 2, 3]), key=None, headers=[])

    # Act / Assert
    with pytest.raises(TypeError):
        await _await_message(adapter)


# ---------- DefaultEnvelopeParser direct ----------


def test__default_envelope_parser__no_payload_field__returns_payload_as_is() -> None:
    # Arrange
    parser = DefaultEnvelopeParser()

    # Act
    result = parser.parse({"a": 1, "b": 2}, headers=None)

    # Assert
    assert result == EnvelopeData(payload={"a": 1, "b": 2})


def test__default_envelope_parser__payload_field_present__extracts_inner_payload_and_metadata() -> None:
    # Arrange
    parser = DefaultEnvelopeParser()
    body: dict[str, JsonValue] = {
        "schema_version": "1.1",
        "trace_id": "t",
        "correlation_id": "c",
        "causation_id": "k",
        "payload": {"x": 1},
    }

    # Act
    result = parser.parse(body, headers=None)

    # Assert
    assert result.payload == {"x": 1}
    assert result.schema_version == "1.1"
    assert result.trace_id == "t"
    assert result.correlation_id == "c"
    assert result.causation_id == "k"


def test__default_envelope_parser__header_metadata_takes_precedence() -> None:
    # Arrange
    parser = DefaultEnvelopeParser()
    body: dict[str, JsonValue] = {
        "schema_version": "from-body",
        "payload": {"x": 1},
    }

    # Act
    result = parser.parse(body, headers={"schema_version": "from-header"})

    # Assert
    assert result.schema_version == "from-header"


# ---------- custom hooks ----------


async def test__kafka_consumer__custom_payload_loader__used_to_decode_record_value(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    raw_payload_calls: list[bytes] = []

    def _custom_loader(record: object) -> dict[str, JsonValue]:
        raw_payload_calls.append(record.value)  # type: ignore[attr-defined]
        return {"loaded": "custom"}

    adapter = KafkaEventConsumer(inner, payload_loader=_custom_loader)
    await _send_raw(kafka_producer, topic=kafka_topic, value={"original": "v"})

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.payload == {"loaded": "custom"}
    assert len(raw_payload_calls) == 1


async def test__kafka_consumer__custom_envelope_parser__used_to_extract_metadata(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    class _Parser(EnvelopeParser):
        def parse(
            self,
            raw_payload: dict[str, JsonValue],
            headers: dict[str, str] | None,
        ) -> EnvelopeData:
            return EnvelopeData(payload=raw_payload, schema_version="injected")

    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner, envelope_parser=_Parser())
    await _send_raw(kafka_producer, topic=kafka_topic, value={"k": "v"})

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.schema_version == "injected"


# ---------- headers decoding ----------


async def test__kafka_consumer__headers_with_none_values__filtered_out(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    inner = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")
    adapter = KafkaEventConsumer(inner)
    # aiokafka allows None values in headers; the adapter should skip them.
    await kafka_producer.send_and_wait(
        topic=kafka_topic,
        value=orjson.dumps({"k": "v"}),
        key=None,
        headers=[("event_type", b"u.c"), ("nullable", cast(bytes, None))],
    )

    # Act
    msg = await _await_message(adapter)

    # Assert
    assert msg.headers is not None
    assert "nullable" not in msg.headers
    assert msg.headers["event_type"] == "u.c"
