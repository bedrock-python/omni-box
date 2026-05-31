"""Unit tests for omni_box.infra.brokers.kafka.consumer."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from omni_box.core.protocols import EnvelopeParser
from omni_box.infra.brokers.kafka.consumer import (
    DefaultEnvelopeParser,
    KafkaEventConsumer,
    _KafkaCommitAckHandle,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(
    *,
    topic: str = "test-topic",
    partition: int = 0,
    offset: int = 10,
    value: bytes | None = b'{"k": "v"}',
    headers: list | None = None,
) -> MagicMock:
    record = MagicMock()
    record.topic = topic
    record.partition = partition
    record.offset = offset
    record.value = value
    record.headers = headers if headers is not None else []
    return record


# ---------------------------------------------------------------------------
# DefaultEnvelopeParser
# ---------------------------------------------------------------------------


class TestDefaultEnvelopeParser:
    def setup_method(self) -> None:
        self.parser = DefaultEnvelopeParser()

    def test__parse__no_payload_key__returns_raw_payload_as_envelope_payload(self) -> None:
        # Arrange
        raw = {"foo": "bar"}

        # Act
        result = self.parser.parse(raw, None)

        # Assert
        assert result.payload == raw

    def test__parse__payload_key_present__extracts_nested_payload(self) -> None:
        # Arrange
        raw = {"payload": {"inner": "data"}}

        # Act
        result = self.parser.parse(raw, None)

        # Assert
        assert result.payload == {"inner": "data"}

    def test__parse__payload_key_present__extracts_meta_from_envelope_when_no_headers(self) -> None:
        # Arrange
        raw = {
            "payload": {"x": 1},
            "schema_version": "2.0.0",
            "trace_id": "t1",
            "correlation_id": "c1",
            "causation_id": "ca1",
        }

        # Act
        result = self.parser.parse(raw, None)

        # Assert
        assert result.schema_version == "2.0.0"
        assert result.trace_id == "t1"
        assert result.correlation_id == "c1"
        assert result.causation_id == "ca1"

    def test__parse__headers_take_priority_over_envelope_fields(self) -> None:
        # Arrange
        raw = {
            "payload": {"x": 1},
            "schema_version": "1.0.0",
            "trace_id": "envelope-trace",
            "correlation_id": "envelope-corr",
            "causation_id": "envelope-caus",
        }
        headers = {
            "schema_version": "3.0.0",
            "trace_id": "header-trace",
            "correlation_id": "header-corr",
            "causation_id": "header-caus",
        }

        # Act
        result = self.parser.parse(raw, headers)

        # Assert
        assert result.schema_version == "3.0.0"
        assert result.trace_id == "header-trace"
        assert result.correlation_id == "header-corr"
        assert result.causation_id == "header-caus"

    def test__parse__no_headers_no_envelope_meta__all_meta_none(self) -> None:
        # Arrange / Act
        result = self.parser.parse({"key": "val"}, None)

        # Assert
        assert result.schema_version is None
        assert result.trace_id is None
        assert result.correlation_id is None
        assert result.causation_id is None


# ---------------------------------------------------------------------------
# _KafkaCommitAckHandle
# ---------------------------------------------------------------------------


class TestKafkaCommitAckHandle:
    @pytest.mark.asyncio
    async def test__commit__calls_consumer_commit_with_offset_plus_one(self) -> None:
        # Arrange
        consumer = AsyncMock()
        record = _make_record(topic="my-topic", partition=2, offset=5)
        handle = _KafkaCommitAckHandle(consumer, record)

        # Act
        await handle.commit()

        # Assert
        from aiokafka import TopicPartition

        consumer.commit.assert_awaited_once_with({TopicPartition("my-topic", 2): 6})


# ---------------------------------------------------------------------------
# KafkaEventConsumer — lifecycle
# ---------------------------------------------------------------------------


class TestKafkaEventConsumerLifecycle:
    def setup_method(self) -> None:
        self.raw_consumer = AsyncMock()
        self.subject = KafkaEventConsumer(self.raw_consumer)

    @pytest.mark.asyncio
    async def test__start__delegates_to_inner_consumer(self) -> None:
        # Act
        await self.subject.start()

        # Assert
        self.raw_consumer.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test__stop__delegates_to_inner_consumer(self) -> None:
        # Act
        await self.subject.stop()

        # Assert
        self.raw_consumer.stop.assert_awaited_once()


# ---------------------------------------------------------------------------
# KafkaEventConsumer — _decode_headers
# ---------------------------------------------------------------------------


class TestDecodeHeaders:
    def setup_method(self) -> None:
        self.subject = KafkaEventConsumer(AsyncMock())

    def test__decode_headers__empty_headers__returns_none(self) -> None:
        # Arrange
        record = _make_record(headers=[])

        # Act
        result = KafkaEventConsumer._decode_headers(record)

        # Assert
        assert result is None

    def test__decode_headers__with_headers__decodes_bytes_to_str(self) -> None:
        # Arrange
        record = _make_record(headers=[("event_type", b"user.created"), ("trace_id", b"abc")])

        # Act
        result = KafkaEventConsumer._decode_headers(record)

        # Assert
        assert result == {"event_type": "user.created", "trace_id": "abc"}

    def test__decode_headers__skips_none_values(self) -> None:
        # Arrange
        record = _make_record(headers=[("k1", None), ("k2", b"val")])

        # Act
        result = KafkaEventConsumer._decode_headers(record)

        # Assert
        assert result == {"k2": "val"}

    def test__decode_headers__all_none_values__returns_none(self) -> None:
        # Arrange
        record = _make_record(headers=[("k1", None)])

        # Act
        result = KafkaEventConsumer._decode_headers(record)

        # Assert
        assert result is None


# ---------------------------------------------------------------------------
# KafkaEventConsumer — _default_payload_loader
# ---------------------------------------------------------------------------


class TestDefaultPayloadLoader:
    def test__default_payload_loader__none_value__raises_value_error(self) -> None:
        # Arrange
        record = _make_record(value=None)

        # Act / Assert
        with pytest.raises(ValueError, match="Empty payload"):
            KafkaEventConsumer._default_payload_loader(record)

    def test__default_payload_loader__bytes__parses_json(self) -> None:
        # Arrange
        record = _make_record(value=orjson.dumps({"hello": "world"}))

        # Act
        result = KafkaEventConsumer._default_payload_loader(record)

        # Assert
        assert result == {"hello": "world"}

    def test__default_payload_loader__non_dict__raises_type_error(self) -> None:
        # Arrange
        record = _make_record(value=orjson.dumps([1, 2, 3]))

        # Act / Assert
        with pytest.raises(TypeError, match="Payload must be dict"):
            KafkaEventConsumer._default_payload_loader(record)


# ---------------------------------------------------------------------------
# KafkaEventConsumer — _default_message_id_getter
# ---------------------------------------------------------------------------


class TestDefaultMessageIdGetter:
    def test__default_message_id_getter__message_id_in_headers__uses_it(self) -> None:
        # Arrange
        record = _make_record(topic="t", partition=1, offset=2)
        headers = {"message_id": "msg-123"}

        # Act
        result = KafkaEventConsumer._default_message_id_getter(record, headers)

        # Assert
        assert result == "msg-123"

    def test__default_message_id_getter__event_id_in_headers__uses_it(self) -> None:
        # Arrange
        record = _make_record(topic="t", partition=1, offset=2)
        headers = {"event_id": "evt-456"}

        # Act
        result = KafkaEventConsumer._default_message_id_getter(record, headers)

        # Assert
        assert result == "evt-456"

    def test__default_message_id_getter__no_headers__uses_topic_partition_offset(self) -> None:
        # Arrange
        record = _make_record(topic="my-topic", partition=3, offset=7)

        # Act
        result = KafkaEventConsumer._default_message_id_getter(record, None)

        # Assert
        assert result == "my-topic:3:7"

    def test__default_message_id_getter__empty_headers__uses_topic_partition_offset(self) -> None:
        # Arrange
        record = _make_record(topic="my-topic", partition=3, offset=7)

        # Act
        result = KafkaEventConsumer._default_message_id_getter(record, {})

        # Assert
        assert result == "my-topic:3:7"


# ---------------------------------------------------------------------------
# KafkaEventConsumer — _default_event_type_getter
# ---------------------------------------------------------------------------


class TestDefaultEventTypeGetter:
    def test__default_event_type_getter__header_present__uses_header(self) -> None:
        # Arrange
        record = _make_record(topic="t")
        payload: dict = {}
        headers = {"event_type": "user.created"}

        # Act
        result = KafkaEventConsumer._default_event_type_getter(record, payload, headers)

        # Assert
        assert result == "user.created"

    def test__default_event_type_getter__no_header_payload_has_event_type__uses_payload(self) -> None:
        # Arrange
        record = _make_record(topic="t")
        payload = {"event_type": "order.placed"}

        # Act
        result = KafkaEventConsumer._default_event_type_getter(record, payload, None)

        # Assert
        assert result == "order.placed"

    def test__default_event_type_getter__no_header_no_payload_event_type__uses_topic(self) -> None:
        # Arrange
        record = _make_record(topic="fallback-topic")
        payload: dict = {}

        # Act
        result = KafkaEventConsumer._default_event_type_getter(record, payload, None)

        # Assert
        assert result == "fallback-topic"

    def test__default_event_type_getter__payload_event_type_whitespace__uses_topic(self) -> None:
        # Arrange
        record = _make_record(topic="fallback-topic")
        payload = {"event_type": "   "}

        # Act
        result = KafkaEventConsumer._default_event_type_getter(record, payload, None)

        # Assert
        assert result == "fallback-topic"


# ---------------------------------------------------------------------------
# KafkaEventConsumer — _default_source_getter
# ---------------------------------------------------------------------------


class TestDefaultSourceGetter:
    def test__default_source_getter__source_header__uses_it(self) -> None:
        # Arrange
        record = _make_record(topic="t")
        headers = {"source": "identity-service"}

        # Act
        result = KafkaEventConsumer._default_source_getter(record, headers)

        # Assert
        assert result == "identity-service"

    def test__default_source_getter__source_service_header__uses_it(self) -> None:
        # Arrange
        record = _make_record(topic="t")
        headers = {"source_service": "order-service"}

        # Act
        result = KafkaEventConsumer._default_source_getter(record, headers)

        # Assert
        assert result == "order-service"

    def test__default_source_getter__no_headers__uses_topic(self) -> None:
        # Arrange
        record = _make_record(topic="my-topic")

        # Act
        result = KafkaEventConsumer._default_source_getter(record, None)

        # Assert
        assert result == "my-topic"


# ---------------------------------------------------------------------------
# KafkaEventConsumer — getone
# ---------------------------------------------------------------------------


class TestKafkaEventConsumerGetone:
    @pytest.mark.asyncio
    async def test__getone__default_config__assembles_consumed_message(self) -> None:
        # Arrange
        record = _make_record(
            topic="events",
            partition=0,
            offset=42,
            value=orjson.dumps({"event_type": "user.created", "data": "x"}),
            headers=[("event_id", b"eid-1"), ("source", b"svc-a")],
        )
        raw_consumer = AsyncMock()
        raw_consumer.getone = AsyncMock(return_value=record)
        subject = KafkaEventConsumer(raw_consumer)

        # Act
        msg = await subject.getone()

        # Assert
        assert msg.message_id == "eid-1"
        assert msg.source == "svc-a"
        assert msg.event_type == "user.created"
        assert msg.payload == {"event_type": "user.created", "data": "x"}
        assert msg.headers == {"event_id": "eid-1", "source": "svc-a"}
        assert msg.ack_handle is not None
        assert msg.raw_message is record

    @pytest.mark.asyncio
    async def test__getone__custom_overrides__uses_custom_callables(self) -> None:
        # Arrange
        record = _make_record(value=b'{"k": "v"}')
        raw_consumer = AsyncMock()
        raw_consumer.getone = AsyncMock(return_value=record)

        custom_payload_loader = MagicMock(return_value={"custom": "payload"})
        custom_message_id_getter = MagicMock(return_value="custom-msg-id")
        custom_event_type_getter = MagicMock(return_value="custom.event")
        custom_source_getter = MagicMock(return_value="custom-source")
        custom_envelope_parser = MagicMock(spec=EnvelopeParser)
        from omni_box.core.protocols import EnvelopeData

        custom_envelope_parser.parse.return_value = EnvelopeData(payload={"custom": "payload"})

        subject = KafkaEventConsumer(
            raw_consumer,
            payload_loader=custom_payload_loader,
            message_id_getter=custom_message_id_getter,
            event_type_getter=custom_event_type_getter,
            source_getter=custom_source_getter,
            envelope_parser=custom_envelope_parser,
        )

        # Act
        msg = await subject.getone()

        # Assert
        assert msg.message_id == "custom-msg-id"
        assert msg.source == "custom-source"
        assert msg.event_type == "custom.event"
        custom_payload_loader.assert_called_once_with(record)
        custom_message_id_getter.assert_called_once()
        custom_event_type_getter.assert_called_once()
        custom_source_getter.assert_called_once()
        custom_envelope_parser.parse.assert_called_once()
