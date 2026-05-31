"""Integration tests for ``KafkaEventPublisher`` against a real Kafka broker."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import cast
from uuid import UUID, uuid4

import orjson
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from omni_box.core.converters.event import EnvelopeEventConverter, RawEventConverter, SchemaVersionedConverter
from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.brokers.kafka.publisher import KafkaEventPublisher
from tests.helpers import FakeOutboxStore

pytestmark = pytest.mark.integration


# ---------- helpers ----------

KafkaConsumerFactory = Callable[..., Awaitable[AIOKafkaConsumer]]
_RECV_TIMEOUT = 10.0


def _make_event(*, topic: str, partition_key: str = "k-1", payload: dict[str, object] | None = None) -> OutboxEvent:
    return OutboxEvent(
        id=uuid4(),
        aggregate_type="user",
        aggregate_id=uuid4(),
        event_type="user.created",
        topic=topic,
        partition_key=partition_key,
        payload=payload or {"email": "u@example.com"},
    )


async def _consume_one(consumer: AIOKafkaConsumer) -> object:
    return await asyncio.wait_for(consumer.getone(), timeout=_RECV_TIMEOUT)


# ---------- publish ----------


async def test__kafka_publisher__raw_converter__sends_payload_bytes_with_event_metadata_headers(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    publisher = KafkaEventPublisher(kafka_producer, RawEventConverter())
    event = _make_event(topic=kafka_topic, payload={"k": "v"})
    consumer = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")

    # Act
    await publisher.publish(event, repo=cast("FakeOutboxStore", FakeOutboxStore()))
    record = await _consume_one(consumer)

    # Assert
    assert orjson.loads(record.value) == {"k": "v"}  # type: ignore[attr-defined]
    assert record.key == b"k-1"  # type: ignore[attr-defined]
    headers = dict(record.headers)  # type: ignore[attr-defined]
    assert headers[b"event_id".decode()] == str(event.id).encode("utf-8")
    assert headers["event_type"] == b"user.created"


async def test__kafka_publisher__schema_versioned_converter__publishes_envelope_with_schema_field(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    publisher = KafkaEventPublisher(kafka_producer, SchemaVersionedConverter())
    event = _make_event(topic=kafka_topic, payload={"x": 1})
    event = event.model_copy(update={"schema_version": "2.0.0"})
    consumer = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")

    # Act
    await publisher.publish(event, repo=cast("FakeOutboxStore", FakeOutboxStore()))
    record = await _consume_one(consumer)

    # Assert
    payload = orjson.loads(record.value)  # type: ignore[attr-defined]
    assert payload == {"schema_version": "2.0.0", "payload": {"x": 1}}


async def test__kafka_publisher__envelope_converter__publishes_full_envelope_with_tracing_headers_when_present(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    publisher = KafkaEventPublisher(kafka_producer, EnvelopeEventConverter(default_schema_version="1.2.3"))
    event = _make_event(topic=kafka_topic).model_copy(
        update={
            "trace_id": "trace-1",
            "correlation_id": "corr-1",
            "causation_id": "caus-1",
        }
    )
    consumer = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")

    # Act
    await publisher.publish(event, repo=cast("FakeOutboxStore", FakeOutboxStore()))
    record = await _consume_one(consumer)

    # Assert
    envelope = orjson.loads(record.value)  # type: ignore[attr-defined]
    assert envelope["schema_version"] == "1.2.3"
    assert envelope["event_type"] == "user.created"
    assert envelope["aggregate_type"] == "user"
    assert envelope["aggregate_id"] == str(event.aggregate_id)
    assert envelope["trace_id"] == "trace-1"
    assert envelope["correlation_id"] == "corr-1"
    assert envelope["causation_id"] == "caus-1"
    headers = dict(record.headers)  # type: ignore[attr-defined]
    assert headers["trace_id"] == b"trace-1"
    assert headers["correlation_id"] == b"corr-1"
    assert headers["causation_id"] == b"caus-1"


async def test__kafka_publisher__event_without_partition_key__sends_null_key(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    publisher = KafkaEventPublisher(kafka_producer, RawEventConverter())
    event = _make_event(topic=kafka_topic).model_copy(update={"partition_key": None})
    consumer = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")

    # Act
    await publisher.publish(event, repo=cast("FakeOutboxStore", FakeOutboxStore()))
    record = await _consume_one(consumer)

    # Assert
    assert record.key is None  # type: ignore[attr-defined]


# ---------- retry / classification ----------


class _BrokenProducer:
    """Fake producer that fails N times with a given exception then succeeds."""

    def __init__(self, fail_times: int, exc: Exception) -> None:
        self._fail_times = fail_times
        self._exc = exc
        self.calls = 0

    async def send_and_wait(self, **_kwargs: object) -> None:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise self._exc


async def test__kafka_publisher__transient_error_under_limit__retries_and_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    sleeps: list[float] = []

    async def _no_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("omni_box.infra.brokers.kafka.publisher.asyncio.sleep", _no_sleep)
    broken = _BrokenProducer(fail_times=2, exc=ConnectionError("transient"))
    publisher = KafkaEventPublisher(cast(AIOKafkaProducer, broken), RawEventConverter(), max_infra_retries=3)
    event = _make_event(topic="any")

    # Act
    await publisher.publish(event, repo=cast("FakeOutboxStore", FakeOutboxStore()))

    # Assert
    assert broken.calls == 3
    assert len(sleeps) == 2


async def test__kafka_publisher__transient_error_over_limit__raises_original_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    async def _no_sleep(_delay: float) -> None:
        return

    monkeypatch.setattr("omni_box.infra.brokers.kafka.publisher.asyncio.sleep", _no_sleep)
    broken = _BrokenProducer(fail_times=10, exc=ConnectionError("permanent transient"))
    publisher = KafkaEventPublisher(cast(AIOKafkaProducer, broken), RawEventConverter(), max_infra_retries=1)

    # Act / Assert
    with pytest.raises(ConnectionError):
        await publisher.publish(_make_event(topic="any"), repo=cast("FakeOutboxStore", FakeOutboxStore()))
    assert broken.calls == 2  # initial + 1 retry


async def test__kafka_publisher__permanent_error__no_retry_and_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    async def _no_sleep(_delay: float) -> None:
        return

    monkeypatch.setattr("omni_box.infra.brokers.kafka.publisher.asyncio.sleep", _no_sleep)
    broken = _BrokenProducer(fail_times=10, exc=ValueError("invalid payload"))
    publisher = KafkaEventPublisher(cast(AIOKafkaProducer, broken), RawEventConverter(), max_infra_retries=5)

    # Act / Assert
    with pytest.raises(ValueError):
        await publisher.publish(_make_event(topic="any"), repo=cast("FakeOutboxStore", FakeOutboxStore()))
    assert broken.calls == 1


# ---------- event_id stability ----------


async def test__kafka_publisher__same_event_published_twice__produces_two_distinct_messages_with_same_event_id(
    kafka_producer: AIOKafkaProducer,
    kafka_topic: str,
    make_kafka_consumer: KafkaConsumerFactory,
) -> None:
    # Arrange
    publisher = KafkaEventPublisher(kafka_producer, RawEventConverter())
    event = _make_event(topic=kafka_topic)
    consumer = await make_kafka_consumer(kafka_topic, group_id=f"g-{uuid4().hex[:6]}")

    # Act
    await publisher.publish(event, repo=cast("FakeOutboxStore", FakeOutboxStore()))
    await publisher.publish(event, repo=cast("FakeOutboxStore", FakeOutboxStore()))
    first = await _consume_one(consumer)
    second = await _consume_one(consumer)

    # Assert
    event_id_str = str(event.id)
    assert UUID(event_id_str) == event.id  # sanity
    assert dict(first.headers)["event_id"] == event_id_str.encode()  # type: ignore[attr-defined]
    assert dict(second.headers)["event_id"] == event_id_str.encode()  # type: ignore[attr-defined]
    assert first.offset != second.offset  # type: ignore[attr-defined]
