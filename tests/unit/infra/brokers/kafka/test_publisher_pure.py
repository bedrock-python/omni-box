"""Unit tests for omni_box.infra.brokers.kafka.publisher."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import orjson
import pytest

from omni_box.core.converters.event import EventConverter
from omni_box.core.models.entities import OutboxEvent
from omni_box.infra.brokers.kafka.publisher import KafkaEventPublisher

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(**kwargs: object) -> OutboxEvent:
    defaults: dict = dict(
        id=uuid4(),
        aggregate_type="user",
        aggregate_id=uuid4(),
        event_type="user.created",
        topic="user-events",
        partition_key="pk-1",
        payload={"k": "v"},
    )
    defaults.update(kwargs)
    return OutboxEvent(**defaults)  # type: ignore[arg-type]


def _make_publisher(
    producer: AsyncMock | None = None,
    converter: MagicMock | None = None,
    max_infra_retries: int = 3,
) -> tuple[KafkaEventPublisher, AsyncMock, MagicMock]:
    if producer is None:
        producer = AsyncMock()
    if converter is None:
        converter = MagicMock(spec=EventConverter)
        converter.convert.return_value = {"k": "v"}
    pub = KafkaEventPublisher(producer, converter, max_infra_retries=max_infra_retries)
    return pub, producer, converter


# ---------------------------------------------------------------------------
# KafkaEventPublisher.publish — success
# ---------------------------------------------------------------------------


class TestKafkaEventPublisherPublish:
    @pytest.mark.asyncio
    async def test__publish__success__calls_send_and_wait_with_correct_args(self) -> None:
        # Arrange
        event = _make_event()
        pub, producer, converter = _make_publisher()
        repo = MagicMock()

        # Act
        await pub.publish(event, repo)

        # Assert
        producer.send_and_wait.assert_awaited_once()
        call_kwargs = producer.send_and_wait.call_args
        assert call_kwargs.kwargs["topic"] == event.topic
        assert call_kwargs.kwargs["value"] == orjson.dumps({"k": "v"})
        assert call_kwargs.kwargs["key"] == event.partition_key.encode("utf-8")

    @pytest.mark.asyncio
    async def test__publish__partition_key_none__key_bytes_is_none(self) -> None:
        # Arrange
        # OutboxEvent requires partition_key as StrippedNonEmptyStr — use a subclass workaround
        # by patching the event attribute directly after creation
        event = _make_event()
        # OutboxEvent is frozen; use model_copy to override partition_key to None
        event_no_key = event.model_copy(update={"partition_key": None})  # type: ignore[arg-type]
        pub, producer, converter = _make_publisher()
        repo = MagicMock()

        # Act
        await pub.publish(event_no_key, repo)

        # Assert
        call_kwargs = producer.send_and_wait.call_args
        assert call_kwargs.kwargs["key"] is None

    @pytest.mark.asyncio
    async def test__publish__headers_encoded_as_bytes_tuples(self) -> None:
        # Arrange
        event = _make_event(headers={"x-custom": "val"})
        pub, producer, _ = _make_publisher()
        repo = MagicMock()

        # Act
        await pub.publish(event, repo)

        # Assert
        call_kwargs = producer.send_and_wait.call_args
        headers: list[tuple[str, bytes]] = call_kwargs.kwargs["headers"]
        header_dict = dict(headers)
        assert header_dict["x-custom"] == b"val"
        # Ensure all values are bytes
        for _k, v in headers:
            assert isinstance(v, bytes)

    @pytest.mark.asyncio
    async def test__publish__transient_error__retries_up_to_max_infra_retries(self) -> None:
        # Arrange
        event = _make_event()
        producer = AsyncMock()
        converter = MagicMock(spec=EventConverter)
        converter.convert.return_value = {"k": "v"}
        pub = KafkaEventPublisher(producer, converter, max_infra_retries=2)
        repo = MagicMock()

        # First 2 calls raise ConnectionError (transient), 3rd succeeds
        producer.send_and_wait.side_effect = [ConnectionError("conn"), ConnectionError("conn"), None]

        with patch("omni_box.infra.brokers.kafka.publisher.asyncio.sleep", new_callable=AsyncMock):
            # Act
            await pub.publish(event, repo)

        # Assert — called 3 times total (2 retries + 1 success)
        assert producer.send_and_wait.call_count == 3

    @pytest.mark.asyncio
    async def test__publish__transient_error_exceeds_max_retries__raises(self) -> None:
        # Arrange
        event = _make_event()
        producer = AsyncMock()
        converter = MagicMock(spec=EventConverter)
        converter.convert.return_value = {"k": "v"}
        pub = KafkaEventPublisher(producer, converter, max_infra_retries=2)
        repo = MagicMock()

        # Always raises transient error
        producer.send_and_wait.side_effect = ConnectionError("conn")

        with patch("omni_box.infra.brokers.kafka.publisher.asyncio.sleep", new_callable=AsyncMock):
            # Act / Assert
            with pytest.raises(ConnectionError):
                await pub.publish(event, repo)

        # 3 attempts total (attempt 0, 1, 2) — on attempt==max_infra_retries it raises
        assert producer.send_and_wait.call_count == 3

    @pytest.mark.asyncio
    async def test__publish__non_transient_error__raises_immediately(self) -> None:
        # Arrange
        event = _make_event()
        producer = AsyncMock()
        converter = MagicMock(spec=EventConverter)
        converter.convert.return_value = {"k": "v"}
        pub = KafkaEventPublisher(producer, converter, max_infra_retries=3)
        repo = MagicMock()

        producer.send_and_wait.side_effect = ValueError("bad topic")

        # Act / Assert
        with pytest.raises(ValueError, match="bad topic"):
            await pub.publish(event, repo)

        # Called only once — no retry for permanent errors
        assert producer.send_and_wait.call_count == 1


# ---------------------------------------------------------------------------
# KafkaEventPublisher._build_headers
# ---------------------------------------------------------------------------


class TestBuildHeaders:
    def setup_method(self) -> None:
        self.producer = AsyncMock()
        self.converter = MagicMock(spec=EventConverter)
        self.converter.convert.return_value = {}
        self.subject = KafkaEventPublisher(self.producer, self.converter)

    def test__build_headers__always_includes_event_id_and_event_type(self) -> None:
        # Arrange
        event = _make_event()

        # Act
        headers = self.subject._build_headers(event)

        # Assert
        assert headers["event_id"] == str(event.id)
        assert headers["event_type"] == event.event_type

    def test__build_headers__includes_optional_fields_when_present(self) -> None:
        # Arrange
        event = _make_event(
            schema_version="1.2.3",
            trace_id="trace-abc",
            correlation_id="corr-xyz",
            causation_id="caus-000",
        )

        # Act
        headers = self.subject._build_headers(event)

        # Assert
        assert headers["schema_version"] == "1.2.3"
        assert headers["trace_id"] == "trace-abc"
        assert headers["correlation_id"] == "corr-xyz"
        assert headers["causation_id"] == "caus-000"

    def test__build_headers__omits_optional_fields_when_none(self) -> None:
        # Arrange
        event = _make_event()  # no trace/correlation/causation/schema_version

        # Act
        headers = self.subject._build_headers(event)

        # Assert
        assert "schema_version" not in headers
        assert "trace_id" not in headers
        assert "correlation_id" not in headers
        assert "causation_id" not in headers

    def test__build_headers__merges_existing_event_headers(self) -> None:
        # Arrange
        event = _make_event(headers={"x-request-id": "req-1", "x-tenant": "acme"})

        # Act
        headers = self.subject._build_headers(event)

        # Assert
        assert headers["x-request-id"] == "req-1"
        assert headers["x-tenant"] == "acme"
        # Built-in headers still present
        assert "event_id" in headers
        assert "event_type" in headers

    def test__build_headers__event_without_headers__no_extra_keys(self) -> None:
        # Arrange
        event = _make_event(headers=None)

        # Act
        headers = self.subject._build_headers(event)

        # Assert — only event_id and event_type
        assert set(headers.keys()) == {"event_id", "event_type"}
