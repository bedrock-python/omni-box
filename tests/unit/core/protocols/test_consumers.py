"""Unit tests for ``omni_box.core.protocols.consumers``."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from omni_box.core.protocols.consumers import (
    AckHandle,
    ConsumedMessage,
    EnvelopeData,
    NullAckHandle,
)

pytestmark = pytest.mark.unit


async def test__null_ack_handle__commit__returns_none_and_does_not_raise() -> None:
    # Arrange
    handle = NullAckHandle()

    # Act
    result = await handle.commit()

    # Assert
    assert result is None


def test__null_ack_handle__class__is_subclass_of_ack_handle() -> None:
    # Arrange / Act / Assert
    assert issubclass(NullAckHandle, AckHandle)


def test__envelope_data__defaults__optional_metadata_is_none() -> None:
    # Arrange / Act
    env = EnvelopeData(payload={"a": 1})

    # Assert
    assert env.payload == {"a": 1}
    assert env.trace_id is None
    assert env.correlation_id is None
    assert env.causation_id is None
    assert env.schema_version is None


def test__envelope_data__frozen__cannot_mutate_fields() -> None:
    # Arrange
    env = EnvelopeData(payload={"a": 1}, trace_id="t-1")

    # Act / Assert
    with pytest.raises(FrozenInstanceError):
        env.payload = {"b": 2}  # type: ignore[misc]


def test__consumed_message__defaults__optional_fields_are_none() -> None:
    # Arrange / Act
    msg = ConsumedMessage(
        message_id="m1",
        source="kafka",
        event_type="user.created",
        payload={"a": 1},
    )

    # Assert
    assert msg.headers is None
    assert msg.trace_id is None
    assert msg.ack_handle is None
    assert msg.raw_message is None


def test__consumed_message__frozen__cannot_mutate_fields() -> None:
    # Arrange
    msg = ConsumedMessage(
        message_id="m1",
        source="kafka",
        event_type="t",
        payload={},
    )

    # Act / Assert
    with pytest.raises(FrozenInstanceError):
        msg.message_id = "m2"  # type: ignore[misc]


def test__ack_handle__is_abstract__cannot_instantiate_directly() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError):
        AckHandle()  # type: ignore[abstract]
