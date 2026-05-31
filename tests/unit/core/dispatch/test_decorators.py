"""Unit tests for ``omni_box.core.dispatch.decorators``."""

from __future__ import annotations

from enum import StrEnum

import pytest

from omni_box.core.dispatch.decorators import event_handler

pytestmark = pytest.mark.unit


class _Event(StrEnum):
    CREATED = "user.created"


class _Topic(StrEnum):
    USERS = "users"


def test__event_handler__with_topic_and_version__attaches_all_metadata() -> None:
    # Arrange / Act
    @event_handler("my.event", topic="my.topic", schema_version="1.0.0")
    async def handler(event: object) -> None:
        pass

    # Assert
    assert handler._is_event_handler is True  # type: ignore[attr-defined]
    assert handler._event_type == "my.event"  # type: ignore[attr-defined]
    assert handler._event_topic == "my.topic"  # type: ignore[attr-defined]
    assert handler._schema_version == "1.0.0"  # type: ignore[attr-defined]


def test__event_handler__no_topic__stores_none_topic() -> None:
    # Arrange / Act
    @event_handler("other.event")
    async def handler(event: object) -> None:
        pass

    # Assert
    assert handler._event_topic is None  # type: ignore[attr-defined]
    assert handler._schema_version is None  # type: ignore[attr-defined]


def test__event_handler__strenum_inputs__normalizes_to_str() -> None:
    # Arrange / Act
    @event_handler(_Event.CREATED, topic=_Topic.USERS)
    async def handler(event: object) -> None:
        pass

    # Assert
    assert handler._event_type == "user.created"  # type: ignore[attr-defined]
    assert handler._event_topic == "users"  # type: ignore[attr-defined]


def test__event_handler__returns_same_function__decorator_is_identity() -> None:
    # Arrange
    async def original(event: object) -> None:
        pass

    # Act
    decorated = event_handler("ev")(original)

    # Assert
    assert decorated is original
