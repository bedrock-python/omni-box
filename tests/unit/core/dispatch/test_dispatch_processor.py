"""Unit tests for ``omni_box.core.dispatch.processor``."""

from __future__ import annotations

from typing import Any, cast

import pytest

from omni_box.core.dispatch.processor import create_dispatching_handler
from omni_box.core.dispatch.registry import EventRouter
from omni_box.core.models.entities import InboxEvent
from omni_box.core.protocols.repository import InboxEventRepository
from omni_box.core.services.results import EventHandlerResult, handler_completed

pytestmark = pytest.mark.unit


class _RecordingHandler:
    async def __call__(
        self,
        event: InboxEvent,
        repo: InboxEventRepository,
        **dependencies: Any,
    ) -> EventHandlerResult | None:
        return handler_completed()


def _inbox_event(source: str = "src1") -> InboxEvent:
    return InboxEvent(
        message_id="m1",
        consumer_group="cg1",
        source=source,
        event_type="ev1",
        payload={"foo": "bar"},
    )


async def test__create_dispatching_handler__valid_router__dispatches_to_event_source() -> None:
    # Arrange
    router = EventRouter()
    handler = _RecordingHandler()
    router.register_handler("ev1", "src1", handler)
    event = _inbox_event(source="src1")
    repo = cast(InboxEventRepository, object())

    dispatcher = create_dispatching_handler(router, dep="x")

    # Act
    result = await dispatcher(event, repo)

    # Assert
    assert result.success is True


async def test__create_dispatching_handler__passes_dependencies_through() -> None:
    # Arrange
    router = EventRouter()
    captured: dict[str, Any] = {}

    class CapturingHandler:
        async def __call__(
            self,
            event: InboxEvent,
            repo: InboxEventRepository,
            **dependencies: Any,
        ) -> EventHandlerResult | None:
            captured.update(dependencies)
            return handler_completed()

    router.register_handler("ev1", "src1", CapturingHandler())
    event = _inbox_event(source="src1")
    repo = cast(InboxEventRepository, object())

    dispatcher = create_dispatching_handler(router, uow="UOW", logger="LOG")

    # Act
    await dispatcher(event, repo)

    # Assert
    assert captured == {"uow": "UOW", "logger": "LOG"}


async def test__create_dispatching_handler__no_handler_for_source__returns_failure() -> None:
    # Arrange
    router = EventRouter()
    event = _inbox_event(source="unknown")
    repo = cast(InboxEventRepository, object())

    dispatcher = create_dispatching_handler(router)

    # Act
    result = await dispatcher(event, repo)

    # Assert
    assert result.success is False
