"""Unit tests for ``omni_box.core.dispatch.registry``."""

from __future__ import annotations

from typing import Any, cast

import pytest

from omni_box.core.dispatch.base import BaseEventHandler
from omni_box.core.dispatch.decorators import event_handler
from omni_box.core.dispatch.exceptions import HandlerAlreadyRegisteredError
from omni_box.core.dispatch.registry import EventRouter
from omni_box.core.models.entities import InboxEvent
from omni_box.core.models.schemas import BaseEventSchema
from omni_box.core.protocols.repository import InboxEventRepository
from omni_box.core.services.results import EventHandlerResult, handler_completed

pytestmark = pytest.mark.unit


# ---------- Helpers ----------


def _inbox_event(
    event_type: str = "user.created",
    schema_version: str | None = None,
    payload: dict[str, Any] | None = None,
) -> InboxEvent:
    return InboxEvent(
        message_id="m1",
        consumer_group="cg1",
        source="src1",
        event_type=event_type,
        payload=payload or {"id": "u1"},
        schema_version=schema_version,
    )


def _fake_repo() -> InboxEventRepository:
    return cast(InboxEventRepository, object())


class _RecordingHandler:
    """Real awaitable handler that records its invocations."""

    def __init__(self, result: EventHandlerResult | None = None) -> None:
        self.result = result
        self.calls: list[tuple[InboxEvent, InboxEventRepository, dict[str, Any]]] = []

    async def __call__(
        self,
        event: InboxEvent,
        repo: InboxEventRepository,
        **dependencies: Any,
    ) -> EventHandlerResult | None:
        self.calls.append((event, repo, dependencies))
        return self.result


# ---------- register_handler ----------


async def test__router__register_handler__dispatches_to_registered_handler() -> None:
    # Arrange
    router = EventRouter()
    handler = _RecordingHandler(handler_completed())
    router.register_handler("order.created", "orders", handler)
    event = _inbox_event(event_type="order.created")
    repo = _fake_repo()

    # Act
    result = await router.dispatch(event, "orders", repo, some_dep="x")

    # Assert
    assert result.success is True
    assert len(handler.calls) == 1
    assert handler.calls[0][0] is event
    assert handler.calls[0][1] is repo
    assert handler.calls[0][2] == {"some_dep": "x"}


def test__router__duplicate_registration__raises_handler_already_registered() -> None:
    # Arrange
    router = EventRouter()
    router.register_handler("ev1", "top1", _RecordingHandler())

    # Act / Assert
    with pytest.raises(HandlerAlreadyRegisteredError):
        router.register_handler("ev1", "top1", _RecordingHandler())


def test__router__normalize_topic__applies_on_register_and_get_topics() -> None:
    # Arrange
    router = EventRouter(normalize_topic=lambda t: t.lower())
    router.register_handler("ev", "ORDERS", _RecordingHandler())

    # Act / Assert
    assert "orders" in router.get_topics()


async def test__router__normalize_topic__applies_on_dispatch() -> None:
    # Arrange
    router = EventRouter(normalize_topic=lambda t: t.lower())
    handler = _RecordingHandler(handler_completed())
    router.register_handler("ev", "ORDERS", handler)
    event = _inbox_event(event_type="ev")

    # Act
    result = await router.dispatch(event, "Orders", _fake_repo())

    # Assert
    assert result.success is True
    assert len(handler.calls) == 1


# ---------- dispatch: no handler ----------


async def test__router__dispatch_no_handler__returns_failure_result() -> None:
    # Arrange
    router = EventRouter()
    event = _inbox_event()

    # Act
    result = await router.dispatch(event, "unknown", _fake_repo())

    # Assert
    assert result.success is False
    assert result.error_message is not None
    assert "No handler for topic='unknown'" in result.error_message
    assert result.count_as_attempt is True


# ---------- dispatch: versioned ----------


async def test__router__versioned_exact_match__invokes_matching_handler() -> None:
    # Arrange
    router = EventRouter()
    h1 = _RecordingHandler(handler_completed())
    h2 = _RecordingHandler(handler_completed())
    router.register_handler("user.created", "users", h1, schema_version="1.0.0")
    router.register_handler("user.created", "users", h2, schema_version="2.0.0")

    # Act
    await router.dispatch(_inbox_event(schema_version="1.0.0"), "users", _fake_repo())
    await router.dispatch(_inbox_event(schema_version="2.0.0"), "users", _fake_repo())

    # Assert
    assert len(h1.calls) == 1
    assert len(h2.calls) == 1


async def test__router__version_fallback_to_generic__uses_none_version_handler() -> None:
    # Arrange
    router = EventRouter()
    generic = _RecordingHandler(handler_completed())
    router.register_handler("user.created", "users", generic)

    # Act
    result = await router.dispatch(_inbox_event(schema_version="9.9.9"), "users", _fake_repo())

    # Assert
    assert result.success is True
    assert len(generic.calls) == 1


# ---------- dispatch: migration ----------


async def test__router__migration_available__migrates_and_dispatches() -> None:
    # Arrange
    router = EventRouter()
    target = _RecordingHandler(handler_completed())
    router.register_handler("migrating.event", "topic-m", target, schema_version="2.0.0")

    def upgrade(p: dict[str, Any]) -> dict[str, Any]:
        return {**p, "migrated": True}

    BaseEventSchema.register_migration("migrating.event", "1.0.0", "2.0.0", upgrade)
    event = _inbox_event(event_type="migrating.event", schema_version="1.0.0", payload={"v": 1})

    # Act
    result = await router.dispatch(event, "topic-m", _fake_repo())

    # Assert
    assert result.success is True
    assert len(target.calls) == 1
    delivered = target.calls[0][0]
    assert delivered.payload == {"v": 1, "migrated": True}
    assert delivered.schema_version == "2.0.0"


async def test__router__migration_scan__skips_unrelated_handlers() -> None:
    # Arrange: register handlers on a different topic and event_type to exercise
    # the loop-skip branch (line 117 -> 116 in registry.py).
    router = EventRouter()
    target = _RecordingHandler(handler_completed())
    other = _RecordingHandler(handler_completed())
    router.register_handler("scan.event", "topic-s", target, schema_version="2.0.0")
    router.register_handler("unrelated.event", "topic-other", other, schema_version="2.0.0")
    event = _inbox_event(event_type="scan.event", schema_version="1.0.0")

    # Act
    result = await router.dispatch(event, "topic-s", _fake_repo())

    # Assert: no migration registered, no generic handler -> failure.
    assert result.success is False
    assert other.calls == []


async def test__router__migration_raises_exception__logs_and_falls_through_to_no_handler() -> None:
    # Arrange
    router = EventRouter()
    different_version_handler = _RecordingHandler(handler_completed())
    router.register_handler("broken.event", "topic-b", different_version_handler, schema_version="2.0.0")

    def broken(_: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("migration boom")

    BaseEventSchema.register_migration("broken.event", "1.0.0", "2.0.0", broken)
    event = _inbox_event(event_type="broken.event", schema_version="1.0.0")

    # Act
    result = await router.dispatch(event, "topic-b", _fake_repo())

    # Assert: migration raised, no other matches -> no-handler failure.
    assert result.success is False
    assert different_version_handler.calls == []


# ---------- register_class / register_instance ----------


def test__router__register_class__registers_methods_under_class_topic() -> None:
    # Arrange
    router = EventRouter()

    class MyHandler(BaseEventHandler):
        topic = "orders"

        @event_handler("order.created")
        async def on_created(self, event: InboxEvent, **deps: Any) -> EventHandlerResult:
            return handler_completed()

        def not_a_handler(self) -> None:
            """Method without decorator should be skipped."""

    # Act
    router.register_class(MyHandler)

    # Assert
    assert "orders" in router.get_topics()
    assert "order.created" in router.get_event_types_for_topic("orders")


def test__router__register_instance__uses_method_topic_override() -> None:
    # Arrange
    router = EventRouter()

    class MyHandler(BaseEventHandler):
        topic = "default-topic"

        @event_handler("ev1", topic="override-topic")
        async def m1(self, event: InboxEvent, **deps: Any) -> EventHandlerResult:
            return handler_completed()

    # Act
    router.register_instance(MyHandler())

    # Assert
    assert "override-topic" in router.get_topics()
    assert "default-topic" not in router.get_topics()


def test__router__register_class_no_topic__raises_value_error() -> None:
    # Arrange
    router = EventRouter()

    class NoTopic(BaseEventHandler):
        @event_handler("ev")
        async def on_ev(self, event: InboxEvent, **deps: Any) -> EventHandlerResult:
            return handler_completed()

    # Act / Assert
    with pytest.raises(ValueError, match="Topic not specified"):
        router.register_class(NoTopic)


def test__router__register_instance_with_external_topic__uses_external() -> None:
    # Arrange
    router = EventRouter()

    class NoClassTopic(BaseEventHandler):
        @event_handler("ev")
        async def on_ev(self, event: InboxEvent, **deps: Any) -> EventHandlerResult:
            return handler_completed()

    # Act
    router.register_instance(NoClassTopic(), topic="external")

    # Assert
    assert "external" in router.get_topics()


# ---------- get_event_types_for_topic ----------


def test__router__get_event_types_for_unknown_topic__returns_empty_set() -> None:
    # Arrange
    router = EventRouter()

    # Act / Assert
    assert router.get_event_types_for_topic("nope") == set()
