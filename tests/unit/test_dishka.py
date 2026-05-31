"""Unit tests for ``omni_box.contrib.dishka`` DI integration."""

# NOTE: deliberately no ``from __future__ import annotations`` so that
# ``inspect.signature`` exposes real class objects in parameter annotations
# rather than forward-reference strings. The DI router relies on real types
# to look up providers in the container.

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest

from omni_box import EventRouter, event_handler
from omni_box.contrib.dishka import (
    DefaultTopicDenormalizer,
    DIAwareEventRouter,
    create_di_router,
)
from omni_box.core.dispatch.base import BaseEventHandler
from omni_box.core.services.results import EventHandlerResult, handler_completed

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_event() -> MagicMock:
    event = MagicMock()
    event.id = "00000000-0000-0000-0000-000000000001"
    event.event_type = "user.created"
    event.schema_version = "1.0.0"
    return event


@pytest.fixture
def fake_repo() -> MagicMock:
    return MagicMock()


class _FakeRequestContainer:
    def __init__(self, deps: dict[type, object]) -> None:
        self._deps = deps

    async def get(self, annotation: type) -> object:
        if annotation in self._deps:
            return self._deps[annotation]
        raise LookupError(f"no provider for {annotation!r}")


class _FakeContainer:
    def __init__(self, deps: dict[type, object] | None = None) -> None:
        self._deps = deps or {}

    @asynccontextmanager
    async def __call__(self) -> Any:
        yield _FakeRequestContainer(self._deps)


def _make_container(deps: dict[type, object] | None = None) -> _FakeContainer:
    """Build an ``AsyncContainer``-like fake with a configurable ``get``."""
    return _FakeContainer(deps)


def test__default_topic_denormalizer__identity() -> None:
    assert DefaultTopicDenormalizer().denormalize("users.v1") == "users.v1"


def test__create_di_router__returns_wrapper() -> None:
    base = EventRouter()
    container = MagicMock()

    di = create_di_router(base, container)

    assert isinstance(di, DIAwareEventRouter)
    assert di.get_base_router() is base


async def test__dispatch_with_di__no_handler__returns_failure_result(
    fake_event: MagicMock, fake_repo: MagicMock
) -> None:
    base = EventRouter()
    di = DIAwareEventRouter(base, MagicMock())

    result = await di.dispatch_with_di(fake_event, topic="users", repo=fake_repo)

    assert isinstance(result, EventHandlerResult)
    assert result.success is False
    assert result.count_as_attempt is True


async def test__dispatch_with_di__handler_with_no_extra_deps__invoked(
    fake_event: MagicMock, fake_repo: MagicMock
) -> None:
    base = EventRouter()

    invoked: dict[str, object] = {}

    class Handlers(BaseEventHandler):
        topic = "users"

        @event_handler(event_type="user.created")
        async def on_created(self, event: object, repo: object) -> EventHandlerResult:
            invoked["event"] = event
            invoked["repo"] = repo
            return handler_completed()

    container = _make_container()
    di = DIAwareEventRouter(base, container)
    di.register_class(Handlers)

    result = await di.dispatch_with_di(fake_event, topic="users", repo=fake_repo)

    assert result.success is True
    assert invoked["event"] is fake_event
    assert invoked["repo"] is fake_repo


async def test__dispatch_with_di__handler_with_typed_dep__resolves_via_container(
    fake_event: MagicMock, fake_repo: MagicMock
) -> None:
    class FakeService:
        pass

    service_instance = FakeService()
    base = EventRouter()
    received: dict[str, object] = {}

    class Handlers(BaseEventHandler):
        topic = "users"

        @event_handler(event_type="user.created")
        async def on_created(self, event: object, repo: object, service: FakeService) -> EventHandlerResult:
            received["service"] = service
            return handler_completed()

    container = _make_container({FakeService: service_instance})
    di = DIAwareEventRouter(base, container)
    di.register_class(Handlers)

    result = await di.dispatch_with_di(fake_event, topic="users", repo=fake_repo)

    assert result.success is True
    assert received["service"] is service_instance


async def test__dispatch_with_di__container_lookup_failure__handler_called_without_dep(
    fake_event: MagicMock, fake_repo: MagicMock
) -> None:
    class FakeService:
        pass

    base = EventRouter()
    received: dict[str, object] = {}

    class Handlers(BaseEventHandler):
        topic = "users"

        @event_handler(event_type="user.created")
        async def on_created(
            self, event: object, repo: object, service: FakeService | None = None
        ) -> EventHandlerResult:
            received["service"] = service
            return handler_completed()

    container = _make_container()  # no FakeService registered
    di = DIAwareEventRouter(base, container)
    di.register_class(Handlers)

    result = await di.dispatch_with_di(fake_event, topic="users", repo=fake_repo)

    # Handler still runs; missing deps fall back to their defaults.
    assert result.success is True
    assert received["service"] is None
