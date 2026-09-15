"""Unit tests for ``omni_box.contrib.dishka`` DI integration."""

# NOTE: deliberately no ``from __future__ import annotations`` so that
# ``inspect.signature`` exposes real class objects in parameter annotations
# rather than forward-reference strings. The DI router relies on real types
# to look up providers in the container.

import uuid
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide

from omni_box import EventRouter, InboxMetrics, OutboxMetrics, OutboxPublisher, event_handler
from omni_box.contrib.dishka import (
    DefaultTopicDenormalizer,
    DIAwareEventRouter,
    EventDispatcherProvider,
    PrometheusInboxMetricsProvider,
    PrometheusOutboxMetricsProvider,
    create_di_router,
)
from omni_box.contrib.settings import BaseInboxSettings, BaseOutboxSettings, OmniBoxObservabilitySettings
from omni_box.core.dispatch.base import BaseEventHandler
from omni_box.core.services.results import EventHandlerResult, handler_completed
from omni_box.infra.metrics import get_inbox_metrics, get_outbox_metrics

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


# -------- Prometheus metrics providers --------


class _SettingsProvider(Provider):
    """What an application registers: its inbox and outbox settings under the kit's own keys."""

    scope = Scope.APP

    def __init__(self, *, inbox_metrics: bool, outbox_metrics: bool) -> None:
        super().__init__()
        self._inbox_metrics = inbox_metrics
        self._outbox_metrics = outbox_metrics

    @provide
    def inbox(self) -> BaseInboxSettings:
        return BaseInboxSettings(observability=OmniBoxObservabilitySettings(enable_metrics=self._inbox_metrics))

    @provide
    def outbox(self) -> BaseOutboxSettings:
        return BaseOutboxSettings(observability=OmniBoxObservabilitySettings(enable_metrics=self._outbox_metrics))


class _PublisherProvider(Provider):
    """A consumer building its service from the key the metrics provider hands out."""

    scope = Scope.APP

    @provide
    def publisher(self, metrics: OutboxMetrics | None) -> OutboxPublisher:
        return OutboxPublisher(MagicMock(), MagicMock(), metrics=metrics)


def _unique_prefix() -> str:
    # The default registry is process-wide; each test registers its own series.
    return f"di_{uuid.uuid4().hex[:8]}"


def _container(*providers: Provider, inbox_metrics: bool, outbox_metrics: bool, prefix: str) -> AsyncContainer:
    return make_async_container(
        EventDispatcherProvider(),
        _SettingsProvider(inbox_metrics=inbox_metrics, outbox_metrics=outbox_metrics),
        PrometheusInboxMetricsProvider(prefix=prefix),
        PrometheusOutboxMetricsProvider(prefix=prefix),
        *providers,
    )


async def test__metrics_providers__metrics_enabled__provide_the_cached_collectors() -> None:
    # Arrange
    prefix = _unique_prefix()
    container = _container(inbox_metrics=True, outbox_metrics=True, prefix=prefix)

    # Act
    inbox = await container.get(InboxMetrics | None)
    outbox = await container.get(OutboxMetrics | None)
    await container.close()

    # Assert
    assert isinstance(inbox, InboxMetrics)
    assert isinstance(outbox, OutboxMetrics)
    assert inbox is get_inbox_metrics(prefix)
    assert outbox is get_outbox_metrics(prefix)


@pytest.mark.parametrize(
    ("inbox_metrics", "outbox_metrics"),
    [(False, True), (True, False)],
    ids=["inbox-off", "outbox-off"],
)
async def test__metrics_providers__section_disabled__provides_none_for_that_section(
    inbox_metrics: bool, outbox_metrics: bool
) -> None:
    # Arrange
    container = _container(inbox_metrics=inbox_metrics, outbox_metrics=outbox_metrics, prefix=_unique_prefix())

    # Act
    inbox = await container.get(InboxMetrics | None)
    outbox = await container.get(OutboxMetrics | None)
    await container.close()

    # Assert
    assert (inbox is not None) is inbox_metrics
    assert (outbox is not None) is outbox_metrics


async def test__metrics_providers__container_rebuilt__does_not_register_the_series_twice() -> None:
    """The reporter's case: a container per test asks for the collectors again."""
    # Arrange
    prefix = _unique_prefix()
    first = _container(inbox_metrics=True, outbox_metrics=True, prefix=prefix)
    first_inbox = await first.get(InboxMetrics | None)
    first_outbox = await first.get(OutboxMetrics | None)
    await first.close()

    # Act
    second = _container(inbox_metrics=True, outbox_metrics=True, prefix=prefix)
    second_inbox = await second.get(InboxMetrics | None)
    second_outbox = await second.get(OutboxMetrics | None)
    await second.close()

    # Assert
    assert second_inbox is first_inbox
    assert second_outbox is first_outbox


async def test__outbox_metrics_provider__consumer_service__processor_receives_the_collector() -> None:
    # Arrange
    prefix = _unique_prefix()
    container = _container(_PublisherProvider(), inbox_metrics=True, outbox_metrics=True, prefix=prefix)

    # Act
    publisher = await container.get(OutboxPublisher)
    await container.close()

    # Assert
    assert publisher._metrics is get_outbox_metrics(prefix)
    assert publisher._processor._metrics is get_outbox_metrics(prefix)
