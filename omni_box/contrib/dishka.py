"""Optional Dishka DI integration for the omni-box event dispatcher.

Available with the ``dishka`` extra:

    pip install "omni-box[dishka]"
"""

from __future__ import annotations

import inspect
from typing import Protocol, cast, runtime_checkable

import structlog

try:
    from dishka import AsyncContainer, Provider, Scope, provide
except ImportError as _e:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "dishka is required for omni_box.contrib.dishka. Install with: pip install 'omni-box[dishka]'"
    ) from _e

from .. import EventRouter, InboxEvent, InboxEventRepository
from ..core.dispatch.names import DispatchName, as_dispatch_str
from ..core.services.results import EventHandlerResult, coerce_handler_outcome

logger = structlog.get_logger(__name__)


@runtime_checkable
class TopicDenormalizer(Protocol):
    """Protocol for topic name denormalization (e.g. stripping prefixes)."""

    def denormalize(self, topic: str) -> str:
        """Remove prefix from topic name."""
        ...


class DIAwareEventRouter:
    """Event router with automatic dependency injection via Dishka.

    Wraps :class:`EventRouter` to automatically resolve handler dependencies
    from a Dishka container based on the method signature.
    """

    def __init__(self, router: EventRouter, container: AsyncContainer):
        self._router = router
        self._container = container
        self._handler_signatures: dict[tuple[str, str, str | None], inspect.Signature] = {}

    def register_class(self, handler_class: type, topic: DispatchName | None = None) -> None:
        """Register a handler class and cache method signatures for DI resolution."""
        self._router.register_class(handler_class, topic)

        class_topic = getattr(handler_class, "topic", topic)
        class_topic_str = as_dispatch_str(cast(DispatchName, class_topic))
        instance = handler_class()

        for _name, method in inspect.getmembers(instance, predicate=inspect.ismethod):
            if not getattr(method, "_is_event_handler", False):
                continue

            event_type = method._event_type  # type: ignore[attr-defined]
            schema_version = cast(str | None, getattr(method, "_schema_version", None))
            raw_method_topic = cast(str | None, getattr(method, "_event_topic", None))
            method_topic_str = raw_method_topic if raw_method_topic is not None else class_topic_str
            key = (method_topic_str, as_dispatch_str(cast(DispatchName, event_type)), schema_version)

            self._handler_signatures[key] = inspect.signature(method)

    def _dispatch_key(self, topic: DispatchName, event: InboxEvent) -> tuple[str, str]:
        topic_str = as_dispatch_str(topic)
        if self._router._normalize_topic:
            topic_str = self._router._normalize_topic(topic_str)
        return topic_str, as_dispatch_str(event.event_type)

    async def dispatch_with_di(
        self, event: InboxEvent, topic: DispatchName, repo: InboxEventRepository
    ) -> EventHandlerResult:
        """Dispatch an event resolving handler dependencies from the container."""
        topic_str, event_type_str = self._dispatch_key(topic, event)
        schema_version = event.schema_version

        key_v = (topic_str, event_type_str, schema_version)
        handler = self._router._handlers.get(key_v)  # type: ignore[attr-defined]

        if not handler:
            key_any = (topic_str, event_type_str, None)
            handler = self._router._handlers.get(key_any)  # type: ignore[attr-defined]
            key = key_any if handler else key_v
        else:
            key = key_v

        if not handler:
            msg = f"No handler registered for topic={topic_str!r} event_type={event_type_str!r}"
            logger.warning(
                "No handler registered for inbox event",
                event_id=str(event.id),
                topic=topic_str,
                event_type=event_type_str,
                error_message=msg,
            )
            return EventHandlerResult(
                success=False,
                error_message=msg,
                count_as_attempt=True,
            )

        sig = self._handler_signatures.get(key)
        if not sig:
            raw = await handler(event, repo)
            return coerce_handler_outcome(raw)

        kwargs: dict[str, object] = {}
        async with self._container() as request_container:
            for param_name, param in sig.parameters.items():
                if param_name in ("self", "event", "repo"):
                    continue

                if param.annotation != inspect.Parameter.empty:
                    try:
                        dep = await request_container.get(param.annotation)
                        kwargs[param_name] = dep
                    except Exception as e:
                        logger.warning(
                            "Failed to resolve dependency",
                            param=param_name,
                            type=param.annotation,
                            error=str(e),
                        )

            raw = await handler(event, repo, **kwargs)
            return coerce_handler_outcome(raw)

    def get_base_router(self) -> EventRouter:
        """Return the underlying base router."""
        return self._router


def create_di_router(
    router: EventRouter,
    container: AsyncContainer,
) -> DIAwareEventRouter:
    """Create a DI-aware router that wraps an existing :class:`EventRouter`."""
    return DIAwareEventRouter(router, container)


class DefaultTopicDenormalizer:
    """Default identity denormalizer (returns the topic name as-is)."""

    def denormalize(self, topic: str) -> str:
        return topic


class EventDispatcherProvider(Provider):
    """Dishka provider for the omni-box event dispatcher.

    Provides:
        - :class:`TopicDenormalizer`: default identity denormalizer (APP scope).
        - :class:`EventRouter`: global router instance (APP scope).
        - :class:`DIAwareEventRouter`: DI-enabled wrapper (REQUEST scope).
    """

    @provide(scope=Scope.APP)
    def get_default_denormalizer(self) -> TopicDenormalizer:
        return DefaultTopicDenormalizer()

    @provide(scope=Scope.APP)
    def get_router(self, denormalizer: TopicDenormalizer) -> EventRouter:
        return EventRouter(normalize_topic=denormalizer.denormalize)

    @provide(scope=Scope.REQUEST)
    def get_di_router(
        self,
        router: EventRouter,
        container: AsyncContainer,
    ) -> DIAwareEventRouter:
        return DIAwareEventRouter(router, container)


__all__ = [
    "DIAwareEventRouter",
    "DefaultTopicDenormalizer",
    "EventDispatcherProvider",
    "TopicDenormalizer",
    "create_di_router",
]
