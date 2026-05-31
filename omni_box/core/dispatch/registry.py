from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

import structlog

from ..models.entities import InboxEvent
from ..models.schemas import BaseEventSchema
from ..protocols import InboxEventRepository, InboxHandler
from ..services.results import EventHandlerResult, coerce_handler_outcome
from .exceptions import HandlerAlreadyRegisteredError
from .names import DispatchName, as_dispatch_str

if TYPE_CHECKING:
    from .base import BaseEventHandler


class EventRouter:
    """Registry and dispatcher for event handlers."""

    def __init__(self, normalize_topic: Callable[[str], str] | None = None) -> None:
        self._handlers: dict[tuple[str, str, str | None], InboxHandler] = {}
        self._topic_handlers: dict[str, set[str]] = {}
        self._normalize_topic = normalize_topic
        self._logger = structlog.get_logger(__name__)

    def register_handler(
        self,
        event_type: DispatchName,
        topic: DispatchName,
        handler: InboxHandler,
        schema_version: str | None = None,
        handler_name: str | None = None,
    ) -> None:
        """Register single event handler."""
        et = as_dispatch_str(event_type)
        top = as_dispatch_str(topic)
        if self._normalize_topic:
            top = self._normalize_topic(top)

        key = (top, et, schema_version)
        if key in self._handlers:
            raise HandlerAlreadyRegisteredError(f"Handler for {top}.{et} (v{schema_version}) already registered")

        self._handlers[key] = handler
        self._topic_handlers.setdefault(top, set()).add(et)
        self._logger.debug(
            "Handler registered",
            topic=top,
            event_type=et,
            schema_version=schema_version,
            handler_name=handler_name,
        )

    def register_class(self, handler_class: type[BaseEventHandler], topic: DispatchName | None = None) -> None:
        """Auto-register all @event_handler decorated methods from class."""
        self.register_instance(handler_class(), topic=topic)

    def register_instance(self, instance: BaseEventHandler, topic: DispatchName | None = None) -> None:
        """Auto-register all @event_handler decorated methods from instance."""
        handler_class = type(instance)
        class_topic = getattr(instance, "topic", topic)
        if not class_topic:
            raise ValueError(f"Topic not specified for {handler_class.__name__}")
        class_topic_str = as_dispatch_str(class_topic)

        registered = 0

        for name, method in inspect.getmembers(instance, predicate=inspect.ismethod):
            if not getattr(method, "_is_event_handler", False):
                continue

            event_type = cast(str, method._event_type)  # type: ignore[attr-defined]
            schema_version = cast(str | None, getattr(method, "_schema_version", None))
            raw_method_topic = cast(str | None, getattr(method, "_event_topic", None))
            method_topic = raw_method_topic if raw_method_topic is not None else class_topic_str

            self.register_handler(
                event_type=event_type,
                topic=method_topic,
                schema_version=schema_version,
                handler=cast(InboxHandler, method),
                handler_name=f"{handler_class.__name__}.{name}",
            )
            registered += 1

        self._logger.info(
            "Handler instance registered",
            class_name=handler_class.__name__,
            handlers_count=registered,
        )

    async def dispatch(
        self,
        event: InboxEvent,
        topic: DispatchName,
        repo: InboxEventRepository,
        **dependencies: object,
    ) -> EventHandlerResult:
        """Dispatch event to registered handler."""
        topic_str = as_dispatch_str(topic)
        if self._normalize_topic:
            topic_str = self._normalize_topic(topic_str)

        event_type_str = as_dispatch_str(event.event_type)
        schema_version = event.schema_version

        # 1. Try exact match
        key_v = (topic_str, event_type_str, schema_version)
        handler = self._handlers.get(key_v)

        # 2. Try migration
        if not handler:
            for (t, et, v), h in self._handlers.items():
                if t == topic_str and et == event_type_str and v != schema_version:
                    try:
                        migrated_payload = BaseEventSchema.migrate_payload(
                            event_type=event_type_str,
                            payload=event.payload,
                            from_version=schema_version,
                            to_version=v or "",
                        )
                        if migrated_payload != event.payload:
                            event = event.model_copy(update={"payload": migrated_payload, "schema_version": v})
                            handler = h
                            self._logger.info(
                                "Migrated event", event_id=str(event.id), from_version=schema_version, to_version=v
                            )
                            break
                    except Exception:
                        self._logger.exception("Failed migration", event_id=str(event.id))

        # 3. Try generic match
        if not handler:
            key_any = (topic_str, event_type_str, None)
            handler = self._handlers.get(key_any)

        if not handler:
            msg = f"No handler for topic={topic_str!r} event_type={event_type_str!r} v={schema_version!r}"
            self._logger.warning("No handler", event_id=str(event.id), topic=topic_str, event_type=event_type_str)
            return EventHandlerResult(success=False, error_message=msg, count_as_attempt=True)

        raw = await handler(event, repo, **dependencies)
        return coerce_handler_outcome(raw)

    def get_topics(self) -> set[str]:
        return set(self._topic_handlers.keys())

    def get_event_types_for_topic(self, topic: DispatchName) -> set[str]:
        return self._topic_handlers.get(as_dispatch_str(topic), set())
