from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from typing import Any, ClassVar, Self

from pydantic import BaseModel

from ..exceptions import OmniBoxError


class SchemaResolutionError(OmniBoxError, ValueError):
    """Raised when ``BaseEventSchema.resolve`` cannot find a matching schema.

    Inherits from ``ValueError`` as well so existing callers that catch
    ``ValueError`` keep working while new code can target the domain hierarchy.
    """


class BaseEventSchema(BaseModel, ABC):
    """Base class for all event payload schemas with self-registration and versioning support."""

    _registry: ClassVar[dict[str, dict[str, type["BaseEventSchema"]]]] = {}
    _migrations: ClassVar[dict[str, dict[tuple[str, str], Callable[[dict[str, Any]], dict[str, Any]]]]] = {}

    @classmethod
    def __init_subclass__(cls, event_type: str | None = None, version_prefix: str | None = None, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if event_type:
            if event_type not in cls._registry:
                cls._registry[event_type] = {}

            # If a version_prefix is provided (e.g. "1" or "1.2"), use it as the registration key.
            # Otherwise, use the full version from schema_version().
            key = version_prefix or cls.schema_version()
            cls._registry[event_type][key] = cls

    @classmethod
    @abstractmethod
    def schema_version(cls) -> str:
        """Return full Semantic Version (e.g., '1.0.0')."""

    @classmethod
    def register_migration(
        cls,
        event_type: str,
        from_version: str,
        to_version: str,
        func: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        """Register a migration function between two versions of an event type."""
        if event_type not in cls._migrations:
            cls._migrations[event_type] = {}
        cls._migrations[event_type][(from_version, to_version)] = func

    @classmethod
    def migrate_payload(
        cls,
        event_type: str,
        payload: dict[str, Any],
        from_version: str | None,
        to_version: str,
    ) -> dict[str, Any]:
        """Migrate payload to the target version if migrations are available.

        If from_version is None, it assumes the earliest known version or skips.
        Currently supports single-step migrations.
        """
        if from_version == to_version:
            return payload

        if not from_version:
            return payload

        type_migrations = cls._migrations.get(event_type)
        if not type_migrations:
            return payload

        # Simple path: try to find a direct migration
        func = type_migrations.get((from_version, to_version))
        if func:
            return func(payload)

        # Multi-step migration pathfinding using BFS
        queue: deque[tuple[str, dict[str, Any]]] = deque([(from_version, payload)])
        visited: set[str] = {from_version}

        while queue:
            current_v, current_p = queue.popleft()

            for (v_start, v_end), migrate_func in type_migrations.items():
                if v_start == current_v and v_end not in visited:
                    new_p = migrate_func(current_p)
                    if v_end == to_version:
                        return new_p

                    visited.add(v_end)
                    queue.append((v_end, new_p))

        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> Self:
        """Create schema instance from raw payload dictionary."""
        return cls.model_validate(payload)

    @classmethod
    def resolve(cls, event_type: str, version: str | None) -> type["BaseEventSchema"]:
        """Resolve the best matching schema class based on event type and version prefix.

        Raises:
            SchemaResolutionError: if no schemas are registered for the event
                type, the version is unknown, or no prefix match exists.
        """
        type_map = cls._registry.get(event_type)
        if not type_map:
            raise SchemaResolutionError(f"No schemas registered for event type: {event_type}")

        if version is None:
            raise SchemaResolutionError(f"No schema_version provided for event type: {event_type}")

        # Longest Prefix Match for SemVer (e.g., for "1.2.3" try "1.2.3", then "1.2", then "1")
        parts = version.split(".")
        prefixes = [".".join(parts[:i]) for i in range(len(parts), 0, -1)]

        for prefix in prefixes:
            if prefix in type_map:
                return type_map[prefix]

        raise SchemaResolutionError(f"Unsupported version {version} for event type {event_type}")

    def to_payload(self) -> dict[str, object]:
        """Convert to dict suitable for OutboxEvent.payload."""
        return self.model_dump(mode="json")
