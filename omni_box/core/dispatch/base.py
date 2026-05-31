from abc import ABC
from enum import StrEnum
from typing import ClassVar


class BaseEventHandler(ABC):
    """Base class for event handlers.

    Subclasses should define topic at class level and use @event_handler
    decorator on methods to register them.

    Example:
        class UserEventHandlers(BaseEventHandler):
            topic = "users"

            @event_handler(UserEventType.CREATED)
            async def on_user_created(self, event: InboxEvent, uow: AsyncUnitOfWork):
                # Handle event
                pass
    """

    topic: ClassVar[str | StrEnum]  # Must be defined in subclass
