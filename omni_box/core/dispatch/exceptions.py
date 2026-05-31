from omni_box.core.exceptions import OmniBoxError


class DispatcherError(OmniBoxError):
    """Base class for dispatcher errors."""


class HandlerAlreadyRegisteredError(DispatcherError):
    """Raised when a handler is already registered for a topic and event type."""
