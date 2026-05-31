"""Dispatching and routing infrastructure."""

from .base import BaseEventHandler
from .decorators import event_handler
from .names import DispatchName, as_dispatch_str
from .processor import create_dispatching_handler
from .registry import EventRouter

__all__ = [
    "BaseEventHandler",
    "DispatchName",
    "EventRouter",
    "as_dispatch_str",
    "create_dispatching_handler",
    "event_handler",
]
