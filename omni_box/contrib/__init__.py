"""Optional, third-party-integration modules.

Each submodule under :mod:`omni_box.contrib` is opt-in and requires an extra
to be installed (see ``pyproject.toml`` ``[project.optional-dependencies]``).
The core ``omni_box`` package never imports anything from ``contrib`` at
runtime — these modules exist purely as convenience adapters for popular
external libraries (Pydantic Settings, Dishka, ...).

Public symbols must be imported explicitly from each submodule
(e.g. ``from omni_box.contrib.dishka import EventDispatcherProvider``).
"""

from __future__ import annotations

__all__: list[str] = []
