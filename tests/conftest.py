"""Root test configuration (no DB fixtures here).

Postgres-backed fixtures live in the conftest files of the directories that
actually need them: ``tests/integration/`` and ``tests/unit/storage/postgres/``.
This keeps pure unit tests runnable without Docker.
"""

from __future__ import annotations
