"""Root test configuration (no DB fixtures here).

Postgres-backed fixtures live in ``tests/integration/conftest.py``, since only
the tests under ``tests/integration/`` need a database; the unit tests in
``tests/unit/infra/storage/postgres/`` mock the session instead.
This keeps pure unit tests runnable without Docker.
"""

from __future__ import annotations
