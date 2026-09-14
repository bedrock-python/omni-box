"""Unit tests for the names ``get_event_constraints`` gives the event tables.

A check constraint is declared with the finished name ``ck_<table>_<rule>`` unless the
``MetaData`` carries a ``ck`` naming convention that interpolates ``%(constraint_name)s``;
then the bases hand the convention the bare rule and it builds the name. A finished name
under such a convention is qualified twice, runs past PostgreSQL's 63-byte limit and is
truncated with a hash (issue #36). These tests compile the DDL through the PostgreSQL
dialect and never touch a database; the catalog round trip lives in
``tests/integration/postgres/test_orm.py``.
"""

from __future__ import annotations

import re

import pytest
from sqlalchemy import CheckConstraint, MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import CreateTable

from omni_box.infra.storage.postgres.orm import (
    InboxEventDBBase,
    InboxEventPartitionedDBBase,
    OutboxEventDBBase,
    OutboxEventPartitionedDBBase,
    get_event_constraints,
)

pytestmark = pytest.mark.unit

PG_MAX_IDENTIFIER = 63
CHECK_RULES = ("attempts_valid", "completed_status_consistency", "lock_consistency")

# sqlalchemy-foundation-kit's DB_NAMING_CONVENTION
FOUNDATION_KIT_CONVENTION = {
    "ix": "%(column_0_label)s_idx",
    "uq": "%(table_name)s_%(column_0_name)s_key",
    "ck": "%(table_name)s_%(constraint_name)s_check",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}
# The convention SQLAlchemy's documentation recommends
SQLALCHEMY_DOCS_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# (abstract base, the prefix its __table_args__ passes to get_event_constraints)
EVENT_BASES = (
    (OutboxEventDBBase, "outbox_events"),
    (InboxEventDBBase, "inbox_events"),
    (OutboxEventPartitionedDBBase, "outbox_events_p"),
    (InboxEventPartitionedDBBase, "inbox_events_p"),
)


def _bind(naming_convention: dict[str, str] | None) -> dict[str, Table]:
    """Bind the four abstract bases to a fresh DeclarativeBase and return its tables by name."""

    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention=naming_convention)

    for base, _ in EVENT_BASES:
        type(f"Concrete{base.__name__}", (Base, base), {})
    return {table.name: table for table in Base.metadata.sorted_tables}


def _check_names(table: Table) -> set[str]:
    return {c.name for c in table.constraints if isinstance(c, CheckConstraint)}


def _ddl_check_names(table: Table) -> set[str]:
    ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    return set(re.findall(r"CONSTRAINT (\S+) CHECK", ddl))


def _index_names(table: Table) -> set[str]:
    return {index.name for index in table.indexes}


def test__event_bases__no_naming_convention__check_names_are_the_documented_ones() -> None:
    # Act
    tables = _bind(None)

    # Assert
    for base, prefix in EVENT_BASES:
        table = tables[base.__tablename__]
        assert _check_names(table) == {f"ck_{prefix}_{rule}" for rule in CHECK_RULES}
        assert _ddl_check_names(table) == _check_names(table)


@pytest.mark.parametrize(
    ("naming_convention", "expected"),
    [
        pytest.param(FOUNDATION_KIT_CONVENTION, "{table}_{rule}_check", id="foundation-kit"),
        pytest.param(SQLALCHEMY_DOCS_CONVENTION, "ck_{table}_{rule}", id="sqlalchemy-docs"),
    ],
)
def test__event_bases__ck_convention__check_names_are_qualified_once_and_fit_postgres(
    naming_convention: dict[str, str], expected: str
) -> None:
    # Act
    tables = _bind(naming_convention)

    # Assert
    for base, _ in EVENT_BASES:
        table = tables[base.__tablename__]
        names = _check_names(table)
        assert names == {expected.format(table=table.name, rule=rule) for rule in CHECK_RULES}
        assert all(len(name) <= PG_MAX_IDENTIFIER for name in names)
        assert _ddl_check_names(table) == names


@pytest.mark.parametrize(
    "naming_convention",
    [
        pytest.param(FOUNDATION_KIT_CONVENTION, id="foundation-kit"),
        pytest.param(SQLALCHEMY_DOCS_CONVENTION, id="sqlalchemy-docs"),
    ],
)
def test__event_bases__ix_convention__explicit_index_names_are_kept(naming_convention: dict[str, str]) -> None:
    # Arrange
    without_convention = _bind(None)

    # Act
    tables = _bind(naming_convention)

    # Assert
    for name, table in tables.items():
        assert _index_names(table) == _index_names(without_convention[name])


def test__event_bases__two_declarative_bases__each_gets_its_own_table_args() -> None:
    # Act
    first = _bind(None)
    second = _bind(None)

    # Assert
    for base, _ in EVENT_BASES:
        name = base.__tablename__
        assert first[name] is not second[name]
        assert _check_names(second[name]) == _check_names(first[name])
        assert _index_names(second[name]) == _index_names(first[name])


@pytest.mark.parametrize(
    "metadata",
    [
        pytest.param(None, id="none"),
        pytest.param(MetaData(), id="default"),
        pytest.param(MetaData(naming_convention={"ck": "%(table_name)s_%(column_0_name)s_check"}), id="ck-by-column"),
    ],
)
def test__get_event_constraints__no_ck_rule_interpolating_the_name__check_names_are_finished(
    metadata: MetaData | None,
) -> None:
    # Act
    constraints = get_event_constraints("custom_events", metadata=metadata)

    # Assert
    names = {c.name for c in constraints if isinstance(c, CheckConstraint)}
    assert names == {f"ck_custom_events_{rule}" for rule in CHECK_RULES}


def test__get_event_constraints__ck_rule_interpolating_the_name__check_names_are_the_bare_rules() -> None:
    # Arrange
    metadata = MetaData(naming_convention=FOUNDATION_KIT_CONVENTION)

    # Act
    constraints = get_event_constraints("custom_events", metadata=metadata)

    # Assert
    names = {c.name for c in constraints if isinstance(c, CheckConstraint)}
    assert names == set(CHECK_RULES)
