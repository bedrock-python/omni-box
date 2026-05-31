"""Postgres- and Kafka-backed fixtures for integration tests."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

try:
    from testcontainers.kafka import KafkaContainer

    HAS_KAFKA_TESTCONTAINER = True
except ImportError:
    KafkaContainer = object  # type: ignore[assignment, misc]
    HAS_KAFKA_TESTCONTAINER = False

try:
    import docker
    from docker.errors import DockerException
except ImportError:
    docker = None  # type: ignore[assignment]
    DockerException = Exception  # type: ignore[assignment, misc]

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

try:
    from testcontainers.postgres import PostgresContainer

    HAS_TESTCONTAINERS = True
except ImportError:
    PostgresContainer = object  # type: ignore[assignment, misc]
    HAS_TESTCONTAINERS = False

from tests.models import Base


def is_docker_available() -> bool:
    """Check if docker is available to run integration tests."""
    if docker is None:
        return False
    try:
        client = docker.from_env()
        client.version()
    except (DockerException, Exception):
        return False
    else:
        return True


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start a PostgreSQL container for the test session."""
    if not HAS_TESTCONTAINERS:
        pytest.skip("testcontainers is not installed.")

    if not is_docker_available():
        pytest.skip("Docker is not available, skipping integration tests that require it.")

    with PostgresContainer("postgres:17-alpine") as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="session")
async def db_engine(postgres_container: PostgresContainer) -> AsyncGenerator[AsyncEngine, None]:
    """Create async database engine for testing."""
    url = postgres_container.get_connection_url()
    if "://" in url:
        _scheme, rest = url.split("://", 1)
        url = f"postgresql+asyncpg://{rest}"

    engine = create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        await conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS inbox_events_partitioned_default "
                "PARTITION OF inbox_events_partitioned DEFAULT"
            )
        )
        await conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS outbox_events_partitioned_default "
                "PARTITION OF outbox_events_partitioned DEFAULT"
            )
        )

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_db(db_engine: AsyncEngine) -> None:
    """Truncate all test tables before each test in this directory."""
    async with db_engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(text(f'TRUNCATE TABLE "{table.name}" RESTART IDENTITY CASCADE'))


@pytest_asyncio.fixture
async def async_session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Create async database session for testing."""
    async_session_maker = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session_maker() as session:
        yield session


# ---------- Kafka fixtures ----------


@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    """Start a Kafka container for the test session."""
    if not HAS_KAFKA_TESTCONTAINER:
        pytest.skip("testcontainers[kafka] is not installed.")
    if not is_docker_available():
        pytest.skip("Docker is not available for Kafka integration tests.")

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kc:
        yield kc


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container: KafkaContainer) -> str:
    """Return Kafka bootstrap-servers connection string."""
    return kafka_container.get_bootstrap_server()


@pytest_asyncio.fixture
async def kafka_topic(kafka_bootstrap: str) -> AsyncGenerator[str, None]:
    """Create a unique topic per test and clean it up afterwards."""
    topic = f"test-{uuid.uuid4().hex[:8]}"
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        yield topic
        await admin.delete_topics([topic])
    finally:
        await admin.close()


@pytest_asyncio.fixture
async def kafka_producer(kafka_bootstrap: str) -> AsyncGenerator[AIOKafkaProducer, None]:
    """Pre-started aiokafka producer with idempotent + ack=all delivery."""
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        enable_idempotence=True,
        acks="all",
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@pytest_asyncio.fixture
async def make_kafka_consumer(
    kafka_bootstrap: str,
) -> AsyncGenerator[
    object,  # callable factory; typed loosely on purpose to avoid leaking aiokafka generics
    None,
]:
    """Factory: ``await make_kafka_consumer(topic, group_id=...) -> AIOKafkaConsumer`` (started)."""
    created: list[AIOKafkaConsumer] = []

    async def _factory(topic: str, *, group_id: str, auto_offset_reset: str = "earliest") -> AIOKafkaConsumer:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
        )
        await consumer.start()
        created.append(consumer)
        return consumer

    try:
        yield _factory
    finally:
        for c in created:
            await c.stop()
