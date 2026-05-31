from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from ..core.constants import (
    DEFAULT_PROCESS_TIMEOUT_SECONDS,
    DEFAULT_PUBLISH_TIMEOUT_SECONDS,
)
from ..core.models.entities import InboxEvent, OutboxEvent
from ..core.pipeline.builder import EventProcessorBuilder
from ..core.pipeline.steps import (
    CircuitBreakerStep,
    DLQStep,
    DLQStorage,
    HandlerExecutionStep,
    MetricsStep,
    OpenTelemetryStep,
    SiblingDeduplicationStep,
)
from ..core.pipeline.strategies.fetch import FilteredFetchStrategy
from ..core.services.processor import EventBatchProcessor

if TYPE_CHECKING:
    from ..core.dispatch.registry import EventRouter
    from ..core.pipeline.step import ProcessingStep
    from ..core.protocols import (
        EventPublisher,
        InboxEventRepository,
        OutboxEventRepository,
    )
    from ..core.protocols.metrics import InboxMetrics, OutboxMetrics
    from ..core.services.results import EventHandlerResult


def create_inbox_processor(
    repo: InboxEventRepository,
    handler: Callable[[InboxEvent, InboxEventRepository], Awaitable[EventHandlerResult | None]],
    *,
    skip_duplicate_siblings: bool = True,
    filter_sources: list[str] | None = None,
    process_timeout: float = DEFAULT_PROCESS_TIMEOUT_SECONDS,
    metrics: InboxMetrics | None = None,
    dlq_storage: DLQStorage[InboxEvent] | None = None,
    enable_otel: bool = False,
    enable_circuit_breaker: bool = False,
    circuit_breaker_failure_threshold: int = 5,
    circuit_breaker_recovery_timeout: int = 60,
    job_name: str = "inbox_processor",
    additional_steps_before: list[ProcessingStep[InboxEvent]] | None = None,
    additional_steps_after: list[ProcessingStep[InboxEvent]] | None = None,
) -> EventBatchProcessor[InboxEvent]:
    """Preset factory for common inbox processing scenarios."""
    builder = EventProcessorBuilder[InboxEvent](repo)

    if filter_sources:
        builder.with_fetch_strategy(FilteredFetchStrategy(sources=filter_sources))

    if enable_otel:
        builder.add_step(OpenTelemetryStep(service_name=job_name))

    if enable_circuit_breaker:
        builder.add_step(
            CircuitBreakerStep(
                failure_threshold=circuit_breaker_failure_threshold,
                recovery_timeout_seconds=circuit_breaker_recovery_timeout,
            )
        )

    if dlq_storage:
        builder.add_step(DLQStep(dlq_storage))

    if additional_steps_before:
        for step in additional_steps_before:
            builder.add_step(step)

    if skip_duplicate_siblings:
        builder.add_step(SiblingDeduplicationStep(enabled=True))

    builder.add_step(HandlerExecutionStep(handler, timeout=process_timeout))  # type: ignore[arg-type]

    if additional_steps_after:
        for step in additional_steps_after:
            builder.add_step(step)

    if metrics:
        builder.add_step(MetricsStep(metrics))
        builder.with_metrics(metrics)

    return builder.with_job_name(job_name).build()


def create_outbox_processor(
    repo: OutboxEventRepository,
    publisher: EventPublisher,
    *,
    publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    metrics: OutboxMetrics | None = None,
    dlq_storage: DLQStorage[OutboxEvent] | None = None,
    enable_otel: bool = False,
    enable_circuit_breaker: bool = False,
    circuit_breaker_failure_threshold: int = 5,
    circuit_breaker_recovery_timeout: int = 60,
    job_name: str = "outbox_processor",
    additional_steps_before: list[ProcessingStep[OutboxEvent]] | None = None,
    additional_steps_after: list[ProcessingStep[OutboxEvent]] | None = None,
) -> EventBatchProcessor[OutboxEvent]:
    """Preset factory for common outbox publishing scenarios."""
    builder = EventProcessorBuilder[OutboxEvent](repo)

    if enable_otel:
        builder.add_step(OpenTelemetryStep(service_name=job_name))

    if enable_circuit_breaker:
        builder.add_step(
            CircuitBreakerStep(
                failure_threshold=circuit_breaker_failure_threshold,
                recovery_timeout_seconds=circuit_breaker_recovery_timeout,
            )
        )

    if dlq_storage:
        builder.add_step(DLQStep(dlq_storage))

    if additional_steps_before:
        for step in additional_steps_before:
            builder.add_step(step)

    builder.add_step(HandlerExecutionStep(publisher.publish, timeout=publish_timeout))

    if additional_steps_after:
        for step in additional_steps_after:
            builder.add_step(step)

    if metrics:
        builder.add_step(MetricsStep(metrics))
        builder.with_metrics(metrics)

    return builder.with_job_name(job_name).build()


def create_dispatching_processor(
    repo: InboxEventRepository,
    router: EventRouter,
    *,
    filter_sources: list[str] | None = None,
    skip_duplicate_siblings: bool = True,
    process_timeout: float = DEFAULT_PROCESS_TIMEOUT_SECONDS,
    dependencies: dict[str, object] | None = None,
    metrics: InboxMetrics | None = None,
    dlq_storage: DLQStorage[InboxEvent] | None = None,
    enable_otel: bool = False,
    enable_circuit_breaker: bool = False,
    circuit_breaker_failure_threshold: int = 5,
    circuit_breaker_recovery_timeout: int = 60,
    job_name: str = "dispatching_processor",
    additional_steps_before: list[ProcessingStep[InboxEvent]] | None = None,
    additional_steps_after: list[ProcessingStep[InboxEvent]] | None = None,
) -> EventBatchProcessor[InboxEvent]:
    """Preset factory for inbox processing with automated event routing."""

    async def dispatch_handler(event: InboxEvent, repo: InboxEventRepository) -> EventHandlerResult:
        return await router.dispatch(event, event.source, repo, **(dependencies or {}))

    builder = EventProcessorBuilder[InboxEvent](repo)

    if filter_sources:
        builder.with_fetch_strategy(FilteredFetchStrategy(sources=filter_sources))

    if enable_otel:
        builder.add_step(OpenTelemetryStep(service_name=job_name))

    if enable_circuit_breaker:
        builder.add_step(
            CircuitBreakerStep(
                failure_threshold=circuit_breaker_failure_threshold,
                recovery_timeout_seconds=circuit_breaker_recovery_timeout,
            )
        )

    if dlq_storage:
        builder.add_step(DLQStep(dlq_storage))

    if additional_steps_before:
        for step in additional_steps_before:
            builder.add_step(step)

    if skip_duplicate_siblings:
        builder.add_step(SiblingDeduplicationStep(enabled=True))

    builder.add_step(HandlerExecutionStep(dispatch_handler, timeout=process_timeout))  # type: ignore[arg-type]

    if additional_steps_after:
        for step in additional_steps_after:
            builder.add_step(step)

    if metrics:
        builder.add_step(MetricsStep(metrics))
        builder.with_metrics(metrics)

    return builder.with_job_name(job_name).build()
