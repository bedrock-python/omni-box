"""Optional Pydantic Settings integration for omni-box.

Available with the ``settings`` extra:

    pip install "omni-box[settings]"

Loading values from the environment:

The composite settings classes (:class:`BaseInboxSettings`,
:class:`BaseOutboxSettings`) read from environment variables with the prefixes
``OMNI_INBOX_`` and ``OMNI_OUTBOX_`` and use ``__`` as the nested-field
delimiter. Subclass them in your application to override the prefix or other
``model_config`` settings.

Example:

    .. code-block:: bash

        export OMNI_OUTBOX_PROCESSOR__BATCH_SIZE=200
        export OMNI_OUTBOX_OBSERVABILITY__ENABLE_OTEL=false
"""

from __future__ import annotations

from pydantic import BaseModel, Field

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError as _e:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "pydantic-settings is required for omni_box.contrib.settings. Install with: pip install 'omni-box[settings]'"
    ) from _e


class OmniBoxProcessorSettings(BaseModel):
    """Configuration for event processor."""

    batch_size: int = Field(default=50, ge=1, description="Batch size for processing")
    interval_seconds: int = Field(default=5, ge=1, description="Interval between runs in seconds")
    process_timeout_seconds: int = Field(default=30, ge=1, description="Timeout for single event processing")
    max_delivery_attempts: int = Field(default=5, ge=1, description="Maximum retry attempts for events")


class OmniBoxMaintenanceSettings(BaseModel):
    """Configuration for maintenance (cleanup)."""

    stale_lock_timeout_seconds: int = Field(default=300, ge=1, description="Timeout for stale locks")
    stale_lock_interval_seconds: int = Field(default=60, ge=1, description="Interval for stale lock release in seconds")
    retention_days: int | None = Field(
        default=None,
        ge=1,
        description="Retention period for processed events (for non-partitioned tables)",
    )
    cleanup_interval_seconds: int | None = Field(
        default=None,
        ge=1,
        description="Cleanup interval in seconds (for non-partitioned tables)",
    )


class OmniBoxResilienceSettings(BaseModel):
    """Configuration for resilience (Circuit Breaker)."""

    enable_circuit_breaker: bool = Field(default=True, description="Enable Circuit Breaker around message processing")
    failure_threshold: int = Field(default=5, ge=1, description="Failure threshold for opening Circuit Breaker")
    recovery_timeout_seconds: int = Field(default=60, ge=1, description="Recovery timeout for Circuit Breaker")


class OmniBoxObservabilitySettings(BaseModel):
    """Configuration for observability.

    ``enable_otel`` defaults to ``False`` because the OpenTelemetry pipeline
    requires the SDK and an exporter to be wired up by the host application.
    Leaving it disabled by default avoids silent no-op tracing and surprises
    when only ``opentelemetry-api`` is installed (no exporter).
    """

    enable_otel: bool = Field(default=False, description="Enable OpenTelemetry tracing")
    enable_metrics: bool = Field(default=True, description="Enable Prometheus metrics")


class BaseInboxSettings(BaseSettings):
    """Composite settings for Inbox processor.

    Override ``model_config`` in subclasses to change the prefix, env file, or
    other Pydantic Settings behaviour.
    """

    model_config = SettingsConfigDict(
        env_prefix="OMNI_INBOX_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    processor: OmniBoxProcessorSettings = Field(default_factory=OmniBoxProcessorSettings)
    maintenance: OmniBoxMaintenanceSettings = Field(default_factory=OmniBoxMaintenanceSettings)
    resilience: OmniBoxResilienceSettings = Field(default_factory=OmniBoxResilienceSettings)
    observability: OmniBoxObservabilitySettings = Field(default_factory=OmniBoxObservabilitySettings)
    skip_duplicate_siblings: bool = Field(default=True, description="Skip duplicates by sibling_id")


class BaseOutboxSettings(BaseSettings):
    """Composite settings for Outbox processor.

    Override ``model_config`` in subclasses to change the prefix, env file, or
    other Pydantic Settings behaviour.
    """

    model_config = SettingsConfigDict(
        env_prefix="OMNI_OUTBOX_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    processor: OmniBoxProcessorSettings = Field(default_factory=OmniBoxProcessorSettings)
    maintenance: OmniBoxMaintenanceSettings = Field(default_factory=OmniBoxMaintenanceSettings)
    resilience: OmniBoxResilienceSettings = Field(default_factory=OmniBoxResilienceSettings)
    observability: OmniBoxObservabilitySettings = Field(default_factory=OmniBoxObservabilitySettings)


__all__ = [
    "BaseInboxSettings",
    "BaseOutboxSettings",
    "OmniBoxMaintenanceSettings",
    "OmniBoxObservabilitySettings",
    "OmniBoxProcessorSettings",
    "OmniBoxResilienceSettings",
]
