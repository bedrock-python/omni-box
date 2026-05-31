from __future__ import annotations


class PipelineError(Exception):
    """Base exception for all pipeline errors."""


class PipelineStoppedError(PipelineError):
    """Error raised when pipeline execution is explicitly stopped."""
