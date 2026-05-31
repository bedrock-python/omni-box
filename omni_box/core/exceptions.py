"""Omni-box domain exceptions."""

from __future__ import annotations

from uuid import UUID


class OmniBoxError(Exception):
    """Base exception for omni-box-related errors."""


class StorageError(OmniBoxError):
    """Base exception for storage operations."""


class StorageConnectionError(StorageError):
    """Connection to storage failed."""


class StorageTimeoutError(StorageError):
    """Storage operation timed out."""


class StorageTransactionError(StorageError):
    """Transaction operation failed."""


class StorageIntegrityError(StorageError):
    """Data integrity constraint violation."""


class EventNotLockedError(OmniBoxError):
    """Raised when an operation requires a lock but the event is not locked."""

    def __init__(self, event_id: UUID) -> None:
        self.event_id = event_id
        super().__init__(f"Event {event_id} is not locked")


class EventLockedByAnotherWorkerError(OmniBoxError):
    """Raised when an operation is performed by a worker that doesn't own the lock."""

    def __init__(self, event_id: UUID, locked_by: str | None, worker_id: str) -> None:
        self.event_id = event_id
        self.locked_by = locked_by
        self.worker_id = worker_id
        super().__init__(f"Event {event_id} is locked by {locked_by}, but operation was attempted by {worker_id}")


class EventAlreadyLockedError(OmniBoxError):
    """Raised when an event is already locked by another worker."""

    def __init__(self, event_id: UUID, locked_by: str | None) -> None:
        self.event_id = event_id
        self.locked_by = locked_by
        super().__init__(f"Event {event_id} is already locked by {locked_by}")


class InvalidEventStateError(OmniBoxError):
    """Raised when an operation is performed on an event in an invalid state."""

    def __init__(
        self, event_id: UUID, current_status: str, expected_statuses: list[str], message: str | None = None
    ) -> None:
        self.event_id = event_id
        self.current_status = current_status
        self.expected_statuses = expected_statuses
        if message:
            super().__init__(f"Event {event_id}: {message}")
        else:
            super().__init__(
                f"Event {event_id} is in state '{current_status}', expected one of: {', '.join(expected_statuses)}"
            )


class EventConcurrentUpdateError(OmniBoxError):
    """Raised when a repository update affects fewer rows than expected.

    Indicates that some events might have been modified by another worker,
    unlocked by an admin, or already reached the target state.
    """

    def __init__(
        self,
        expected: int,
        actual: int,
        message: str | None = None,
        missing_ids: list[UUID] | None = None,
    ) -> None:
        self.expected = expected
        self.actual = actual
        self.missing_ids = missing_ids or []

        msg = message or f"Concurrent update detected: expected {expected} rows, but updated {actual}"
        if self.missing_ids:
            # Show up to first 10 missing IDs for diagnostics
            ids_str = ", ".join(str(i) for i in self.missing_ids[:10])
            if len(self.missing_ids) > 10:
                ids_str += ", ..."
            msg += f". Missing IDs: [{ids_str}]"

        super().__init__(msg)


class UnsupportedCapabilityError(OmniBoxError):
    """Raised when a required repository capability is not available."""

    def __init__(self, capability: str, repo_type: str) -> None:
        self.capability = capability
        self.repo_type = repo_type
        super().__init__(
            f"Repository {repo_type} does not support {capability}. "
            f"Use a repository that implements the required protocol or disable this feature."
        )


class InboxPersistError(OmniBoxError):
    """Raised when the inbox consumer fails to persist a consumed message.

    Typically means the underlying transaction was rolled back (DB outage,
    integrity violation, lock conflict). The broker offset is intentionally
    not committed so the message can be redelivered.
    """

    def __init__(self, message_id: str, cause: BaseException | None = None) -> None:
        self.message_id = message_id
        self.cause = cause
        super().__init__(f"Failed to persist inbox event for message {message_id!r}")
