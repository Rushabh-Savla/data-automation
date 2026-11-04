"""Typed exception hierarchy used across connectors, services, and Celery tasks.

Connectors raise these instead of letting arbitrary third-party exceptions
propagate, so Celery's retry policy (see app/tasks/celery_app.py) can react
correctly: transient failures are retried with backoff, fatal ones are not.
"""


class ConnectorError(Exception):
    """Base class for all source-connector failures."""


class TransientConnectorError(ConnectorError):
    """A retryable failure: timeout, rate limit, connection reset, 5xx, etc."""


class FatalConnectorError(ConnectorError):
    """A non-retryable failure: bad credentials, 404, malformed request."""


class RateLimitedError(TransientConnectorError):
    """The source explicitly rate-limited us; caller may include retry_after."""

    def __init__(self, message: str, retry_after_s: float | None = None):
        super().__init__(message)
        self.retry_after_s = retry_after_s


class JobControlSignal(Exception):
    """Base class for cooperative job-control interrupts raised inside connector
    loops. Connectors only know "should_stop() said stop" — they raise the
    generic JobStopped; the specific Pause/Cancel/TargetReached subclasses are
    available for callers that do know (and want to log/branch on) the reason.
    """


class JobStopped(JobControlSignal):
    """Generic stop signal raised by a connector when should_stop() returns
    True mid-loop. The task wrapper catches this as a clean, non-failure exit."""


class JobPaused(JobStopped):
    """Raised when should_stop() detects a pause flag; caller persists checkpoint."""


class JobCancelled(JobStopped):
    """Raised when should_stop() detects a cancel flag; caller persists checkpoint."""


class JobTargetReached(JobStopped):
    """Raised when should_stop() detects the job's target_count has been hit —
    stops issuing new work without marking the job cancelled; the actual
    completion_reason is computed separately by JobCompletionEvaluator."""


class StorageError(Exception):
    """Failure writing to / reading from the configured StorageBackend."""


class ZipSlipError(StorageError):
    """A zip archive member would extract outside its target directory."""
