"""Structured logging setup shared by the API process and Celery workers."""

import logging
import sys
from contextvars import ContextVar

_job_id_ctx: ContextVar[str] = ContextVar("job_id", default="-")
_job_source_task_id_ctx: ContextVar[str] = ContextVar("job_source_task_id", default="-")


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.job_id = _job_id_ctx.get()
        record.job_source_task_id = _job_source_task_id_ctx.get()
        return True


# Request paths that don't exist anywhere in this app but get hit
# persistently by LAN device-discovery tools / network scanners on
# deployers' networks (observed: a router/IoT companion app probing every
# host on the subnet for a common "device info" API) — legitimately 404,
# not a bug here, just log noise. Suppressed from uvicorn's access log
# rather than the app's own logger, since these never reach a route handler.
_JUNK_ACCESS_LOG_PATHS = ("/api/v1/device/info/unprotected",)


class SuppressJunkAccessLogs(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not any(path in message for path in _JUNK_ACCESS_LOG_PATHS)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  %(levelname)-8s  [job=%(job_id)s task=%(job_source_task_id)s]  "
            "%(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    handler.addFilter(ContextFilter())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]

    # uvicorn.access logs via its own handler (set up by uvicorn itself,
    # not via the root logger above), so it needs its own filter — a no-op
    # in the worker process, which never runs uvicorn.
    logging.getLogger("uvicorn.access").addFilter(SuppressJunkAccessLogs())

    # NAS client libraries log every protocol message at INFO (smbprotocol alone emits
    # ~20 lines per file operation), which would bury the job logs. Warnings and
    # errors still come through.
    for noisy in ("smbprotocol", "smbclient", "spnego", "paramiko"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def bind_job_context(job_id: str | None = None, job_source_task_id: str | None = None) -> None:
    if job_id is not None:
        _job_id_ctx.set(job_id)
    if job_source_task_id is not None:
        _job_source_task_id_ctx.set(job_source_task_id)


def clear_job_context() -> None:
    _job_id_ctx.set("-")
    _job_source_task_id_ctx.set("-")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
