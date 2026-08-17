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
