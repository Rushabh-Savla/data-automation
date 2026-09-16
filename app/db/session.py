"""Single synchronous SQLAlchemy engine/session shared by FastAPI (via threadpool)
and Celery workers. See plan §1: two parallel data-access stacks (async for the API,
sync for Celery) would violate DRY for a low-QPS control-plane API — one sync
engine is simpler and sufficient.
"""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

_settings = get_settings()

# pool_size/max_overflow are set explicitly rather than left at SQLAlchemy's
# defaults (5 + 10): with a Celery prefork worker at --concurrency=N, every
# child gets its own pool, so the cluster-wide ceiling is N * (pool_size +
# max_overflow). At the previous default that is 12 * 15 = 180 connections
# against a stock Postgres max_connections of 100 — the pool would hand out
# connections the server then refuses. 5 + 5 keeps a 12-child worker plus the
# API under budget.
engine = create_engine(
    _settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
    pool_recycle=1800,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def dispose_engine_for_fork() -> None:
    """Drop every pooled connection inherited from the parent process.

    MUST be called in each Celery prefork child (see the worker_process_init
    handler in app/tasks/celery_app.py). `engine` is created at module import
    time, so the parent's pool — including any live TCP sockets to Postgres —
    is inherited by every forked child. Two processes then issue queries over
    the *same* socket and libpq fails with:

        psycopg.OperationalError: sending query failed:
        another command is already in progress

    which is exactly what killed a relevance_filter_batch, turned its chord
    into a ChordError, and stranded a job in `running` forever.

    `close=False` is required, not incidental: the default (close=True) would
    politely close those sockets, but they belong to the parent, which is
    still using them — closing them here breaks the parent and any sibling
    child. close=False abandons this process's references and lets the pool
    open fresh connections on next use, leaving the parent's untouched.
    """
    engine.dispose(close=False)


def get_db() -> Iterator[Session]:
    """FastAPI dependency: yields a request-scoped session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Context manager for Celery tasks / scripts: commits on success, rolls back on error."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
