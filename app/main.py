"""FastAPI application factory and router mounting."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1 import assets, credentials, exports, jobs, nas, sources, stats
from app.api.ws import job_progress_ws
from app.core.config import get_settings
from app.core.logging import configure_logging

configure_logging()

# Importing the task modules here (not just app.tasks.celery_app) triggers
# each connector's `register_source_discovery_task(...)` module-level side
# effect, so the API process's job_service._SOURCE_DISCOVERY_TASKS registry
# is populated even though this process never runs a Celery worker loop.
# Celery's `include=[...]` only does this automatically inside a worker.
import app.tasks.discovery_tasks  # noqa: E402,F401

app = FastAPI(
    title="Data Automation Platform API",
    version="0.1.0",
)

# Only relevant when frontend and backend are served from two different
# origins with no reverse proxy in front of both — the Vite dev proxy and
# the prod nginx container both make requests same-origin already, so this
# is a safety net, not the primary mechanism (see frontend plan §6).
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_allowed_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs.router, prefix="/api/v1")
app.include_router(assets.router, prefix="/api/v1")
app.include_router(credentials.router, prefix="/api/v1")
app.include_router(sources.router, prefix="/api/v1")
app.include_router(stats.router, prefix="/api/v1")
app.include_router(exports.router, prefix="/api/v1")
app.include_router(nas.router, prefix="/api/v1")
app.include_router(job_progress_ws.router)


@app.get("/")
def root():
    return {
        "service": "Data Automation Platform API",
        "status": "running",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.get("/healthz")
@app.get("/api/v1/health")
def health():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    db_ok = False
    redis_ok = False
    try:
        from app.db.session import session_scope
        from sqlalchemy import text

        with session_scope() as session:
            session.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False

    try:
        from app.core.redis_client import get_redis

        redis_client = get_redis()
        redis_ok = redis_client.ping() is True
    except Exception:
        redis_ok = False

    from fastapi.responses import JSONResponse

    status_code = 200 if (db_ok and redis_ok) else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "ready" if (db_ok and redis_ok) else "not_ready",
            "database": "ok" if db_ok else "unreachable",
            "redis": "ok" if redis_ok else "unreachable",
        },
    )
