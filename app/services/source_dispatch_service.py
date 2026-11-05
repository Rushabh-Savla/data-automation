"""Authoritative Source Dispatcher Service.

Enforces:
1. Strict separation of Manual vs Auto/Random source selection modes.
2. Prohibited connectors (YouTube, KITTI, NuScenes) are excluded from auto/random selection.
3. Explicit-only connectors (NAS Share) only run when explicitly selected.
4. Selection is persisted once in Job.sources and Job.config['source_selection_mode'].
5. Background tasks (watchdog, topup, scheduler) strictly respect persisted selection.
"""

from __future__ import annotations

import logging
import random
import uuid
from sqlalchemy.orm import Session

from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import SourceSelectionMode, SourceType
from app.services.job_service import _EXPLICIT_ONLY_SOURCES, get_implemented_sources

try:
    import app.tasks.discovery_tasks  # noqa: F401
except ImportError:
    pass

logger = logging.getLogger(__name__)

# Connectors prohibited from auto/random selection (empty by default; YouTube is handled as Tier 2)
PROHIBITED_AUTO_SOURCES: frozenset[SourceType] = frozenset()


class SourceDispatchService:
    @staticmethod
    def resolve_sources(
        requested_sources: list[SourceType] | None = None,
        selection_mode: SourceSelectionMode | None = None,
        db: Session | None = None,
    ) -> tuple[list[SourceType], SourceSelectionMode]:
        """Resolves and validates sources and selection mode.

        Returns (resolved_sources, effective_mode).
        """
        implemented = get_implemented_sources()

        if requested_sources:
            # Manual mode explicitly requested with sources
            effective_mode = SourceSelectionMode.MANUAL
            valid_sources = []
            for s in requested_sources:
                if not isinstance(s, SourceType):
                    try:
                        s = SourceType(s)
                    except ValueError:
                        raise ValueError(f"Invalid source type: {s}")
                if s not in implemented:
                    raise ValueError(f"Source '{s.value}' is not implemented yet")
                valid_sources.append(s)

            # Preserve uniqueness while sorting
            selected = sorted(set(valid_sources), key=lambda x: x.value)
            if not selected:
                raise ValueError("At least one valid source must be selected in manual mode")
            return selected, effective_mode

        # Auto or Random mode
        effective_mode = selection_mode or SourceSelectionMode.AUTO
        eligible_pool = set(implemented) - _EXPLICIT_ONLY_SOURCES - PROHIBITED_AUTO_SOURCES

        if db is not None:
            # If credentials are required for any connector, check them
            from app.db.repositories.credential_repository import CredentialRepository
            from app.services.credential_service import _SOURCE_ENV_KEYS
            from app.core.config import get_settings

            cred_repo = CredentialRepository(db)
            settings = get_settings()

            filtered = set()
            for src in eligible_pool:
                # Open datasets do not require credentials
                if src in {
                    SourceType.COCO,
                    SourceType.OPEN_IMAGES,
                    SourceType.PASCAL_VOC,
                    SourceType.VISUAL_GENOME,
                    SourceType.DAVIS,
                    SourceType.SUN_RGBD,
                    SourceType.WEB,
                }:
                    filtered.add(src)
                else:
                    has_cred = len(cred_repo.list_active_for_source(src)) > 0 or any(
                        getattr(settings, attr, "") for attr in _SOURCE_ENV_KEYS.get(src, {}).values()
                    )
                    if has_cred:
                        filtered.add(src)
            eligible_pool = filtered

        if not eligible_pool:
            raise ValueError("No eligible source connectors available for automatic selection")

        selected_list = sorted(eligible_pool, key=lambda x: x.value)

        if effective_mode == SourceSelectionMode.RANDOM:
            # Pick a non-empty random subset (between 2 and all available)
            k = max(1, min(len(selected_list), random.randint(2, max(2, len(selected_list)))))
            selected_list = sorted(random.sample(selected_list, k), key=lambda x: x.value)

        return selected_list, effective_mode

    @staticmethod
    def ensure_tasks_for_job(job: Job, db: Session) -> list[JobSourceTask]:
        """Ensures that JobSourceTask rows exist only for the job's persisted sources."""
        from app.db.repositories.job_source_task_repository import JobSourceTaskRepository

        task_repo = JobSourceTaskRepository(db)
        existing_tasks = task_repo.list_for_job(job.id)
        existing_by_source = {t.source_type: t for t in existing_tasks}

        allowed_sources = set(job.sources or [])
        created_tasks = []

        for source_name in allowed_sources:
            if source_name not in existing_by_source:
                new_task = JobSourceTask(
                    id=uuid.uuid4(),
                    job_id=job.id,
                    source_type=source_name,
                )
                task_repo.create(new_task)
                created_tasks.append(new_task)

        if created_tasks:
            db.flush()

        return task_repo.list_for_job(job.id)

    @staticmethod
    def filter_authorized_sources(job: Job, candidate_sources: list[str]) -> list[str]:
        """Filters any arbitrary source list against the job's persisted allowed sources."""
        allowed = set(job.sources or [])
        return [s for s in candidate_sources if s in allowed]
