"""Async relevance-filtering and deduplication — the downstream stages
triggered by `connector_finished`, run as background tasks so large,
high-noise sources (web scraping, YouTube) don't block ingestion workers.

Per the architecture doc, only web/YouTube assets go through YOLO-World
relevance filtering — Kaggle/Roboflow assets are already curated/labeled
datasets, not noisy scraped content, so they stay `promoted` untouched.
Deduplication runs across every source's assets for the job.
"""

import logging
import uuid

from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.session import session_scope
from app.domain.enums import AssetStatus, SourceType
from app.filtering.dedup import PerceptualDeduplicator
from app.filtering.yolo_world_filter import RelevanceFilter, derive_prompt_classes
from app.services.progress_service import ProgressPublisher
from app.core.redis_client import get_redis
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

_FILTERED_SOURCES = {SourceType.WEB, SourceType.YOUTUBE}


@celery_app.task(name="app.tasks.filtering_tasks.run_relevance_filter", bind=True)
def run_relevance_filter(self, job_id: str):
    job_uuid = uuid.UUID(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        asset_repo = AssetRepository(db)
        job = job_repo.get(job_uuid)
        if job is None:
            return

        candidates = [
            a for a in asset_repo.list_for_job(job_uuid, status=AssetStatus.PROMOTED, limit=100_000)
            if SourceType(a.source_type) in _FILTERED_SOURCES
        ]
        if not candidates:
            run_deduplication.delay(job_id)
            return

        prompt_classes = derive_prompt_classes(job.query)
        conf_threshold = job.config.get("yolo_world_conf_threshold", 0.15)
        relevance_filter = RelevanceFilter(conf_threshold=conf_threshold)

        passed = rejected = 0
        for asset in candidates:
            if not asset.nas_path:
                continue
            try:
                score = relevance_filter.score(asset.nas_path, prompt_classes)
            except Exception as e:
                logger.warning(f"Relevance scoring failed for asset {asset.id}: {e}")
                score = 0.0

            if score >= conf_threshold:
                asset_repo.set_status(asset.id, AssetStatus.RELEVANCE_PASSED, relevance_score=score)
                passed += 1
            else:
                asset_repo.set_status(asset.id, AssetStatus.RELEVANCE_REJECTED, relevance_score=score)
                # This asset was already counted in job.accepted_count at
                # promotion time (the target_count fast-path needs a running
                # count before filtering can possibly have run) — correct it
                # now that we know it's actually irrelevant.
                job_repo.increment_counts(job_uuid, accepted=-1, rejected=1)
                rejected += 1

        db.flush()
        logger.info(f"Relevance filter for job {job_id}: {passed} passed, {rejected} rejected")

    ProgressPublisher(get_redis()).publish(
        job_id, {"type": "relevance_filter_complete", "passed": passed, "rejected": rejected}
    )
    run_deduplication.delay(job_id)


@celery_app.task(name="app.tasks.filtering_tasks.run_deduplication", bind=True)
def run_deduplication(self, job_id: str):
    job_uuid = uuid.UUID(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        asset_repo = AssetRepository(db)
        job = job_repo.get(job_uuid)
        if job is None:
            return

        assets = asset_repo.list_for_dedup(job_uuid)
        pairs = [(str(a.id), a.phash) for a in assets]

        deduper = PerceptualDeduplicator(hamming_threshold=job.config.get("dedup_hamming_threshold", 5))
        groups = deduper.find_duplicate_groups(pairs)

        duplicate_count = 0
        for group in groups:
            for dup_id in group.duplicate_asset_ids:
                asset_repo.set_status(
                    uuid.UUID(dup_id), AssetStatus.DUPLICATE, is_duplicate_of=uuid.UUID(group.keep_asset_id)
                )
                duplicate_count += 1

        if duplicate_count:
            job_repo.increment_counts(job_uuid, accepted=-duplicate_count, duplicate=duplicate_count)

        db.flush()
        logger.info(f"Dedup for job {job_id}: {duplicate_count} duplicates found across {len(groups)} group(s)")

    ProgressPublisher(get_redis()).publish(job_id, {"type": "dedup_complete", "duplicate_count": duplicate_count})

    from app.tasks.orchestration_tasks import evaluate_job_completion

    evaluate_job_completion.delay(job_id)
