"""Async relevance-filtering and deduplication — the downstream stages
triggered by `connector_finished`, run as background tasks so large,
high-noise sources (web scraping, YouTube) don't block ingestion workers.

Every source's PROMOTED assets go through relevance filtering, not just
web/YouTube — dataset-title-level matches (Kaggle) and video-sequence-name
matches (DAVIS) can both let visually-mismatched images through even
though the connector itself never raised an error (e.g. a Kaggle dataset
titled "Dog vs Cat Classification" matching a "dog" query, then yielding
its cat images too). Two independent signals vote on relevance: YOLO-World
(region/object-detection based, app/filtering/yolo_world_filter.py) and
SmolVLM (whole-image yes/no VQA, app/filtering/smolvlm_filter.py) — an
asset passes if EITHER signal says yes (OR, like dedup's OR-combine).
This is deliberately permissive rather than requiring both to agree:
SmolVLM-256M (a small model) is reliable on clear single-subject images
but unreliably says "no" on busy scenes with a small/background instance
of the query object — testing on real pipeline images confirmed this
(a genuine COCO dog image, correctly matched by COCO's own ground-truth
annotation, got a false "no" from SmolVLM alone on a cluttered scene).
Requiring both signals to agree (AND) would have meant SmolVLM's blind
spots could reject already-correct images from sources that never had
the mismatch problem in the first place; OR-combining means SmolVLM only
ever *adds* a second way to pass, never a second way to unfairly fail.

Deduplication runs across every source's assets for the job.
"""

import logging
import uuid

from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.session import session_scope
from app.domain.enums import AssetStatus
from app.filtering.dedup import HybridDeduplicator
from app.filtering.smolvlm_filter import VisualVerifier
from app.filtering.yolo_world_filter import RelevanceFilter, derive_prompt_classes
from app.services.progress_service import ProgressPublisher
from app.core.redis_client import get_redis
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.filtering_tasks.run_relevance_filter", bind=True)
def run_relevance_filter(self, job_id: str):
    job_uuid = uuid.UUID(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        asset_repo = AssetRepository(db)
        job = job_repo.get(job_uuid)
        if job is None:
            return

        candidates = asset_repo.list_for_job(job_uuid, status=AssetStatus.PROMOTED, limit=100_000)
        if not candidates:
            run_deduplication.delay(job_id)
            return

        prompt_classes = derive_prompt_classes(job.query)
        conf_threshold = job.config.get("yolo_world_conf_threshold", 0.15)
        relevance_filter = RelevanceFilter(conf_threshold=conf_threshold)
        visual_verifier = VisualVerifier()

        passed = rejected = 0
        for asset in candidates:
            if not asset.nas_path:
                continue
            try:
                score = relevance_filter.score(asset.nas_path, prompt_classes)
            except Exception as e:
                logger.warning(f"Relevance scoring failed for asset {asset.id}: {e}")
                score = 0.0

            try:
                vlm_pass = visual_verifier.verify(asset.nas_path, job.query)
            except Exception as e:
                logger.warning(f"SmolVLM verification failed for asset {asset.id}: {e}")
                vlm_pass = False

            if score >= conf_threshold or vlm_pass:
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
        pairs = [(str(a.id), a.phash, a.embedding) for a in assets]

        deduper = HybridDeduplicator(
            hamming_threshold=job.config.get("dedup_hamming_threshold", 5),
            cosine_threshold=job.config.get("dedup_cosine_threshold", 0.97),
        )
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
