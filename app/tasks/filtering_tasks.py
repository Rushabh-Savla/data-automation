"""Async relevance-filtering and deduplication — the downstream stages
triggered by `connector_finished`, run as background tasks so large,
high-noise sources (web scraping, YouTube) don't block ingestion workers.

Every source's PROMOTED assets go through relevance filtering, not just
web/YouTube — dataset-title-level matches (Kaggle) and video-sequence-name
matches (DAVIS) can both let visually-mismatched images through even
though the connector itself never raised an error (e.g. a Kaggle dataset
titled "Dog vs Cat Classification" matching a "dog" query, then yielding
its cat images too).

Filtering is a hybrid two-stage search, not a single model: YOLO-World
(app/filtering/yolo_world_filter.py) proposes a candidate region for the
query's keyword-derived classes — cheap, general, works for any query —
then SmolVLM (app/filtering/smolvlm_filter.py) answers the *full original
query* as a yes/no question against that region, and SmolVLM's answer is
the sole accept/reject decision. YOLO-World's own confidence score is
never used to gate the decision (only stored on the asset for
observability) because it structurally can't represent compound queries —
"red cane" or "people wearing helmet" would both pass on a bare "cane"/
"people" class match regardless of color or whether a helmet was actually
present, which is exactly the bug this replaced. There's no fixed keyword
list that can enumerate every way a query can be compound (color,
negation, counting, spatial relations...), so nothing here classifies the
query — every query is treated the same way: detect a candidate region,
then let a real language-grounded model judge the whole sentence against
it.

Deduplication runs across every source's assets for the job.

Relevance filtering is dispatched as a Celery chord of small batches
(relevance_filter_batch), not one big loop in run_relevance_filter itself
— mirrors the same chord pattern discovery_tasks.py already uses for
per-item downloads (chord(download_group)(on_source_finished.s(...))).
Each asset costs one YOLO-World + one SmolVLM inference call (~5-25s
combined); at 50k+ assets, one task looping over all of them would take
days and would hit celery_task_time_limit_s (app/core/config.py) long
before finishing — and since session_scope() only commits once at the
end of its `with` block, a mid-loop kill would lose every asset's status
change made so far, with nothing left to ever trigger run_deduplication.
Batching bounds both the per-task runtime and the blast radius of a
crash to one batch instead of the whole job.
"""

import logging
import uuid

from celery import chord

from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.session import session_scope
from app.domain.enums import AssetStatus
from app.filtering.dedup import HybridDeduplicator
from app.filtering.smolvlm_filter import VisualVerifier
from app.filtering.yolo_world_filter import DetectionResult, RelevanceFilter, derive_prompt_classes
from app.services.progress_service import ProgressPublisher
from app.core.redis_client import get_redis
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

_DEFAULT_RELEVANCE_BATCH_SIZE = 50


def _chunk(items: list, batch_size: int) -> list[list]:
    batch_size = max(1, batch_size)
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


@celery_app.task(name="app.tasks.filtering_tasks.run_relevance_filter", bind=True)
def run_relevance_filter(self, job_id: str):
    """Dispatcher: collects candidate asset ids and fans them out as a
    chord of relevance_filter_batch tasks, with finalize_relevance_filter
    as the completion callback."""
    job_uuid = uuid.UUID(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        asset_repo = AssetRepository(db)
        job = job_repo.get(job_uuid)
        if job is None:
            return

        candidate_ids = asset_repo.list_ids_for_job(job_uuid, status=AssetStatus.PROMOTED, limit=100_000)
        batch_size = job.config.get("relevance_filter_batch_size", _DEFAULT_RELEVANCE_BATCH_SIZE)

    if not candidate_ids:
        run_deduplication.delay(job_id)
        return

    batches = _chunk([str(asset_id) for asset_id in candidate_ids], batch_size)
    group = [relevance_filter_batch.s(job_id, batch) for batch in batches]
    chord(group)(finalize_relevance_filter.s(job_id))


@celery_app.task(name="app.tasks.filtering_tasks.relevance_filter_batch", bind=True)
def relevance_filter_batch(self, job_id: str, asset_ids: list[str]) -> dict:
    """Runs the hybrid detect-then-verify check for one batch of assets,
    committing its own results independently of every other batch — see
    the module docstring for why this can't be one big loop at scale."""
    job_uuid = uuid.UUID(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        asset_repo = AssetRepository(db)
        job = job_repo.get(job_uuid)
        if job is None:
            return {"passed": 0, "rejected": 0}

        prompt_classes = derive_prompt_classes(job.query)
        conf_threshold = job.config.get("yolo_world_conf_threshold", 0.15)
        query = job.query
        relevance_filter = RelevanceFilter(conf_threshold=conf_threshold)
        visual_verifier = VisualVerifier()

        passed = rejected = 0
        for asset_id_str in asset_ids:
            asset = asset_repo.get(uuid.UUID(asset_id_str))
            if asset is None or not asset.nas_path:
                continue
            try:
                detection = relevance_filter.detect(asset.nas_path, prompt_classes)
            except Exception as e:
                logger.warning(f"Relevance detection failed for asset {asset.id}: {e}")
                detection = DetectionResult(score=0.0, box=None)

            try:
                vlm_pass = visual_verifier.verify(asset.nas_path, query, box=detection.box)
            except Exception as e:
                logger.warning(f"SmolVLM verification failed for asset {asset.id}: {e}")
                vlm_pass = False

            score = detection.score
            if vlm_pass:
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

    ProgressPublisher(get_redis()).publish(
        job_id, {"type": "relevance_filter_progress", "passed": passed, "rejected": rejected}
    )
    return {"passed": passed, "rejected": rejected}


@celery_app.task(name="app.tasks.filtering_tasks.finalize_relevance_filter", bind=True)
def finalize_relevance_filter(self, batch_results: list[dict], job_id: str):
    """Chord callback: sums every batch's counts, publishes the same
    relevance_filter_complete event the pre-chunking implementation did
    (unchanged name/shape), then hands off to dedup exactly as before."""
    passed = sum(r.get("passed", 0) for r in batch_results if r)
    rejected = sum(r.get("rejected", 0) for r in batch_results if r)
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
