"""Async relevance-filtering and deduplication — the downstream stages
triggered by `connector_finished`, run as background tasks so large,
high-noise sources (web scraping, YouTube) don't block ingestion workers.

Every source's PROMOTED assets go through relevance filtering, not just
web/YouTube — dataset-title-level matches (Kaggle) and video-sequence-name
matches (DAVIS) can both let visually-mismatched images through even
though the connector itself never raised an error (e.g. a Kaggle dataset
titled "Dog vs Cat Classification" matching a "dog" query, then yielding
its cat images too).

Filtering is a cheap-first cascade, not a single model.

Stage 1 is CLIP (app/filtering/clip_relevance.py), scored against the
normalised image embedding that StandardizationService already stored on
every asset at promotion time for deduplication. That makes it effectively
free -- one text-encoder pass per batch plus a matrix multiply, with no
image decode and no per-image inference -- and it yields a continuous
score rather than a bit, which is what `Asset.relevance_score` now holds.
Prompts come from app/filtering/query_expansion.py, which expands
superordinate categories into concrete nouns and scores the query's prompt
ensemble against a fixed background/contrast set.

Stage 2 is SmolVLM (app/filtering/smolvlm_filter.py), and it runs ONLY on
the uncertain middle band between the accept and reject thresholds. It is
asked a normalised question rather than the raw query: the original code
passed the job's text through verbatim, so a real job asked "Does this
image show weapon on movies scence?" -- typo and all -- and that answer was
the sole accept/reject decision for all 754 assets. YOLO-World is consulted
only to crop toward a candidate region for those escalated few; its score
never gates anything, because it structurally cannot represent compound
queries ("red cane", "people wearing helmet" would pass on a bare
"cane"/"people" match). On the job that motivated this rewrite it returned
0.0 for every asset even at a 0.05 threshold, so it was contributing a full
inference pass per image for no signal at all -- hence it now runs on a
small minority of assets and can be disabled per job.

Crucially there are THREE outcomes per asset, not two: accept, reject, and
error. An exception must never be recorded as a rejection, because a
rejection deletes the file from NAS. The previous code set vlm_pass=False
on any exception, so a model OOM or a transient read failure destroyed the
image as if the model had confidently judged it irrelevant. An undecided
asset is instead left PROMOTED, keeps its file, and is reconsidered by a
later pass.

Deduplication runs across every source's assets for the job.

Relevance filtering is dispatched as a fan-out of small batches
(relevance_filter_batch), not one big loop in run_relevance_filter itself.
Each asset costs one YOLO-World + one SmolVLM inference call (~5-25s
combined); at 50k+ assets, one task looping over all of them would take
days and would hit celery_task_time_limit_s (app/core/config.py) long
before finishing.

The join is a Redis counter (app/services/fan_in.py), NOT a Celery chord.
This used to be `chord(group)(finalize_relevance_filter.s(job_id))`, and
that is a job-killing construct: a chord's callback runs only if every
member succeeds, so one batch raising turned the whole thing into a
ChordError and finalize_relevance_filter never ran — which meant
run_deduplication, trim_excess_assets and evaluate_job_completion never
ran either, and the job sat in `running` forever with no path to a
terminal state. That is precisely how job 565f5036 stranded for 17 hours
after a transient `psycopg.OperationalError: another command is already
in progress`. Batching bounds the per-task runtime; the FanIn barrier is
what actually bounds the blast radius of a crash to one batch.

Two further properties keep that guarantee honest:
- relevance_filter_batch must NEVER raise. Its body is wrapped so that
  the FanIn decrement happens in a `finally:`, whether the batch
  succeeded, failed, or was killed by a soft time limit.
- finalize_relevance_filter derives its counts from the DB rather than
  from the batches' return values, so a batch that died contributes its
  real partial work instead of poisoning the total. That also makes
  finalize idempotent, which is what lets the watchdog re-drive a
  half-finished filtering pass (see orchestration_tasks.watchdog_scan).

Inference is deliberately performed with NO database session open. The
previous code held one session_scope() for an entire batch — up to ~3300s
of idle-in-transaction on CPU — which is the window the inherited-socket
fork bug rode in on (see app/db/session.dispose_engine_for_fork).
"""

import logging
import uuid

from celery.exceptions import SoftTimeLimitExceeded

from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.session import session_scope
from app.domain.enums import AssetStatus
from app.filtering.dedup import HybridDeduplicator
from app.filtering.smolvlm_filter import VisualVerifier
from app.filtering.yolo_world_filter import RelevanceFilter
from app.services.fan_in import FanIn
from app.services.job_counters import reconcile
from app.services.progress_service import ProgressPublisher
from app.services.standardization_service import delete_asset_files
from app.core.config import get_settings
from app.core.redis_client import get_redis
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

_DEFAULT_RELEVANCE_BATCH_SIZE = 50


def _chunk(items: list, batch_size: int) -> list[list]:
    batch_size = max(1, batch_size)
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


_FANIN_NAME = "relevance_filter"
_FINALIZE_GUARD_KEY = "job:{job_id}:filter_finalized"
# Window in which a duplicate finalize is suppressed. Generous because
# run_deduplication behind it can be slow on a large job, but finite so a
# watchdog re-drive minutes later is never blocked by a stale guard.
_FINALIZE_GUARD_TTL_S = 300

# How many assets to write back per transaction. Inference happens with no
# session open; results are flushed in small chunks so that a batch killed
# mid-flight (soft time limit, worker loss) keeps the work it already did
# instead of rolling the whole batch back.
_WRITEBACK_CHUNK = 10


@celery_app.task(name="app.tasks.filtering_tasks.run_relevance_filter", bind=True)
def run_relevance_filter(self, job_id: str):
    """Dispatcher: collects candidate asset ids, arms the FanIn barrier and
    fans them out as independent relevance_filter_batch tasks.

    Safe to call more than once for the same job: it only ever selects
    PROMOTED assets, so assets an earlier pass already classified are not
    reconsidered. That idempotency is what lets watchdog_scan re-drive a
    filtering pass that died half-way."""
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
        # Nothing left to classify — still go through finalize so a job whose
        # assets were all filtered by a previous (crashed) pass reaches dedup
        # and completion instead of stopping here.
        finalize_relevance_filter.delay(job_id)
        return

    batches = _chunk([str(asset_id) for asset_id in candidate_ids], batch_size)

    redis_client = get_redis()
    # Clear any guard left behind by a previous pass, otherwise a re-drive
    # would arm the barrier and then have its finalize suppressed.
    redis_client.delete(_FINALIZE_GUARD_KEY.format(job_id=job_id))
    FanIn(redis_client).start(job_id, _FANIN_NAME, len(batches))

    # Publish the denominator up front so the UI can render real filtering
    # progress ("340 / 754") instead of freezing for the whole pass.
    ProgressPublisher(redis_client).publish(
        job_id,
        {
            "type": "relevance_filter_started",
            "total": len(candidate_ids),
            "batches": len(batches),
        },
    )

    for batch in batches:
        relevance_filter_batch.delay(job_id, batch)


# Outcome of the relevance decision for one asset. "error" is a distinct
# third state on purpose: it must never be conflated with a genuine "no",
# because a rejection deletes the file. Previously any exception -- a model
# OOM, a transient read failure -- set vlm_pass=False and the image was
# destroyed as if the model had confidently judged it irrelevant.
_ACCEPT = "accept"
_REJECT = "reject"
_ERROR = "error"


def _classify_batch(
    job_query: str,
    conf_threshold: float,
    items: list[tuple[str, str, list[float] | None]],
    accept_at: float,
    reject_at: float,
    use_detector: bool = True,
) -> list[tuple[str, str, float | None]]:
    """Decides relevance for (asset_id, nas_path, embedding) triples.

    Returns (asset_id, outcome, score) where outcome is _ACCEPT / _REJECT /
    _ERROR and score is the CLIP relevance in [0, 1] (None only when it could
    not be computed at all).

    Two stages, cheap first:

    1. CLIP scores every asset from the embedding already stored on the row
       at promotion time. No image is decoded and no per-image model runs, so
       this costs one text-encoder pass for the whole batch. Assets that land
       above accept_at or below reject_at are decided here and never reach
       the VLM.
    2. Only the uncertain middle band goes to the VLM, which is the expensive
       part (measured at ~66s/image on CPU). YOLO-World is consulted solely
       to crop toward a candidate region for those few, not to gate anything.

    Kept out of the task body so the expensive work cannot hold a database
    transaction: an hour of idle-in-transaction is what let the
    inherited-socket fork bug surface as a mid-batch OperationalError."""
    from app.filtering import clip_relevance, query_expansion

    if not items:
        return []

    relevance_config_positive = query_expansion.positive_prompts(job_query)
    relevance_config_negative = query_expansion.negative_prompts()
    scores = clip_relevance.score_embeddings(
        [embedding for _, _, embedding in items],
        relevance_config_positive,
        relevance_config_negative,
    )

    decisions: list[tuple[str, str, float | None]] = []
    uncertain: list[tuple[int, str, str]] = []

    for index, ((asset_id_str, nas_path, _embedding), score) in enumerate(zip(items, scores)):
        band = clip_relevance.classify(score, accept_at, reject_at)
        if band == "accept":
            decisions.append((asset_id_str, _ACCEPT, score))
        elif band == "reject":
            decisions.append((asset_id_str, _REJECT, score))
        else:
            # Placeholder; replaced below once the VLM has adjudicated.
            decisions.append((asset_id_str, _ERROR, score))
            uncertain.append((index, asset_id_str, nas_path))

    if not uncertain:
        return decisions

    logger.info(
        f"Relevance: {len(items) - len(uncertain)} decided by CLIP, "
        f"{len(uncertain)} escalated to the VLM"
    )

    question = query_expansion.normalize_query(job_query)
    prompt_classes = query_expansion.derive_classes(job_query)

    relevance_filter = None
    if use_detector and prompt_classes:
        try:
            relevance_filter = RelevanceFilter(conf_threshold=conf_threshold)
        except Exception as e:
            logger.warning(f"YOLO-World unavailable, verifying uncropped: {e}")

    try:
        visual_verifier = VisualVerifier()
    except Exception as e:
        # The whole escalation path is unusable. Leave every uncertain asset
        # as _ERROR so it stays PROMOTED and is retried, rather than deleting
        # images because a model failed to load.
        logger.exception(f"SmolVLM unavailable, deferring {len(uncertain)} uncertain assets: {e}")
        return decisions

    for index, asset_id_str, nas_path in uncertain:
        box = None
        if relevance_filter is not None:
            try:
                box = relevance_filter.detect(nas_path, prompt_classes).box
            except Exception as e:
                logger.warning(f"Relevance detection failed for asset {asset_id_str}: {e}")

        try:
            vlm_pass = visual_verifier.verify(nas_path, question, box=box)
        except Exception as e:
            # Explicitly NOT a rejection. An exception means we do not know,
            # and "we do not know" must not destroy the file.
            logger.warning(f"SmolVLM verification failed for asset {asset_id_str}: {e}")
            continue

        decisions[index] = (asset_id_str, _ACCEPT if vlm_pass else _REJECT, decisions[index][2])

    return decisions


@celery_app.task(name="app.tasks.filtering_tasks.relevance_filter_batch", bind=True)
def relevance_filter_batch(self, job_id: str, asset_ids: list[str]) -> dict:
    """Runs the hybrid detect-then-verify check for one batch of assets.

    This task MUST NOT raise. Every exit path goes through the `finally`
    that decrements the FanIn barrier, because a batch that dies without
    decrementing would leave the barrier permanently short of zero and the
    job would never finalize — the exact failure this design replaced."""
    passed = rejected = errored = 0
    error: str | None = None

    try:
        # Parsed inside the try, not above it: every statement in this task
        # must sit on a path that reaches the `finally`, or the FanIn barrier
        # is left short of zero and the job never finalizes.
        job_uuid = uuid.UUID(job_id)

        # -- Load phase: short transaction, no inference. ------------------
        with session_scope() as db:
            job_repo = JobRepository(db)
            asset_repo = AssetRepository(db)
            job = job_repo.get(job_uuid)
            if job is None:
                return {"passed": 0, "rejected": 0}

            query = job.query
            # Use the class-level constant as the fallback so both the filter
            # object's own default and this config lookup always agree — there
            # is exactly one source of truth (RelevanceFilter.DEFAULT_CONF_THRESHOLD).
            conf_threshold = job.config.get(
                "yolo_world_conf_threshold", RelevanceFilter.DEFAULT_CONF_THRESHOLD
            )
            settings = get_settings()
            relevance_config = job.config.get("relevance") or {}
            accept_at = relevance_config.get("accept_threshold", settings.relevance_accept_threshold)
            reject_at = relevance_config.get("reject_threshold", settings.relevance_reject_threshold)
            use_detector = relevance_config.get("yolo_world_enabled", True)

            # The embedding is read here, with the row, rather than recomputed:
            # StandardizationService already stored a normalised CLIP vector
            # for every asset at promotion time for dedup's benefit, and
            # relevance scoring against it is a dot product.
            items: list[tuple[str, str, list[float] | None]] = []
            for asset_id_str in asset_ids:
                asset = asset_repo.get(uuid.UUID(asset_id_str))
                if asset is None or not asset.nas_path:
                    continue
                # Re-check the status rather than trusting the id list we were
                # handed. celery_app sets task_acks_late=True, so a batch that
                # was in flight when a worker restarted is redelivered and runs
                # again — and an asset this batch already rejected would then be
                # rejected a second time, decrementing accepted_count and
                # incrementing rejected_count twice for one image. That was
                # observed live: a job with 754 assets reported 797 rejections.
                # Filtering only ever acts on PROMOTED assets, so skipping
                # anything already classified makes the batch idempotent under
                # redelivery.
                if asset.status != AssetStatus.PROMOTED:
                    continue
                items.append((asset_id_str, str(asset.nas_path), asset.embedding))

        # -- Inference phase: NO session held. -----------------------------
        decisions = _classify_batch(
            query, conf_threshold, items, accept_at, reject_at, use_detector=use_detector
        )

        # -- Write-back phase: short transactions, chunked so a kill keeps
        #    whatever was already decided. ---------------------------------
        for chunk in _chunk(decisions, _WRITEBACK_CHUNK):
            with session_scope() as db:
                job_repo = JobRepository(db)
                asset_repo = AssetRepository(db)
                for asset_id_str, outcome, score in chunk:
                    asset_uuid = uuid.UUID(asset_id_str)
                    if outcome == _ERROR:
                        # We could not decide. Leave the asset PROMOTED so a
                        # later pass reconsiders it, keep its file, and touch
                        # no counters. Deleting here — which is what the old
                        # fail-closed path did — destroyed images whenever a
                        # model merely failed to load.
                        errored += 1
                        continue
                    if outcome == _ACCEPT:
                        asset_repo.set_status(
                            asset_uuid, AssetStatus.RELEVANCE_PASSED, relevance_score=score
                        )
                        passed += 1
                    else:
                        asset = asset_repo.get(asset_uuid)
                        asset_repo.set_status(
                            asset_uuid, AssetStatus.RELEVANCE_REJECTED, relevance_score=score
                        )
                        # Only the relevant/filtered set should ever sit on NAS —
                        # delete the file now that it's known to be irrelevant,
                        # rather than leaving it orphaned forever (see
                        # delete_asset_files' docstring).
                        if asset is not None:
                            delete_asset_files(asset)
                        # This asset was already counted in job.accepted_count at
                        # promotion time (the target_count fast-path needs a running
                        # count before filtering can possibly have run) — correct it
                        # now that we know it's actually irrelevant.
                        job_repo.increment_counts(job_uuid, accepted=-1, rejected=1)
                        rejected += 1

    except SoftTimeLimitExceeded:
        # Swallowed on purpose: re-raising would skip nothing (the finally
        # still runs) but would mark the task FAILED and lose the partial
        # counts we did commit. The work already written back stands, and
        # the assets left PROMOTED get picked up by the next pass.
        error = "soft_time_limit_exceeded"
        logger.warning(f"relevance_filter_batch hit the soft time limit for job {job_id}")
    except Exception as e:
        error = str(e)
        logger.exception(f"relevance_filter_batch failed for job {job_id}")
    finally:
        ProgressPublisher(get_redis()).publish(
            job_id,
            {"type": "relevance_filter_progress", "passed": passed, "rejected": rejected},
        )
        if FanIn(get_redis()).finish(job_id, _FANIN_NAME) <= 0:
            finalize_relevance_filter.delay(job_id)

    if errored:
        logger.warning(
            f"{errored} asset(s) in this batch could not be decided and were left "
            f"PROMOTED for a later pass (job {job_id})"
        )
    return {"passed": passed, "rejected": rejected, "errored": errored, "error": error}


@celery_app.task(name="app.tasks.filtering_tasks.finalize_relevance_filter", bind=True)
def finalize_relevance_filter(self, job_id: str):
    """Fan-in continuation: reports the pass/reject totals and hands off to dedup.

    Counts come from the DATABASE, not from the batches' return values. That
    is the property that makes this correct in the presence of a batch that
    died: a crashed batch contributes whatever it actually committed rather
    than poisoning the total or (as with a chord) preventing this from running
    at all. It also makes the task idempotent, so watchdog_scan can re-drive a
    half-finished filtering pass without corrupting anything."""
    redis_client = get_redis()

    # Idempotency guard, same NX idiom as check_and_advance_job's dispatch key.
    # A duplicate finalize is harmless (run_deduplication re-queries state), but
    # suppressing it avoids a redundant dedup pass over a large job.
    if not redis_client.set(
        _FINALIZE_GUARD_KEY.format(job_id=job_id), "1", nx=True, ex=_FINALIZE_GUARD_TTL_S
    ):
        logger.info(f"finalize_relevance_filter for job {job_id} already ran; skipping duplicate")
        return

    FanIn(redis_client).clear(job_id, _FANIN_NAME)

    job_uuid = uuid.UUID(job_id)
    with session_scope() as db:
        asset_repo = AssetRepository(db)
        passed = asset_repo.count_for_job(job_uuid, status=AssetStatus.RELEVANCE_PASSED)
        rejected = asset_repo.count_for_job(job_uuid, status=AssetStatus.RELEVANCE_REJECTED)
        # Bring both the job and per-source rollups back in line with the
        # assets now that the pass is done. The per-asset increments made
        # during filtering only touch the job row, so without this the
        # per-source table keeps showing pre-filter numbers that can never
        # sum to the header.
        reconcile(db, job_uuid)

    logger.info(f"Relevance filter for job {job_id}: {passed} passed, {rejected} rejected")

    ProgressPublisher(redis_client).publish(
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
                dup_asset = asset_repo.get(uuid.UUID(dup_id))
                asset_repo.set_status(
                    uuid.UUID(dup_id), AssetStatus.DUPLICATE, is_duplicate_of=uuid.UUID(group.keep_asset_id)
                )
                # Only the kept (non-duplicate) copy should remain on NAS.
                if dup_asset is not None:
                    delete_asset_files(dup_asset)
                duplicate_count += 1

        db.flush()
        # Derived rather than hand-decremented: the DUPLICATE status written
        # above is the fact, and reconcile reads it back for both rollups.
        reconcile(db, job_uuid)
        db.flush()
        logger.info(f"Dedup for job {job_id}: {duplicate_count} duplicates found across {len(groups)} group(s)")

    ProgressPublisher(get_redis()).publish(job_id, {"type": "dedup_complete", "duplicate_count": duplicate_count})

    trim_excess_assets(job_id)

    from app.tasks.orchestration_tasks import evaluate_job_completion

    evaluate_job_completion.delay(job_id)


def _select_trim_candidates(assets: list[tuple[str, float | None]], excess: int) -> list[str]:
    """Pure selection logic for trim_excess_assets: given (asset_id,
    relevance_score) pairs and how many need to be cut, returns the
    `excess` worst-scoring asset ids to remove — None scores sort first
    (treated as the least-confident matches, cut before any scored asset)."""
    if excess <= 0:
        return []
    ordered = sorted(assets, key=lambda pair: (pair[1] is not None, pair[1]))
    return [asset_id for asset_id, _ in ordered[:excess]]


def trim_excess_assets(job_id: str) -> None:
    """If a job over-fetched past target_count (see the over-fetch buffer
    in app/tasks/_helpers.py::build_on_progress, which deliberately fetches
    more than target_count up front so relevance/dedup attrition still
    lands on target), trims the worst-scoring surplus back down to exactly
    target_count — reusing RELEVANCE_REJECTED + delete_asset_files rather
    than introducing a new status or deletion path. Runs synchronously
    inside run_deduplication's flow (cheap: one query + a handful of
    status updates, not per-asset model inference) rather than as its own
    Celery task."""
    job_uuid = uuid.UUID(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        asset_repo = AssetRepository(db)
        job = job_repo.get(job_uuid)
        if job is None or job.target_count is None:
            return

        excess = job.accepted_count - job.target_count
        if excess <= 0:
            return

        kept = asset_repo.list_for_job(job_uuid, status=AssetStatus.RELEVANCE_PASSED, limit=100_000)
        trim_ids = _select_trim_candidates([(str(a.id), a.relevance_score) for a in kept], excess)
        if not trim_ids:
            return

        by_id = {str(a.id): a for a in kept}
        for asset_id in trim_ids:
            asset = by_id[asset_id]
            asset_repo.set_status(asset.id, AssetStatus.RELEVANCE_REJECTED)
            delete_asset_files(asset)

        db.flush()
        reconcile(db, job_uuid)
        db.flush()
        logger.info(f"Trimmed {len(trim_ids)} excess asset(s) for job {job_id} to match target_count")