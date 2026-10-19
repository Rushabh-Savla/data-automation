"""Async relevance-filtering and deduplication — the downstream stages
triggered by `connector_finished`, run as background tasks so large,
high-noise sources (web scraping, YouTube) don't block ingestion workers.

Every source's PROMOTED assets go through relevance filtering, not just
web/YouTube — dataset-title-level matches (Kaggle) and video-sequence-name
matches (DAVIS) can both let visually-mismatched images through even
though the connector itself never raised an error (e.g. a Kaggle dataset
titled "Dog vs Cat Classification" matching a "dog" query, then yielding
its cat images too).

Relevance is decided by one model looking at the whole frame:
app/filtering/vlm_filter.py, a VLM asked whether the query is clearly
visible.

It is deliberately NOT a cheap-first cascade any more. The previous design
scored a stored CLIP embedding and escalated only an uncertain middle band
to a VLM, which is the textbook arrangement and was wrong here: measured on
a real job's frames, CLIP's separation between actual weapons and briefcase
or wagon-wheel frames was +0.007. The band therefore never fired, every one
of that job's 500 accepted assets scored 90-100%, and the filter was passing
essentially everything while displaying a confident-looking percentage next
to images containing nothing of the sort. SigLIP, YOLO-World and a 256M VLM
were measured too; app/filtering/vlm_filter.py records the numbers. The
cause is structural -- a global image-text embedding encodes scene gist, in
which a briefcase and a pistol in a dark room are near-identical -- so no
prompt tuning recovers it.

The replacement model is both more accurate and faster than the 256M one it
replaces (0.18s vs 0.39s per image on an RTX A4000), so it runs on every
asset. That removes the accept/reject thresholds entirely: there is nothing
left to calibrate, and no band whose miscalibration can silently disable
filtering.

app/filtering/query_expansion.py still normalises the query into the
question ("weapon on movies scence" -> "a weapon"); the old gate passed the
raw typo'd string to the model verbatim.

Crucially there are THREE outcomes per asset, not two: accept, reject, and
error. An exception must never be recorded as a rejection, because a
rejection deletes the file from NAS. The previous code set the verdict to
False on any exception, so a model OOM or a transient read failure destroyed
the image as if the model had confidently judged it irrelevant. An undecided
asset is instead left PROMOTED, keeps its file, and is reconsidered later.

Deduplication runs across every source's assets for the job.

Relevance filtering is dispatched as a fan-out of small batches
(relevance_filter_batch), not one big loop in run_relevance_filter itself.
Each asset costs one VLM inference call (~0.2s on GPU
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
from app.domain.enums import JOB_TERMINAL_STATUSES, AssetStatus
from app.filtering.dedup import HybridDeduplicator
from app.services.fan_in import FanIn
from app.services.job_counters import reconcile
from app.services.progress_service import ProgressPublisher
from app.services.standardization_service import delete_asset_files
from app.storage import asset_files
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

    redis_client = get_redis()
    # Clear any guard left behind by a previous pass, otherwise a re-drive
    # would arm the barrier and then have its finalize suppressed. This has to
    # happen before the no-candidates early return below as well: a top-up
    # round that fetches nothing new (the supply is dry) lands there straight
    # after the previous round's finalize set the guard, and without the clear
    # its finalize was swallowed as a "duplicate". Nothing then ended the job
    # until the watchdog re-drove it filtering_stall_threshold_s (30 min) later.
    redis_client.delete(_FINALIZE_GUARD_KEY.format(job_id=job_id))

    if not candidate_ids:
        # Nothing left to classify — still go through finalize so a job whose
        # assets were all filtered by a previous (crashed) pass reaches dedup
        # and completion instead of stopping here.
        finalize_relevance_filter.delay(job_id)
        return

    batches = _chunk([str(asset_id) for asset_id in candidate_ids], batch_size)

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


# Outcome of the relevance decision for one asset. "error" is a distinct
# third state on purpose: it must never be conflated with a genuine "no",
# because a rejection deletes the file. Previously any exception -- a model
# OOM, a transient read failure -- was recorded as a rejection and the image
# was destroyed as if the model had confidently judged it irrelevant.
_ACCEPT = "accept"
_REJECT = "reject"
_ERROR = "error"


def _classify_batch(
    job_query: str,
    items: list[tuple[str, str]],
) -> list[tuple[str, str, float | None]]:
    """Decides relevance for (asset_id, nas_path) pairs.

    Returns (asset_id, outcome, score) where outcome is _ACCEPT / _REJECT /
    _ERROR. `score` is 1.0 / 0.0 / None -- the VLM gives a verdict, not a
    calibrated probability, and recording a fabricated confidence is what
    made the previous implementation so misleading: it showed "95.2%" next to
    images containing no weapon at all.

    A single model decides, on the whole frame. The previous cascade scored a
    CLIP embedding first and escalated only an uncertain middle band, which
    sounds cheaper but measured a separation of +0.007 between real weapons
    and briefcases -- so the band never fired and everything was accepted.
    See app/filtering/vlm_filter.py for the full measurement.

    Kept out of the task body so the expensive work cannot hold a database
    transaction: an hour of idle-in-transaction is what let the
    inherited-socket fork bug surface as a mid-batch OperationalError."""
    from app.filtering import query_expansion
    from app.filtering.vlm_filter import VisualVerifier

    if not items:
        return []

    # "car and person" is a request for either class, not for one thing that is
    # both -- see query_expansion.query_subjects. Most queries are a single subject.
    subjects = query_expansion.query_subjects(job_query)

    try:
        verifier = VisualVerifier()
    except Exception:
        # The gate itself is unusable. Every asset becomes _ERROR so it stays
        # PROMOTED for a later pass, rather than being deleted because a model
        # failed to load.
        logger.exception(f"Relevance model unavailable; deferring {len(items)} asset(s)")
        return [(asset_id, _ERROR, None) for asset_id, _ in items]

    verdicts = verifier.verify_batch_any([path for _, path in items], subjects)

    decisions: list[tuple[str, str, float | None]] = []
    for (asset_id, _path), verdict in zip(items, verdicts):
        if verdict is None:
            decisions.append((asset_id, _ERROR, None))
        elif verdict:
            decisions.append((asset_id, _ACCEPT, 1.0))
        else:
            decisions.append((asset_id, _REJECT, 0.0))

    accepted = sum(1 for _, o, _ in decisions if o == _ACCEPT)
    logger.info(
        f"Relevance ({subjects!r}): {accepted}/{len(decisions)} accepted"
    )
    return decisions


@celery_app.task(name="app.tasks.filtering_tasks.rerun_relevance_filter", bind=True)
def rerun_relevance_filter(self, job_id: str, include_rejected: bool = False):
    """Re-judges a job's assets with the current relevance gate.

    The escape hatch that did not exist when the gate was wrong. A job filtered
    by a miscalibrated model has an accepted set that cannot be trusted, and
    without this the only remedy was to re-collect everything from scratch.

    Resets RELEVANCE_PASSED assets back to PROMOTED and re-dispatches the normal
    filtering pass, so there is exactly one code path that decides relevance.

    `include_rejected` additionally reconsiders rejected assets, but only those
    whose file still exists: a rejection deletes the file from NAS, so most
    rejected rows have nothing left to look at. Those are skipped rather than
    being re-judged on a missing file (which would score them as errors and
    leave them stuck PROMOTED forever).
    """
    import os

    job_uuid = uuid.UUID(job_id)

    with session_scope() as db:
        asset_repo = AssetRepository(db)
        restored = 0

        for asset in asset_repo.list_for_job(job_uuid, status=AssetStatus.RELEVANCE_PASSED, limit=100_000):
            asset_repo.set_status(asset.id, AssetStatus.PROMOTED, relevance_score=None)
            restored += 1

        skipped_missing = 0
        if include_rejected:
            for asset in asset_repo.list_for_job(
                job_uuid, status=AssetStatus.RELEVANCE_REJECTED, limit=100_000
            ):
                if not asset.nas_path or not os.path.exists(str(asset.nas_path)):
                    skipped_missing += 1
                    continue
                asset_repo.set_status(asset.id, AssetStatus.PROMOTED, relevance_score=None)
                restored += 1

        db.flush()

    logger.info(
        f"Re-filtering job {job_id}: {restored} asset(s) reset to PROMOTED"
        + (f", {skipped_missing} rejected asset(s) skipped (file already deleted)" if include_rejected else "")
    )

    # Clear the guards so the pass actually dispatches rather than being
    # suppressed as a duplicate of the run that produced the bad result.
    redis_client = get_redis()
    redis_client.delete(_FINALIZE_GUARD_KEY.format(job_id=job_id))
    redis_client.delete(f"job:{job_id}:filtering_dispatched")

    run_relevance_filter.delay(job_id)


def _all_assets_judged(job_id: str) -> bool:
    """True when no asset of the job is still waiting for a relevance verdict.

    Never raises: it runs in the `finally` of relevance_filter_batch, which must always
    reach the FanIn decrement's continuation. If the database cannot be read it says no,
    which leaves the decision to the counter exactly as before.
    """
    try:
        with session_scope() as db:
            return AssetRepository(db).count_for_job(uuid.UUID(job_id), status=AssetStatus.PROMOTED) == 0
    except Exception:
        logger.exception(f"Could not check whether job {job_id} is fully judged")
        return False


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
            items: list[tuple[str, str]] = []
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
                try:
                    items.append((asset_id_str, asset_files.local_path_for_read(str(asset.nas_path))))
                except (FileNotFoundError, asset_files.NasUnreachable) as e:
                    # Image is on the NAS and could not be fetched right now: leave it
                    # PROMOTED for a later pass rather than judge it blind.
                    logger.warning(f"Skipping asset {asset_id_str}: {e}")

        # -- Inference phase: NO session held. -----------------------------
        decisions = _classify_batch(query, items)

        # -- Write-back phase: short transactions, chunked so a kill keeps
        #    whatever was already decided. ---------------------------------
        for chunk in _chunk(decisions, _WRITEBACK_CHUNK):
            chunk_passed: list[str] = []
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
                        chunk_passed.append(asset_id_str)
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

            # Accumulate passed IDs in Redis; the upload fires once ALL batches for
            # this job are done (see finalize_relevance_filter / schedule_offload_for_job).
            # Uploading mid-pass would hit the NAS in bursts from many concurrent
            # filter workers — deferring lets us send everything in one slow drip.
            if chunk_passed:
                _redis = get_redis()
                _redis.rpush(f"job:{job_id}:offload_pending", *chunk_passed)
                _redis.expire(f"job:{job_id}:offload_pending", 86400)  # 24 h safety TTL

        # A batch can still land PASSED verdicts after the job has already
        # gone terminal: trim_excess_assets (filtering_tasks.py) only ever
        # runs once, from plan_topup_round's final round, sized off whatever
        # was already RELEVANCE_PASSED at that moment -- an asset from the
        # same round that was still downloading/awaiting classification then
        # is invisible to it. Observed directly: a target_count=11 job
        # finished target_count_reached at 8 passed, then kept climbing to
        # 14 as the rest of that round's batches finalized, with nothing
        # left to re-trim a job that's already COMPLETED. Re-checking here,
        # every time a PASS is written, closes that gap regardless of which
        # timing path produced the straggler -- trim_excess_assets is a
        # cheap no-op (one query) whenever there's nothing to trim, so this
        # costs nothing on the far more common case where the job is still
        # actively running and this must NOT fire (trimming mid-round would
        # destroy the very margin the next round is trying to build, see
        # run_deduplication's docstring on why trim isn't called from there).
        if passed:
            with session_scope() as db:
                job = JobRepository(db).get(job_uuid)
                job_is_terminal = job is not None and job.status in JOB_TERMINAL_STATUSES
            if job_is_terminal:
                trim_excess_assets(job_id)

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
        last_batch_out = FanIn(get_redis()).finish(job_id, _FANIN_NAME) <= 0
        # Also hand on when nothing is left awaiting a verdict, even if the counter is not at
        # zero. A restart kills batches in flight; Celery re-delivers enough of them that every
        # asset does get judged, but the killed ones never decremented, so the counter never
        # reached zero and the job sat idle until the watchdog's timer ended it (a COCO job:
        # all 1507 images judged at 12:17, completed at 12:48). Finalize is guarded against
        # running twice, so an extra trigger is harmless.
        if last_batch_out or _all_assets_judged(job_id):
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

    # All relevance verdicts are now committed. Upload every RELEVANCE_PASSED asset
    # to NAS in one slow drip — deferred from filter_batch so the NAS is not hit by
    # concurrent burst writes from many parallel filter workers mid-pass.
    from app.tasks.nas_tasks import schedule_offload_for_job
    schedule_offload_for_job(job_id)

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

    # trim_excess_assets is deliberately NOT called here any more. Trimming
    # the surplus back down to target between rounds destroys the very margin
    # the next round is trying to build, and would make a job oscillate around
    # its target instead of converging on it. The planner runs the trim on the
    # final round, once no further rounds are coming.
    from app.tasks.orchestration_tasks import plan_topup_round

    plan_topup_round.delay(job_id)


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

        if job.accepted_count <= job.target_count:
            return

        # excess must be sized off len(kept), not job.accepted_count: that
        # counter also includes PROMOTED assets awaiting a filtering
        # decision (reconcile() treats "not yet filtered" as "provisionally
        # accepted" — see app/services/job_counters.py), which this function
        # can't trim since it only selects from RELEVANCE_PASSED. Sizing off
        # job.accepted_count let excess exceed len(kept) whenever a
        # meaningful number of assets were still undecided, and
        # ordered[:excess] on a shorter list silently returns the WHOLE
        # list — trimming every relevance-passed asset instead of just the
        # surplus, leaving only the undecided leftovers as the job's
        # "accepted" total once reconcile() ran again.
        kept = asset_repo.list_for_job(job_uuid, status=AssetStatus.RELEVANCE_PASSED, limit=100_000)
        excess = len(kept) - job.target_count
        if excess <= 0:
            return

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