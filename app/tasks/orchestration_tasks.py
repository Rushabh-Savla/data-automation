"""Orchestration: the chord callback that marks a source complete and, once
every source for a job is terminal, evaluates whether the whole job is done
(directly for now; Phase 4 inserts the relevance-filter/dedup chain here
before completion evaluation). Also the Beat-scheduled watchdog that resumes
stalled tasks and kicks off scheduled/deadline-based jobs.
"""

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, func, delete

from app.core.config import calculate_initial_candidates, get_settings, resolve_overfetch_buffer
from app.core.logging import bind_job_context, clear_job_context
from app.core.redis_client import get_redis
from app.db.models.candidate_window import CandidateWindow
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.repositories.image_batch_repository import ImageBatchRepository
from app.db.session import session_scope
from app.domain.completion import JobCompletionEvaluator, JobSnapshot
from app.domain.enums import (
    ASSET_ACCEPTED_STATUSES,
    AssetStatus,
    JOB_SOURCE_TASK_TERMINAL_STATUSES,
    JobSourceTaskStatus,
    JobStatus,
    SourceType,
)
from app.domain.recovery import is_orphaned, orphan_cutoff
from app.domain.state_machine import JobStateMachine
from app.domain.topup import SourceState, TopupPlan, TopupPlanner, TopupSnapshot, filtering_incomplete
from app.tasks._helpers import buffered_target
from app.services.cancellation_service import CancellationService
from app.services.progress_service import ProgressPublisher
from app.services.worker_boot import last_worker_boot
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.orchestration_tasks.on_source_finished", bind=True)
def on_source_finished(self, results, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_job_context(job_id, job_source_task_id)

    cancellation = CancellationService(get_redis())
    window_waiting = False

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            clear_job_context()
            return

        had_errors = any(isinstance(r, dict) and r.get("error") for r in (results or []))
        had_stopped = any(isinstance(r, dict) and r.get("stopped") for r in (results or []))
        had_window_full = any(
            isinstance(r, dict) and r.get("window_full")
            for r in (results or [])
        )

        # Record which dispatched items (keyed the same way download tasks
        # key their own checkpoint — see _run_item_download) reached a
        # terminal outcome in this chord round, vs. which were stopped
        # mid-flight (paused/cancelled). Only sources that write
        # "_pending_items" (currently Kaggle — see discover_kaggle) ever
        # consult this; for every other source it's a harmless no-op extra
        # key in the checkpoint dict. This is what lets a resume redispatch
        # only the datasets that are actually still outstanding instead of
        # re-running discovery and rebuilding the whole chord (see
        # JobService.resume / _resume_task).
        done_items = {
            r["item"]
            for r in (results or [])
            if isinstance(r, dict)
            and r.get("item")
            and not r.get("stopped")
            and not r.get("window_full")
            and not r.get("deferred")
        }
        if done_items:
            existing_completed = set(task.checkpoint.get("_completed_items", []))
            task_repo.heartbeat(
                task_uuid,
                checkpoint={
                    **(task.checkpoint or {}),
                    "_completed_items": sorted(existing_completed | done_items),
                },
            )
            db.flush()

        if had_stopped and cancellation.is_paused(job_id) and not cancellation.is_cancelled(job_id):
            # Paused mid-flight, not actually done: leave the task in whatever
            # non-terminal status the download task left it in (DOWNLOADING)
            # so JobService.resume() sees it as eligible to re-dispatch, and
            # skip the completion check below entirely — nothing about the
            # job's overall completion changed, it's still just paused.
            db.flush()
            clear_job_context()
            return

        pending_items = (task.checkpoint or {}).get("_pending_items") or {}
        completed_items = set((task.checkpoint or {}).get("_completed_items", []))
        has_remaining = bool(pending_items and not set(pending_items.keys()).issubset(completed_items))

        if had_stopped and cancellation.is_cancelled(job_id):
            final_status = JobSourceTaskStatus.CANCELLED
        elif had_window_full:
            final_status = JobSourceTaskStatus.WINDOW_WAITING
            window_waiting = True
        elif has_remaining:
            final_status = JobSourceTaskStatus.PARTIALLY_PROCESSED
            window_waiting = True
        elif had_errors:
            final_status = JobSourceTaskStatus.PARTIALLY_PROCESSED if has_remaining else JobSourceTaskStatus.COMPLETED_PARTIAL
        else:
            final_status = JobSourceTaskStatus.COMPLETED

        if task.status != JobSourceTaskStatus.CANCELLED:
            if final_status == JobSourceTaskStatus.COMPLETED or (final_status == JobSourceTaskStatus.WINDOW_WAITING and not had_errors):
                task_repo.set_status(task_uuid, final_status, clear_error=True)
            else:
                task_repo.set_status(task_uuid, final_status)
        db.flush()

        # Clear per-source dispatch lock so subsequent batches can be scheduled
        try:
            get_redis().delete(f"job:{job_id}:source:{task.source_type}:dispatch_lock")
        except Exception:
            pass

    # This source's raw intermediates (downloaded videos, extracted archives)
    # have served their purpose: anything worth keeping was promoted to NAS and
    # recorded in the DB during download. Releasing the scratch space here is
    # what cleanup_local was written for -- it simply had no caller anywhere,
    # which is why 3.3 GB of dead workdirs accumulated against 307 MB of
    # delivered assets. Dispatched rather than inlined so a slow rmtree over
    # tens of thousands of files does not hold up the job's progression.
    from app.tasks.cleanup_tasks import cleanup_local

    if not window_waiting:
        cleanup_local.delay(job_source_task_id)
    else:
        logger.info(
            f"[WINDOW] Source task {job_source_task_id} is waiting for "
            "the active candidate window; preserving its work directory."
        )

    schedule_eligible_source_tasks.delay(job_id)
    check_and_advance_job(job_id)
    clear_job_context()


def check_and_advance_job(job_id: str) -> None:
    """Checks whether every JobSourceTask for the job is now terminal, and if
    so, kicks off the downstream filtering/completion pipeline.

    Called from two places: on_source_finished (the chord callback, for
    sources whose discovery found at least one item) AND directly from each
    discover_* task's zero-items early return. That second call site matters
    a lot: a source that legitimately finds nothing — no credentials
    configured, a query with no matches, a blocked/rate-limited search — never
    creates a chord, so on_source_finished would never fire for it. If that
    source happens to be the last one to go terminal, nothing would ever
    re-check "is the whole job done" again and the job would sit in `running`
    forever despite every source having already finished. This function is
    the single place that check happens, reachable from both paths.

    Because it's reachable from up to one call per source, two (or more)
    sources going terminal within the same instant is a real race: multiple
    concurrent invocations can each read all_terminal_for_job() as True
    before any of them has committed a side effect that would change that
    answer, and each would dispatch run_relevance_filter — which processes
    the same PROMOTED assets more than once and double-counts
    accepted/rejected on the job (observed in production: accepted_count
    went negative from a job whose web+kaggle sources finished seconds
    apart). The Redis NX guard below makes the dispatch atomic across
    concurrent callers so only the one that actually sees the job through
    the transition proceeds; it expires quickly so a later resume of the
    same job can legitimately re-run the pipeline for newly promoted assets.

    Also where fast-path escalation happens: a small-target-count auto-
    sourced job (see JobService.create()) only creates/dispatches a
    JobSourceTask for web scraping up front, stashing the remaining sources
    in job.config["_fast_path_remaining_sources"]. Once web (currently the
    only tracked task) goes terminal, all_terminal_for_job is trivially
    True — if the target wasn't actually reached from web alone, this is
    where the remaining sources get created and dispatched instead of
    proceeding straight to filtering.
    """
    job_uuid = uuid.UUID(job_id)
    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        job_repo = JobRepository(db)
        asset_repo = AssetRepository(db)

        job = job_repo.get(job_uuid)
        if job is None or job.status not in (JobStatus.RUNNING, JobStatus.DISCOVERING):
            return

        job_target_count = job.target_count
        job_accepted_count = job.accepted_count

        all_terminal = task_repo.all_terminal_for_job(job_uuid)
        tasks = task_repo.list_for_job(job_uuid)
        all_waiting_or_terminal = bool(tasks) and all(
            t.status in JOB_SOURCE_TASK_TERMINAL_STATUSES
            or t.status in (JobSourceTaskStatus.WINDOW_WAITING, JobSourceTaskStatus.STALLED)
            for t in tasks
        )

        # Check if an eligible candidate window or batch is ready for processing
        cancellation = CancellationService(get_redis())

        from app.db.models.candidate_window import CandidateWindow
        from sqlalchemy import case
        active_window = db.execute(
            select(CandidateWindow)
            .where(
                CandidateWindow.job_id == job_uuid,
                CandidateWindow.status.in_(["FULL", "ACQUIRING", "OPEN"]),
            )
            .order_by(
                case((CandidateWindow.status == "FULL", 0), else_=1),
                CandidateWindow.window_index.desc(),
            )
            .limit(1)
        ).scalar_one_or_none()

        window_is_full = active_window is not None and active_window.status == "FULL"
        window_limit = active_window.candidate_limit if active_window else None

        candidate_window = job.config.get("candidate_window", get_settings().download_candidate_window)
        from app.db.repositories.image_batch_repository import ImageBatchRepository
        active_b = ImageBatchRepository(db).get_active_batch(job_uuid)
        active_batch_target = (
            active_b.target_image_count
            if (active_b and active_b.target_image_count)
            else min(candidate_window, job_target_count or candidate_window)
        )
        from app.core.config import calculate_initial_candidates
        window_buffered_target = calculate_initial_candidates(active_batch_target, job_config=job.config)

        job_accepted_count = int(job.accepted_count or 0)

        promoted_count = asset_repo.count_for_job(job_uuid, status=AssetStatus.PROMOTED)
        relevance_passed_count = asset_repo.count_for_job(job_uuid, status=AssetStatus.RELEVANCE_PASSED)
        candidate_count = promoted_count + relevance_passed_count
        effective_accepted = job_accepted_count

        window_ready = (
            cancellation.is_target_reached(str(job_id))
            or (window_is_full and candidate_count >= window_buffered_target)
            or (all_waiting_or_terminal and candidate_count > 0)
            or (window_limit is not None and candidate_count >= window_limit and candidate_count > 0)
            or effective_accepted >= window_buffered_target
            or candidate_count >= window_buffered_target
        )

        target_reached = job_target_count is not None and effective_accepted >= job_target_count
        if target_reached and promoted_count == 0 and relevance_passed_count == 0:
            logger.info(f"Job {job_id}: Target count reached ({job_accepted_count}/{job_target_count}) and no candidates pending. Evaluating completion.")
            from app.tasks.orchestration_tasks import evaluate_job_completion
            evaluate_job_completion.delay(job_id)
            return

        if not all_terminal and not window_ready:
            if all_waiting_or_terminal and candidate_count == 0:
                logger.info(
                    f"Job {job_id}: all sources waiting/terminal and 0 candidates remaining. "
                    "Triggering process_active_batch to evaluate progress or replenish shortfall."
                )
                from app.tasks.batch_orchestration import process_active_batch
                process_active_batch.delay(str(job_uuid))
            return

        pending_tiers = job.config.get("_orchestration_pending_tiers") or []
        remaining_sources = job.config.get("_fast_path_remaining_sources") or []

        from app.domain.enums import is_source_task_exhausted
        tier_ready = all_terminal or (bool(tasks) and all(is_source_task_exhausted(t) for t in tasks))
        if (pending_tiers or remaining_sources) and not target_reached and tier_ready:
            current_tier = job.config.get("_orchestration_tier", 1)
            next_tier_sources = []
            new_pending_tiers = []

            if pending_tiers:
                next_tier_sources = pending_tiers[0]
                new_pending_tiers = pending_tiers[1:]
                next_tier_num = current_tier + 1
            else:
                next_tier_sources = remaining_sources
                next_tier_num = current_tier + 1

            redis_client = get_redis()
            escalate_key = f"job:{job_id}:tier_escalated_{next_tier_num}"
            if not redis_client.set(escalate_key, "1", nx=True, ex=86400):
                # Another concurrent caller already claimed this tier escalation
                return

            new_config = dict(job.config)
            new_config["_orchestration_tier"] = next_tier_num
            new_config["_orchestration_pending_tiers"] = new_pending_tiers
            new_config["_fast_path_remaining_sources"] = [s for tier in new_pending_tiers for s in tier]
            job_repo.set_config(job_uuid, new_config)

            new_tasks = []
            for source_value in next_tier_sources:
                task = JobSourceTask(id=uuid.uuid4(), job_id=job_uuid, source_type=source_value)
                task_repo.create(task)
                new_tasks.append((source_value, task.id))
            db.flush()

        else:
            new_tasks = None
            next_tier_num = 1
            next_tier_sources = []

    if new_tasks:
        from app.services.job_service import _SOURCE_DISCOVERY_TASKS, JobService

        tier_names = {1: "Tier 1 (Web)", 2: "Tier 2 (Videos: YouTube)", 3: "Tier 3 (Datasets)"}
        tier_label = tier_names.get(next_tier_num, f"Tier {next_tier_num}")
        logger.info(
            f"[RESOURCE ORCHESTRATION] Escalating Job {job_id} to {tier_label} "
            f"because target not yet met ({job_accepted_count}/{job_target_count}). Dispatching: {next_tier_sources}"
        )
        for source_value, task_id in new_tasks:
            task_path = _SOURCE_DISCOVERY_TASKS.get(SourceType(source_value))
            if task_path is not None:
                JobService._dispatch(task_path, job_id, str(task_id))
        return

    redis_client = get_redis()
    dispatch_key = f"job:{job_id}:filtering_dispatched"
    if not redis_client.set(dispatch_key, "1", nx=True, ex=300):
        return

    ProgressPublisher(redis_client).publish(job_id, {"type": "connector_finished"})
    # Downstream pipeline: relevance filter -> dedup -> completion evaluation.
    # When batch processing is active, run_relevance_filter processes the current
    # bounded window, followed by process_active_batch to verify accepted items
    # and plan replacement windows.
    from app.tasks.filtering_tasks import run_relevance_filter

    run_relevance_filter.delay(job_id)


# How long to wait before asking the relevance filter to try again, and how many
# times: 30 x 2min is an hour of a model that will not load. After that the
# watchdog's own re-drive (every filtering_stall_threshold_s) keeps the job alive.
_REFILTER_DELAY_S = 120
_REFILTER_MAX_ATTEMPTS = 30


def _defer_until_filtered(job_id: str, undecided: int, total: int) -> None:
    """Retries the relevance filter shortly instead of planning from a filter that failed."""
    redis_client = get_redis()
    key = f"job:{job_id}:refilter_attempts"
    attempts = int(redis_client.incr(key))
    redis_client.expire(key, 6 * 3600)
    if attempts > _REFILTER_MAX_ATTEMPTS:
        logger.error(
            f"Top-up for job {job_id}: {undecided}/{total} asset(s) still undecided after "
            f"{_REFILTER_MAX_ATTEMPTS} filtering retries; leaving it to the watchdog"
        )
        return
    logger.warning(
        f"Top-up for job {job_id}: {undecided}/{total} asset(s) could not be judged by the "
        f"relevance filter (model unavailable?); retrying it in {_REFILTER_DELAY_S}s "
        f"(attempt {attempts}/{_REFILTER_MAX_ATTEMPTS}) instead of planning another round"
    )
    from app.tasks.filtering_tasks import run_relevance_filter

    run_relevance_filter.apply_async(args=[job_id], countdown=_REFILTER_DELAY_S)


@celery_app.task(name="app.tasks.orchestration_tasks.plan_topup_round", bind=True)
def plan_topup_round(self, job_id: str):
    """Decides whether to fetch more data, or to finish the job.

    Sits between deduplication and completion evaluation. Before this existed
    the chain went straight from dedup to evaluate_job_completion, which can
    only move a job to a terminal state -- so a job whose filtering removed
    more than the fixed 1.5x over-fetch buffer anticipated simply ended short
    and was reported `completed_partial`. On the job that motivated this work,
    754 downloaded assets became 107 survivors against a target of 500, and
    nothing ever went back for more.

    The sizing decision itself is in app/domain/topup.py (pure, unit tested);
    this task is only the orchestration around it.
    """
    job_uuid = uuid.UUID(job_id)
    bind_job_context(job_id)
    settings = get_settings()

    # Resolved inside the transaction, acted on after it commits: dispatching
    # Celery tasks from inside an open transaction races them against the
    # commit, and they can read state that has not landed yet.
    plan = None
    redispatch: list[tuple[str, str]] = []

    try:
        with session_scope() as db:
            job_repo = JobRepository(db)
            task_repo = JobSourceTaskRepository(db)
            asset_repo = AssetRepository(db)
            job = job_repo.get(job_uuid)
            if job is None or job.status not in (JobStatus.DISCOVERING, JobStatus.RUNNING):
                return

            cancellation = CancellationService(get_redis())
            if cancellation.is_cancelled(job_id) or cancellation.is_paused(job_id):
                return

            topup_state = (job.config or {}).get("_topup") or {}
            round_number = int(topup_state.get("round", 1))
            previous_accepted = topup_state.get("accepted_after_last_round")
            previous_promoted = topup_state.get("promoted_after_last_round")

            # Post-filter survivors, deliberately not job.accepted_count: that
            # column is incremented optimistically at promotion time and only
            # corrected once filtering has run. Sizing a round off it would
            # repeat the original mistake of reacting to a pre-filter number.
            accepted = asset_repo.count_for_job(job_uuid, status=ASSET_ACCEPTED_STATUSES)
            total_assets = asset_repo.count_for_job(job_uuid)
            # Assets the filter could not decide are still PROMOTED. Counting them as
            # rejections read a filter outage as a 2% yield (see filtering_incomplete).
            undecided = asset_repo.count_for_job(job_uuid, status=AssetStatus.PROMOTED)
            if filtering_incomplete(undecided, total_assets):
                # Nothing was written in this transaction, so dispatching from inside it
                # cannot race a commit. Deliberately does not fall through to the trim
                # and completion below: the job is not done, its filtering is.
                _defer_until_filtered(job_id, undecided, total_assets)
                return
            get_redis().delete(f"job:{job_id}:refilter_attempts")
            promoted_total = total_assets - undecided

            tasks = task_repo.list_for_job(job_uuid)

            def _is_source_exhausted(task) -> bool:
                cp = task.checkpoint or {}
                disc_exhausted = bool(cp.get("discovery_exhausted"))
                pending = cp.get("_pending_items") or {}
                completed = set(cp.get("_completed_items") or [])
                unconsumed = [k for k in pending.keys() if k not in completed]
                return disc_exhausted and len(unconsumed) == 0

            sources = [
                SourceState(
                    source_type=str(getattr(t.source_type, "value", t.source_type)),
                    accepted=t.accepted_count,
                    exhausted=_is_source_exhausted(t),
                )
                for t in tasks
            ]

            gained_last_round = (
                accepted - previous_accepted if previous_accepted is not None else None
            )
            consecutive_no_progress = int((job.config or {}).get("_topup", {}).get("consecutive_no_progress", 0))
            if gained_last_round is not None:
                if gained_last_round <= 0:
                    consecutive_no_progress += 1
                else:
                    consecutive_no_progress = 0

            plan = TopupPlanner.plan(
                TopupSnapshot(
                    target_count=job.target_count,
                    accepted_post_filter=accepted,
                    promoted_total=promoted_total,
                    round_number=round_number,
                    max_rounds=job.config.get("topup_max_rounds", settings.topup_max_rounds) if job and job.config else settings.topup_max_rounds,
                    sources=sources,
                    gained_last_round=gained_last_round,
                    promoted_last_round=(
                        promoted_total - previous_promoted if previous_promoted is not None else None
                    ),
                    consecutive_no_progress=consecutive_no_progress,
                )
            )

            if plan is None:
                shortfall = (job.target_count or 0) - accepted
                if shortfall > 0:
                    unexhausted_sources = [s for s in sources if not s.exhausted]
                    if not unexhausted_sources:
                        unexhausted_sources = sources
                    logger.info(
                        f"Top-up for job {job_id}: round {round_number}, short by {shortfall}. "
                        f"Continuing infinite replenishment iterations across {len(unexhausted_sources)} source(s)..."
                    )
                    import math
                    per_source = math.ceil(max(25, shortfall) / max(1, len(unexhausted_sources)))
                    quota = {s.source_type: per_source for s in unexhausted_sources}
                    plan = TopupPlan(
                        round_number=round_number + 1,
                        shortfall=shortfall,
                        observed_yield=0.05,
                        quota=quota,
                        max_rounds=None,
                    )

            if plan is None:
                logger.info(
                    f"Top-up for job {job_id}: target fulfilled at {accepted}/{job.target_count} "
                    f"after round {round_number}"
                )
                config = dict(job.config or {})
                config["_topup_stopped"] = False
                job_repo.set_config(job_uuid, config)
            else:
                logger.info(
                    f"Top-up for job {job_id}: round {plan.round_number}, short by "
                    f"{plan.shortfall}, observed yield {plan.observed_yield:.2f}, "
                    f"fetching ~{plan.total_quota} more"
                )

                config = dict(job.config or {})
                config["_topup_stopped"] = False
                config["_topup"] = {
                    "round": plan.round_number,
                    "quota": plan.quota,
                    "accepted_after_last_round": accepted,
                    "promoted_after_last_round": promoted_total,
                    "consecutive_no_progress": consecutive_no_progress,
                    # Raw (pre-filter) accepted_count as of right now, before
                    # this round's downloads start — the fixed baseline
                    # build_on_progress anchors this round's target to. Not
                    # the same number as accepted_after_last_round above,
                    # which is post-filter; the target-reached check in
                    # build_on_progress compares against job.accepted_count,
                    # which is the raw/pre-filter counter, so it needs a
                    # baseline in that same unit.
                    "raw_accepted_at_round_start": job.accepted_count,
                }
                job_repo.set_config(job_uuid, config)

                # Clearing these is not optional. should_stop() consults the
                # job-wide target flag and each source's cap flag, both set when
                # the previous round hit its ceiling. Leaving them set means
                # every connector in the new round stops on its first iteration
                # and the round fetches precisely nothing.
                cancellation.clear_target_reached(job_id)
                for task in tasks:
                    cancellation.clear_source_cap_reached(job_id, str(task.id))

                # Filtering is dispatched once per round; the previous round's
                # guard would otherwise suppress this round's pass.
                get_redis().delete(f"job:{job_id}:filtering_dispatched")

                exhausted = {s.source_type for s in sources if s.exhausted}
                allowed_sources = set(job.sources or [])
                for task in tasks:
                    source_value = str(getattr(task.source_type, "value", task.source_type))
                    if source_value in exhausted or source_value not in allowed_sources:
                        continue
                    task_repo.set_status(task.id, JobSourceTaskStatus.PENDING)
                    task_repo.heartbeat(task.id)
                    redispatch.append((source_value, str(task.id), task.checkpoint or {}))

        # -- After the transaction has committed. --------------------------
        if plan is None:
            # No further rounds, so any overshoot is final: trim back to
            # exactly target_count and evaluate completion. Trimming is
            # deliberately deferred to here rather than run after every round,
            # because cutting the surplus between rounds would destroy the
            # margin the next round is trying to build.
            from app.tasks.filtering_tasks import trim_excess_assets

            trim_excess_assets(job_id)

            # Finalize any incomplete active batch so its accepted assets are manifested and sent to NAS
            try:
                from app.tasks.batch_orchestration import BatchOrchestrator
                from app.domain.enums import BatchStatus
                with session_scope() as batch_db:
                    orchestrator = BatchOrchestrator(batch_db, job_uuid)
                    active = orchestrator.batch_repo.get_active_batch(job_uuid)
                    if active and active.status not in (
                        BatchStatus.COMPLETED,
                        BatchStatus.READY_FOR_UPLOAD,
                        BatchStatus.UPLOADING,
                        BatchStatus.NAS_UPLOADING,
                        BatchStatus.VERIFYING,
                    ):
                        if (active.accepted_image_count or 0) > 0:
                            manifest_path = orchestrator.create_batch_manifest(active.id)
                            if manifest_path is not None:
                                from app.storage.nas_writer import enqueue_nas_batch
                                from app.tasks.nas_tasks import process_nas_upload_queue
                                enqueue_nas_batch(active.id, job_uuid, manifest_path)
                                process_nas_upload_queue.delay()
                                logger.info(
                                    f"Top-up planner: active batch {active.id} finalized with {active.accepted_image_count} images for NAS upload."
                                )
                        else:
                            orchestrator.batch_repo.set_status(active.id, BatchStatus.PARTIAL)
                            logger.info(
                                f"Top-up planner: empty active batch {active.id} marked PARTIAL without NAS upload."
                            )
                        task_repo_batch = JobSourceTaskRepository(batch_db)
                        for t in task_repo_batch.list_for_job(job_uuid):
                            if t.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                                cp = t.checkpoint or {}
                                pending = cp.get("_pending_items") or {}
                                completed = set(cp.get("_completed_items", []))
                                has_remaining = bool(pending and not set(pending.keys()).issubset(completed))
                                if has_remaining:
                                    task_repo_batch.set_status(t.id, JobSourceTaskStatus.PARTIALLY_PROCESSED)
                                else:
                                    task_repo_batch.set_status(t.id, JobSourceTaskStatus.COMPLETED_PARTIAL)
                        batch_db.commit()
            except Exception as e:
                logger.warning(f"Error finalizing active batch in plan_topup_round: {e}")

            evaluate_job_completion.delay(job_id)
            return

        ProgressPublisher(get_redis()).publish(
            job_id,
            {
                "type": "topup_round",
                "round": plan.round_number,
                "max_rounds": plan.max_rounds,
                "shortfall": plan.shortfall,
                "observed_yield": round(plan.observed_yield, 3),
            },
        )

        for source_value, task_id, cp in redispatch:
            _resume_task(job_id, task_id, source_value, cp)
    finally:
        clear_job_context()


@celery_app.task(name="app.tasks.orchestration_tasks.evaluate_job_completion", bind=True)
def evaluate_job_completion(self, job_id: str):
    from app.domain.enums import (
        JOB_SOURCE_TASK_TERMINAL_STATUSES,
        JOB_TERMINAL_STATUSES,
        JobSourceTaskStatus,
        JobStatus,
        JobCompletionReason,
    )
    job_uuid = uuid.UUID(job_id)
    bind_job_context(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        task_repo = JobSourceTaskRepository(db)
        job = job_repo.get(job_uuid)
        if job is None or job.status not in (JobStatus.DISCOVERING, JobStatus.RUNNING, JobStatus.CANCELLING):
            clear_job_context()
            return

        cancellation = CancellationService(get_redis())
        # job.status == CANCELLING (Postgres, durable) is treated as
        # sufficient on its own, not just cancellation.is_cancelled() (Redis,
        # ephemeral): a Redis restart/eviction between JobService.cancel()
        # setting the flag and this task running loses the flag while the DB
        # still faithfully remembers CANCELLING, and relying on the flag
        # alone stranded such jobs forever — every re-drive fell through to
        # the normal complete/fail/complete_partial path below and hit
        # IllegalTransitionError, since none of those events are legal from
        # CANCELLING.
        if cancellation.is_cancelled(job_id) or job.status == JobStatus.CANCELLING:
            if job.status != JobStatus.CANCELLING:
                # Redis's cancel flag is set synchronously, but the DB
                # transaction that moved job.status to CANCELLING may not
                # have committed yet by the time this task's own session
                # reads it (see JobService.cancel()/jobs.py::_apply_control).
                # finish_cancel only accepts CANCELLING as its source state —
                # retry briefly instead of crashing on IllegalTransitionError,
                # which would otherwise strand the job in CANCELLING forever.
                clear_job_context()
                raise self.retry(countdown=2, max_retries=5)
            job_repo.set_status(
                job_uuid,
                JobStateMachine.transition(job.status, "finish_cancel"),
                completion_reason=None,
            )
            from app.domain.enums import JobCompletionReason, JOB_SOURCE_TASK_TERMINAL_STATUSES, JobSourceTaskStatus
            from app.tasks.cleanup_tasks import cleanup_local

            job_repo.set_status(job_uuid, JobStatus.CANCELLED, completion_reason=JobCompletionReason.CANCELLED)
            for t in task_repo.list_for_job(job_uuid):
                if t.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                    task_repo.set_status(t.id, JobSourceTaskStatus.CANCELLED)
                    cleanup_local.delay(str(t.id))

            staging_dir = get_settings().work_root / "staging" / f"job_{job_id}"
            if staging_dir.exists():
                import shutil
                shutil.rmtree(staging_dir, ignore_errors=True)

            cancellation.clear_active_tasks(job_id)
            ProgressPublisher(get_redis()).publish(job_id, {"type": "job_status", "status": "cancelled"})
            from app.tasks.cleanup_tasks import cleanup_shared_cache
            cleanup_shared_cache.delay()
            clear_job_context()
            return

        from app.services.job_counters import reconcile
        reconcile(db, job_uuid)
        db.commit()
        db.refresh(job)

        # Always enforce strict target consistency before making the completion snapshot:
        # If accepted assets exceed target_count, trim the lowest confidence excess assets
        # down to exactly target_count.
        if job.target_count is not None and job.accepted_count > job.target_count:
            from app.tasks.filtering_tasks import trim_excess_assets
            trim_excess_assets(job_id)
            db.refresh(job)

        all_sources_terminal = task_repo.all_terminal_for_job(job_uuid)
        if not all_sources_terminal:
            # Check if all batches are completed/partial, meaning batch orchestration is finished
            from app.db.repositories.image_batch_repository import ImageBatchRepository
            from app.domain.enums import BatchStatus
            from app.db.models.candidate_window import CandidateWindow
            from app.db.models.asset import Asset
            batch_repo = ImageBatchRepository(db)
            batches = batch_repo.list_for_job(job_uuid)

            # Prune empty ghost batches with 0 assets that were created spuriously
            cleaned_batches = []
            for b in batches:
                if (b.accepted_image_count or 0) == 0 and b.status not in (BatchStatus.COMPLETED, BatchStatus.PARTIAL):
                    asset_count = db.execute(
                        select(func.count(Asset.id)).where(Asset.batch_id == b.id)
                    ).scalar_one()
                    if asset_count == 0:
                        db.execute(delete(CandidateWindow).where(CandidateWindow.batch_id == b.id))
                        db.delete(b)
                        continue
                cleaned_batches.append(b)
            db.flush()
            batches = cleaned_batches

            all_batches_done = not batches or all(
                b.status in (BatchStatus.COMPLETED, BatchStatus.PARTIAL) for b in batches
            )
            tasks = task_repo.list_for_job(job_uuid)
            all_waiting_or_terminal = bool(tasks) and all(
                t.status in JOB_SOURCE_TASK_TERMINAL_STATUSES
                or t.status in (JobSourceTaskStatus.WINDOW_WAITING, JobSourceTaskStatus.STALLED, JobSourceTaskStatus.COMPLETED_PARTIAL)
                for t in tasks
            )
            if all_batches_done and all_waiting_or_terminal:
                # If target is not met, NEVER declare sources exhausted or terminate if eligible sources remain.
                # Continuous sequential iterations: trigger the sequential scheduler to continue or advance round.
                if job.target_count is not None and job.accepted_count < job.target_count:
                    from app.domain.source_priority import evaluate_sequential_execution_plan
                    from app.domain.enums import is_source_task_exhausted
                    plan = evaluate_sequential_execution_plan(job, tasks)
                    if not plan.all_sources_complete or any(not is_source_task_exhausted(t) for t in tasks):
                        logger.info(
                            f"[EVALUATE] Job {job_id} target not met ({job.accepted_count}/{job.target_count}); "
                            f"sources remain eligible (active: {plan.active_source}). Triggering sequential scheduler."
                        )
                        schedule_eligible_source_tasks.delay(job_id)
                        clear_job_context()
                        return

                for t in tasks:
                    if t.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                        cp = t.checkpoint or {}
                        pending = cp.get("_pending_items") or {}
                        completed = set(cp.get("_completed_items", []))
                        has_remaining = bool(pending and not set(pending.keys()).issubset(completed))
                        if has_remaining:
                            t_status = JobSourceTaskStatus.PARTIALLY_PROCESSED
                        elif t.accepted_count and t.discovered_count and t.accepted_count >= t.discovered_count:
                            t_status = JobSourceTaskStatus.COMPLETED
                        else:
                            t_status = JobSourceTaskStatus.SOURCE_EXHAUSTED
                        task_repo.set_status(t.id, t_status)
                db.flush()
                all_sources_terminal = all(
                    t.status in JOB_SOURCE_TASK_TERMINAL_STATUSES for t in tasks
                )

        snapshot = JobSnapshot(
            target_count=job.target_count,
            deadline_at=job.deadline_at,
            accepted_count=job.accepted_count,
            all_sources_terminal=all_sources_terminal,
        )
        reason = JobCompletionEvaluator.should_stop(snapshot)
        if reason is None:
            clear_job_context()
            return

        success = JobCompletionEvaluator.is_success(reason, job.accepted_count, job.target_count)
        event = "complete" if success else ("fail" if job.accepted_count == 0 else "complete_partial")
        new_status = JobStateMachine.transition(job.status, event)

        if success:
            try:
                cancellation.mark_target_reached(job_id)
            except Exception:
                pass

        # Protect in-flight and active NAS uploads for both COMPLETED and COMPLETED_PARTIAL jobs
        if new_status in (JobStatus.COMPLETED, JobStatus.COMPLETED_PARTIAL):
            from app.services.nas_config_service import is_storage_active
            from app.db.models.image_batch import NasTransferBatch, ImageBatch
            from app.db.models.asset import Asset
            from app.domain.enums import TransferStatus, BatchStatus
            if is_storage_active():
                from app.storage import asset_files

                # Under Flexible Batch Delivery: ensure any finalized non-empty batches
                # are manifested and enqueued so no valid images are stranded in local staging
                from app.tasks.batch_orchestration import BatchOrchestrator
                unmanifested = db.execute(
                    select(ImageBatch).where(
                        ImageBatch.job_id == job_uuid,
                        ImageBatch.status.in_([BatchStatus.PROCESSING, BatchStatus.PARTIAL, BatchStatus.DISCOVERING]),
                        ImageBatch.accepted_image_count > 0,
                    )
                ).scalars().all()
                if unmanifested:
                    orchestrator = BatchOrchestrator(db, job_uuid)
                    for b in unmanifested:
                        m_path = orchestrator.create_batch_manifest(b.id)
                        if m_path:
                            from app.storage.nas_writer import enqueue_nas_batch
                            enqueue_nas_batch(b.id, job_uuid, m_path, db=db)

                # Wait only for assets in batches that are actively in the upload pipeline
                from sqlalchemy import or_
                has_in_flight = db.execute(
                    select(Asset.id)
                    .join(ImageBatch, Asset.batch_id == ImageBatch.id, isouter=True)
                    .where(
                        Asset.job_id == job_uuid,
                        Asset.status.in_([
                            AssetStatus.READY_FOR_UPLOAD,
                            AssetStatus.UPLOADING,
                        ]),
                        Asset.nas_path.is_not(None),
                        ~Asset.nas_path.like(f"{asset_files.NAS_SCHEME}%"),
                        or_(
                            ImageBatch.id.is_(None),
                            ImageBatch.status.in_([
                                BatchStatus.READY_FOR_UPLOAD,
                                BatchStatus.UPLOADING,
                                BatchStatus.NAS_UPLOADING,
                                BatchStatus.VERIFYING,
                            ]),
                        ),
                    ).limit(1)
                ).scalar_one_or_none() is not None

                has_active_transfers = db.execute(
                    select(NasTransferBatch.id).where(
                        NasTransferBatch.job_id == job_uuid,
                        NasTransferBatch.transfer_status.in_([
                            TransferStatus.PENDING,
                            TransferStatus.UPLOADING,
                            TransferStatus.VERIFIED,
                        ]),
                    ).limit(1)
                ).scalar_one_or_none() is not None

                pending_assets = db.execute(
                    select(Asset).where(
                        Asset.job_id == job_uuid,
                        Asset.status.in_([
                            AssetStatus.RELEVANCE_PASSED,
                            AssetStatus.READY_FOR_UPLOAD,
                            AssetStatus.UPLOADING,
                        ]),
                    )
                ).scalars().all()
                has_pending_pipeline = False
                for pa in pending_assets:
                    if pa.nas_path and (asset_files.is_remote(pa.nas_path) or Path(pa.nas_path).exists()):
                        has_pending_pipeline = True
                    else:
                        pa.status = AssetStatus.FAILED
                if not has_pending_pipeline and pending_assets:
                    db.flush()
                    reconcile(db, job_uuid)
                    db.flush()

                completed_nas = db.execute(
                    select(func.count(Asset.id)).where(
                        Asset.job_id == job_uuid,
                        Asset.status.in_([AssetStatus.COMPLETED, AssetStatus.NAS_VERIFIED]),
                    )
                ).scalar_one()

                is_short_of_target = (
                    new_status == JobStatus.COMPLETED
                    and job.target_count is not None
                    and completed_nas < job.target_count
                )

                if has_in_flight or has_active_transfers or (is_short_of_target and has_pending_pipeline):
                    retries = getattr(getattr(self, "request", None), "retries", 0)
                    from app.tasks.nas_tasks import reconcile_nas_transfers
                    try:
                        reconcile_nas_transfers()
                    except Exception as e:
                        logger.warning(f"Reconcile NAS transfers warning: {e}")
                    if retries < 120:
                        countdown = 5 if retries < 30 else (10 if retries < 60 else 15)
                        logger.info(
                            f"[EVALUATE] Job {job_id} reached completion ({new_status.value}) but has active uploads or pending delivery "
                            f"(in_flight={has_in_flight}, active_transfers={has_active_transfers}, delivered={completed_nas}/{job.target_count}). "
                            f"Retrying completion evaluation in {countdown}s (retry {retries}/120)..."
                        )
                        clear_job_context()
                        raise self.retry(countdown=countdown, max_retries=120)
                    else:
                        logger.error(
                            f"[EVALUATE] Job {job_id} reached retry timeout but active transfers still running "
                            f"(in_flight={has_in_flight}, active_transfers={has_active_transfers}). "
                            "Aborting completion transition to prevent data loss."
                        )
                        clear_job_context()
                        raise self.retry(countdown=30, max_retries=240)

                # If all pipeline stages are drained, but delivered NAS count is STILL below target_count:
                # Do NOT mark COMPLETED with TARGET_COUNT_REACHED!
                if is_short_of_target:
                    logger.warning(
                        f"[EVALUATE] Job {job_id} pipeline drained but actual NAS delivered count "
                        f"({completed_nas}/{job.target_count}) is short of target. Re-triggering sources to complete requested count."
                    )
                    from app.tasks.orchestration_tasks import schedule_eligible_source_tasks
                    schedule_eligible_source_tasks.delay(job_id)
                    clear_job_context()
                    return
        job_repo.set_status(job_uuid, new_status, completion_reason=reason)

        from app.tasks.cleanup_tasks import cleanup_local
        all_tasks = task_repo.list_for_job(job_uuid)
        target_task_status = (
            JobSourceTaskStatus.COMPLETED
            if new_status == JobStatus.COMPLETED
            else JobSourceTaskStatus.COMPLETED_PARTIAL
        )
        for t in all_tasks:
            if t.status not in (JobSourceTaskStatus.FAILED, JobSourceTaskStatus.CANCELLED):
                task_repo.set_status(t.id, target_task_status, clear_error=True)
            elif new_status == JobStatus.COMPLETED:
                task_repo.set_status(t.id, t.status, clear_error=True)
            if t.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                cleanup_local.delay(str(t.id))

        db.commit()

        if new_status == JobStatus.FAILED:
            staging_dir = get_settings().work_root / "staging" / f"job_{job_id}"
            if staging_dir.exists():
                import shutil
                shutil.rmtree(staging_dir, ignore_errors=True)

        # Actively revoke any lingering discovery or download tasks still executing for this job
        current_id = {self.request.id} if getattr(self, "request", None) and getattr(self.request, "id", None) else set()
        cancellation.revoke_active_tasks(job_id, exclude_task_ids=current_id)
        if new_status in (JobStatus.COMPLETED, JobStatus.COMPLETED_PARTIAL):
            cancellation.mark_target_reached(job_id)
        else:
            cancellation.clear_target_reached(job_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        ProgressPublisher(get_redis()).publish(
            job_id,
            {
                "type": "job_status",
                "status": new_status.value,
                "completion_reason": reason.value if reason else None,
                "completed_at": now_iso if new_status in JOB_TERMINAL_STATUSES else None,
            },
        )

    # Only after the job is actually marked COMPLETED/COMPLETED_PARTIAL in the
    # DB (i.e. the transaction above has committed) do we start moving its kept
    # assets to NAS — never while the job is still RUNNING/DISCOVERING. A
    # FAILED job (accepted_count == 0) has nothing RELEVANCE_PASSED to offload,
    # so it's excluded rather than special-cased. This is still a single
    # dispatch per job, not per relevance-filter batch, so the NAS is not hit
    # with a concurrent burst of writes from many parallel filter workers —
    # schedule_offload_for_job/offload_asset_batch then drip the actual
    # uploads slowly (paced batches over one shared connection, see
    # nas_tasks.py).
    if new_status in (JobStatus.COMPLETED, JobStatus.COMPLETED_PARTIAL):
        from app.tasks.nas_tasks import consolidate_job_dataset_on_nas, schedule_offload_for_job

        schedule_offload_for_job(job_id)
        consolidate_job_dataset_on_nas.delay(job_id)

    from app.tasks.cleanup_tasks import cleanup_shared_cache
    cleanup_shared_cache.delay()

    clear_job_context()


@celery_app.task(name="app.tasks.orchestration_tasks.scan_schedules")
def scan_schedules():
    """Beat-scheduled: fires recurring job templates whose cron is due.

    A template spawns a child Job per firing and starts it; see
    app/services/schedule_service.py for why it is modelled that way rather
    than re-running the template row itself.

    next_run_at is advanced BEFORE the child is started, so a failure while
    starting cannot leave the template permanently due and spawning a child on
    every 60s sweep.
    """
    from app.services.schedule_service import build_child_job, next_fire_time

    now = datetime.now(timezone.utc)
    to_start: list[str] = []

    with session_scope() as db:
        job_repo = JobRepository(db)
        for template in job_repo.list_due_schedules(now):
            try:
                upcoming = next_fire_time(
                    template.schedule_cron, template.schedule_timezone or "UTC", after=now
                )
            except Exception:
                logger.exception(
                    f"Schedule for template {template.id} is unusable; disabling it"
                )
                # Clearing the cron stops an unparseable expression from being
                # retried every 60 seconds forever.
                template.schedule_cron = None
                continue

            first_fire = template.next_run_at is None
            template.next_run_at = upcoming
            template.last_run_at = now

            if first_fire:
                # A template created moments ago has no next_run_at yet. Arm it
                # rather than treating "never scheduled" as "overdue", which
                # would run it immediately regardless of what the cron says.
                logger.info(f"Schedule for template {template.id} armed; first run at {upcoming}")
                continue

            child = build_child_job(template, now)
            db.add(child)
            db.flush()
            for source in child.sources or []:
                db.add(
                    JobSourceTask(
                        id=uuid.uuid4(), job_id=child.id, source_type=source
                    )
                )
            db.flush()
            to_start.append(str(child.id))
            logger.info(
                f"Schedule fired for template {template.id}: created run {child.id}, "
                f"next at {upcoming}"
            )

    # Started after the transaction commits so the discovery tasks cannot race
    # the child's own INSERT.
    for child_id in to_start:
        with session_scope() as db:
            from app.services.job_service import JobService

            JobService(db).start(uuid.UUID(child_id))


@celery_app.task(name="app.tasks.orchestration_tasks.watchdog_scan")
def watchdog_scan():
    """Beat-scheduled (every 60s) safety net: resumes stalled JobSourceTasks,
    kicks off scheduled jobs whose start time has passed, and sweeps for
    deadline-based jobs that should have completed but didn't get an event."""
    settings = get_settings()
    now = datetime.now(timezone.utc)
    # Work last seen before the latest worker boot belongs to a process that died
    # in a restart; once the grace period has passed it is resumed immediately
    # rather than after the stall thresholds (app/domain/recovery.py).
    restart_cutoff = orphan_cutoff(last_worker_boot(get_redis()), now, settings.watchdog_restart_grace_s)

    with session_scope() as db:
        job_repo = JobRepository(db)
        task_repo = JobSourceTaskRepository(db)

        # -- Resume stalled tasks -----------------------------------------------
        for task in task_repo.list_stalled(settings.watchdog_stall_threshold_s, now, orphan_cutoff=restart_cutoff):
            logger.warning(f"Watchdog: task {task.id} stalled (source={task.source_type}), resuming")
            task_repo.set_status(task.id, JobSourceTaskStatus.DOWNLOADING)
            task_repo.heartbeat(task.id)
            db.commit()
            _resume_task(str(task.job_id), str(task.id), task.source_type, task.checkpoint or {})

        # -- Kick off scheduled jobs whose start time has passed -------------------
        for job in job_repo.list_due_scheduled(now):
            logger.info(f"Watchdog: starting scheduled job {job.id}")
            from app.services.job_service import JobService

            JobService(db).start(job.id)

        # -- Deadline sweep for running/discovering jobs, and re-drive jobs stuck
        # in CANCELLING (safety net for the commit-ordering race in
        # JobService.cancel()/evaluate_job_completion's retry guard) --------------
        _cancelling_stall_threshold_s = 120
        for job in job_repo.list_active():
            if job.status in (JobStatus.RUNNING, JobStatus.DISCOVERING):
                schedule_eligible_source_tasks.delay(str(job.id))
            if job.deadline_at is not None and now >= job.deadline_at:
                evaluate_job_completion.delay(str(job.id))
            elif (
                job.status == JobStatus.CANCELLING
                and (now - job.updated_at).total_seconds() > _cancelling_stall_threshold_s
            ):
                logger.warning(f"Watchdog: job {job.id} stuck in CANCELLING, re-driving completion")
                evaluate_job_completion.delay(str(job.id))
            elif (
                job.status in (JobStatus.RUNNING, JobStatus.DISCOVERING)
                and (
                    (now - (job.updated_at.replace(tzinfo=timezone.utc) if job.updated_at and job.updated_at.tzinfo is None else (job.updated_at or now))).total_seconds() > settings.filtering_stall_threshold_s
                    # ... or it has been idle since before the last worker boot, i.e.
                    # the chain was killed by a restart and nothing is coming back.
                    or is_orphaned(job.updated_at.replace(tzinfo=timezone.utc) if job.updated_at and job.updated_at.tzinfo is None else job.updated_at, restart_cutoff)
                )
                and (
                    task_repo.all_terminal_for_job(job.id)
                    or (
                        AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED) >= (
                            int(job.config["candidate_window"])
                            if (job.config and "candidate_window" in job.config)
                            else calculate_initial_candidates(
                                (
                                    ImageBatchRepository(db).get_active_batch(job.id).target_image_count
                                    if ImageBatchRepository(db).get_active_batch(job.id) and ImageBatchRepository(db).get_active_batch(job.id).target_image_count
                                    else min(settings.batch_accepted_target, job.target_count or settings.batch_accepted_target)
                                ),
                                job_config=job.config,
                            )
                        )
                    )
                    or (
                        AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED) > 0
                        and (
                            CancellationService(get_redis()).is_target_reached(str(job.id))
                            or db.execute(
                                select(CandidateWindow.status)
                                .where(CandidateWindow.job_id == job.id)
                                .order_by(CandidateWindow.created_at.desc())
                                .limit(1)
                            ).scalar_one_or_none() == "FULL"
                        )
                    )
                    or (
                        AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED) == 0
                        and bool(task_repo.list_for_job(job.id))
                        and all(
                            t.status in JOB_SOURCE_TASK_TERMINAL_STATUSES or t.status == JobSourceTaskStatus.WINDOW_WAITING
                            for t in task_repo.list_for_job(job.id)
                        )
                    )
                )
                and not get_redis().exists(f"job:{job.id}:filtering_in_progress")
                # Re-driving is idempotent but not free, and the pass takes a while
                # to bump updated_at: without this it would be re-driven every scan.
                and get_redis().set(f"watchdog:redrive:{job.id}", "1", nx=True, ex=300)
            ):
                promoted_count = AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED)
                if promoted_count > 0 or task_repo.all_terminal_for_job(job.id):
                    logger.warning(
                        f"Watchdog: job {job.id} has {promoted_count} PROMOTED assets waiting "
                        f"(all_terminal={task_repo.all_terminal_for_job(job.id)}) and is idle since {job.updated_at}; "
                        f"re-driving filtering"
                    )
                    redis_client = get_redis()
                    redis_client.delete(f"job:{job.id}:filtering_dispatched")
                    from app.tasks.filtering_tasks import run_relevance_filter

                    run_relevance_filter.delay(str(job.id))
                else:
                    logger.warning(
                        f"Watchdog: job {job.id} sources waiting/terminal with 0 PROMOTED assets, "
                        f"re-driving process_active_batch"
                    )
                    from app.tasks.batch_orchestration import process_active_batch

                    process_active_batch.delay(str(job.id))


def _resume_task(job_id: str, job_source_task_id: str, source_type, checkpoint: dict) -> None:
    """Re-invokes the source's discovery task, which will redispatch per-item
    download tasks that each read their own namespaced checkpoint back out of
    JobSourceTask.checkpoint and continue from there. Idempotency for the
    already-processed items is enforced by the Asset unique constraint, so a
    from-scratch discovery re-run cannot duplicate data — the only cost is a
    cheap re-run of the (already-cached-by-most-SDKs) discovery/search step
    and, as a known minor limitation, a re-increment of the task's
    discovered_count rollup (a display counter, not a correctness-critical one).

    Dispatch goes through _SOURCE_DISCOVERY_TASKS — the same registry
    JobService.start uses — rather than a hand-written if/elif chain. That
    chain was a duplicate of the registry and had silently drifted out of
    sync: WEB, YOUTUBE and ROBOFLOW were missing from it, so a stalled task
    on any of those three fell through to a log line and was never resumed.
    Because `stalled` is not a terminal status, such a task also kept
    all_terminal_for_job() False forever, pinning the whole job in `running`.
    Reading the registry makes every registered connector resumable by
    construction and removes the class of bug entirely."""
    from app.services.job_service import _SOURCE_DISCOVERY_TASKS, _resume_pending_items

    try:
        source = SourceType(source_type)
    except ValueError:
        logger.warning(f"Watchdog: unknown source_type={source_type}, cannot resume")
        return

    with session_scope() as db:
        from app.db.models.job import Job
        job = db.get(Job, uuid.UUID(job_id))
        if job and str(source.value) not in (job.sources or []):
            logger.warning(f"Watchdog: source {source_type} is not in job {job_id} sources, skipping resume")
            return

    if _resume_pending_items(source, job_id, job_source_task_id, checkpoint):
        return

    if not _SOURCE_DISCOVERY_TASKS:
        import app.tasks.discovery_tasks  # defensive: populate _SOURCE_DISCOVERY_TASKS

    task_path = _SOURCE_DISCOVERY_TASKS.get(source)
    if task_path is None:
        logger.warning(f"Watchdog: no connector registered for source_type={source_type}")
        return

    celery_app.send_task(task_path, args=[job_id, job_source_task_id])


@celery_app.task(name="app.tasks.orchestration_tasks.schedule_eligible_source_tasks", bind=True)
def schedule_eligible_source_tasks(self, job_id: str, max_batch_per_source: int = 20) -> dict:
    """Checkpoint-driven persistent scheduler that dispatches bounded batches across all unexhausted sources.

    Continuous ingestion without arbitrary round caps or deadlocks.
    """
    job_uuid = uuid.UUID(job_id)
    with session_scope() as db:
        job = JobRepository(db).get(job_uuid)
        if job is None or job.status not in (JobStatus.RUNNING, JobStatus.DISCOVERING):
            return {"status": "inactive_job"}

        target = job.target_count
        accepted = job.accepted_count or 0
        if target is not None and accepted >= target:
            return {"status": "target_reached"}

        from app.tasks.batch_orchestration import BatchOrchestrator
        from app.db.models.candidate_window import CandidateWindow
        from app.db.models.candidate_reservation import CandidateReservation
        from app.services.candidate_reservation_service import cleanup_stale_reservations

        orchestrator = BatchOrchestrator(db, job_uuid)
        active_b = orchestrator.get_or_create_active_batch()
        if active_b is None:
            return {"status": "no_active_batch"}

        window = db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.batch_id == active_b.id)
            .order_by(CandidateWindow.window_index.desc())
            .limit(1)
        ).scalar_one_or_none()

        if window is not None:
            # Clean up abandoned reservations (> 60s without update)
            cleanup_stale_reservations(db, window.id, stale_seconds=60)
            db.flush()

            # If window is FULL and all reservations are resolved, advance the batch
            if window.status == "FULL":
                active_reserved = db.execute(
                    select(func.count(CandidateReservation.id)).where(
                        CandidateReservation.window_id == window.id,
                        CandidateReservation.status == "RESERVED",
                    )
                ).scalar_one()
                if active_reserved == 0:
                    from app.tasks.batch_orchestration import process_active_batch
                    process_active_batch.delay(str(job_uuid))
                    return {"status": "window_full_advancing_batch"}

        task_repo = JobSourceTaskRepository(db)
        tasks = task_repo.list_for_job(job_uuid)
        from app.domain.source_priority import evaluate_sequential_execution_plan, is_source_round_complete
        from app.services.job_service import _resume_pending_items, _SOURCE_DISCOVERY_TASKS
        if not _SOURCE_DISCOVERY_TASKS:
            import app.tasks.discovery_tasks

        from app.services.cancellation_service import CancellationService
        canc = CancellationService(get_redis())
        for t in tasks:
            if t.status == JobSourceTaskStatus.DOWNLOADING and canc.is_source_cap_reached(str(job_uuid), str(t.id)):
                task_repo.set_status(t.id, JobSourceTaskStatus.PARTIALLY_PROCESSED)
        db.flush()

        # Evaluate canonical priority plan across configured sources
        plan = evaluate_sequential_execution_plan(job, tasks)

        # Check if all sources completed current round
        if plan.all_sources_complete:
            if target is not None and accepted >= target:
                from app.tasks.orchestration_tasks import evaluate_job_completion
                evaluate_job_completion.delay(job_id)
                return {"status": "target_reached", "round": plan.current_round}

            # Increment round without arbitrary round limits; start again from Priority 1 (WEB)
            next_round = plan.current_round + 1
            cfg = dict(job.config or {})
            cfg["topup_round"] = next_round
            job.config = cfg
            for t in tasks:
                cp = dict(t.checkpoint or {})
                cp["discovery_exhausted"] = False
                task_repo.heartbeat(t.id, checkpoint=cp)
                task_repo.set_status(t.id, JobSourceTaskStatus.PENDING)
                canc.clear_source_cap_reached(str(job_uuid), str(t.id))
            db.commit()
            plan = evaluate_sequential_execution_plan(job, tasks)

        active_src = plan.active_source
        if active_src is None:
            return {"status": "idle", "plan": "all_sources_complete"}

        # STRICT SINGLE-SOURCE EXECUTION:
        # Only the currently active source may receive new discovery, download, extraction and candidate-ingestion tasks.
        active_task = next((t for t in tasks if t.source_type == active_src.value), None)
        if active_task is None:
            active_task = JobSourceTask(id=uuid.uuid4(), job_id=job_uuid, source_type=active_src.value)
            task_repo.create(active_task)
            db.flush()

        if active_task.status == JobSourceTaskStatus.DOWNLOADING:
            return {
                "status": "active_source_downloading",
                "active_source": active_src.value,
                "position": plan.active_source_position,
            }

        source_str = active_src.value
        source_lock_key = f"job:{job_id}:source:{source_str}:dispatch_lock"
        if not get_redis().set(source_lock_key, "1", nx=True, ex=30):
            return {
                "status": "source_locked",
                "active_source": source_str,
                "position": plan.active_source_position,
            }

        dispatched = {}
        cp = active_task.checkpoint or {}
        pending = cp.get("_pending_items") or {}
        completed = set(cp.get("_completed_items", []))
        unconsumed = [k for k in pending.keys() if k not in completed]

        if unconsumed:
            task_repo.set_status(active_task.id, JobSourceTaskStatus.DOWNLOADING)
            task_repo.heartbeat(active_task.id)
            ok = _resume_pending_items(
                active_src, str(job_uuid), str(active_task.id), active_task.checkpoint, max_batch=max_batch_per_source
            )
            if ok:
                dispatched[source_str] = len(unconsumed[:max_batch_per_source])
            else:
                get_redis().delete(source_lock_key)
        else:
            task_path = _SOURCE_DISCOVERY_TASKS.get(active_src)
            if task_path is not None and not cp.get("discovery_exhausted", False):
                task_repo.set_status(active_task.id, JobSourceTaskStatus.DISCOVERING)
                task_repo.heartbeat(active_task.id)
                celery_app.send_task(task_path, args=[str(job_uuid), str(active_task.id)])
                dispatched[source_str] = "discovery_dispatched"
            else:
                task_repo.set_status(active_task.id, JobSourceTaskStatus.COMPLETED)
                get_redis().delete(source_lock_key)

        db.commit()
        return {
            "status": "scheduled",
            "active_source": source_str,
            "position": plan.active_source_position,
            "dispatched": dispatched,
        }

