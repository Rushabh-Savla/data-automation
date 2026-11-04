"""Orchestration: the chord callback that marks a source complete and, once
every source for a job is terminal, evaluates whether the whole job is done
(directly for now; Phase 4 inserts the relevance-filter/dedup chain here
before completion evaluation). Also the Beat-scheduled watchdog that resumes
stalled tasks and kicks off scheduled/deadline-based jobs.
"""

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select

from app.core.config import get_settings
from app.core.logging import bind_job_context, clear_job_context
from app.core.redis_client import get_redis
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.completion import JobCompletionEvaluator, JobSnapshot
from app.domain.enums import (
    AssetStatus,
    JOB_SOURCE_TASK_TERMINAL_STATUSES,
    JobSourceTaskStatus,
    JobStatus,
    SourceType,
)
from app.domain.recovery import is_orphaned, orphan_cutoff
from app.domain.state_machine import JobStateMachine
from app.domain.topup import SourceState, TopupPlanner, TopupSnapshot, filtering_incomplete
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
            r["item"] for r in (results or []) if isinstance(r, dict) and r.get("item") and not r.get("stopped") and not r.get("window_full")
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

        if had_stopped and cancellation.is_cancelled(job_id):
            final_status = JobSourceTaskStatus.CANCELLED
        elif had_window_full:
            final_status = JobSourceTaskStatus.WINDOW_WAITING
            window_waiting = True
        elif had_errors:
            final_status = JobSourceTaskStatus.COMPLETED_PARTIAL
        else:
            final_status = JobSourceTaskStatus.COMPLETED

        if task.status != JobSourceTaskStatus.CANCELLED:
            task_repo.set_status(task_uuid, final_status)
        db.flush()

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

        all_terminal = task_repo.all_terminal_for_job(job_uuid)
        tasks = task_repo.list_for_job(job_uuid)
        all_waiting_or_terminal = bool(tasks) and all(
            t.status in JOB_SOURCE_TASK_TERMINAL_STATUSES or t.status == JobSourceTaskStatus.WINDOW_WAITING
            for t in tasks
        )

        # Check if an eligible candidate window or batch is ready for processing
        cancellation = CancellationService(get_redis())

        from app.db.models.candidate_window import CandidateWindow
        active_window = db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.job_id == job_uuid)
            .order_by(CandidateWindow.window_index.desc())
            .limit(1)
        ).scalar_one_or_none()

        window_is_full = active_window is not None and active_window.status == "FULL"
        window_limit = active_window.candidate_limit if active_window else None

        candidate_window = job.config.get("candidate_window", get_settings().download_candidate_window)
        active_batch_target = min(candidate_window, job.target_count or candidate_window)
        window_buffered_target = buffered_target(active_batch_target, get_settings().target_overfetch_buffer)

        promoted_count = asset_repo.count_for_job(job_uuid, status=AssetStatus.PROMOTED)

        window_ready = (
            cancellation.is_target_reached(str(job_id))
            or (window_is_full and promoted_count > 0)
            or (all_waiting_or_terminal and promoted_count > 0)
            or (window_limit is not None and promoted_count >= window_limit and promoted_count > 0)
            or job.accepted_count >= window_buffered_target
            or promoted_count >= window_buffered_target
            or (promoted_count >= active_batch_target and promoted_count > 0)
        )

        if not all_terminal and not window_ready:
            return

        remaining_sources = job.config.get("_fast_path_remaining_sources") or []
        target_reached = job.target_count is not None and job.accepted_count >= job.target_count

        if remaining_sources and not target_reached and all_terminal:
            redis_client = get_redis()
            escalate_key = f"job:{job_id}:fast_path_escalated"
            if not redis_client.set(escalate_key, "1", nx=True, ex=86400):
                # Another concurrent caller already claimed the escalation —
                # nothing more to do this round; its own dispatch will make
                # this job's task set non-terminal again until it finishes.
                return

            new_config = dict(job.config)
            new_config.pop("_fast_path_remaining_sources", None)
            job_repo.set_config(job_uuid, new_config)

            new_tasks = []
            for source_value in remaining_sources:
                task = JobSourceTask(id=uuid.uuid4(), job_id=job_uuid, source_type=source_value)
                task_repo.create(task)
                new_tasks.append((source_value, task.id))
            db.flush()

        else:
            new_tasks = None

    if remaining_sources and not target_reached and new_tasks:
        from app.services.job_service import _SOURCE_DISCOVERY_TASKS, JobService

        logger.info(f"Fast-path escalation for job {job_id}: target not reached from web, dispatching {remaining_sources}")
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
            accepted = asset_repo.count_for_job(job_uuid, status=AssetStatus.RELEVANCE_PASSED)
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
            sources = [
                SourceState(
                    source_type=str(getattr(t.source_type, "value", t.source_type)),
                    accepted=t.accepted_count,
                    exhausted=bool((t.checkpoint or {}).get("discovery_exhausted")),
                )
                for t in tasks
            ]

            plan = TopupPlanner.plan(
                TopupSnapshot(
                    target_count=job.target_count,
                    accepted_post_filter=accepted,
                    promoted_total=promoted_total,
                    round_number=round_number,
                    max_rounds=settings.topup_max_rounds,
                    sources=sources,
                    gained_last_round=(
                        accepted - previous_accepted if previous_accepted is not None else None
                    ),
                    promoted_last_round=(
                        promoted_total - previous_promoted if previous_promoted is not None else None
                    ),
                )
            )

            if plan is None:
                logger.info(
                    f"Top-up for job {job_id}: stopping at {accepted}/{job.target_count} "
                    f"after round {round_number}"
                )
            else:
                logger.info(
                    f"Top-up for job {job_id}: round {plan.round_number}, short by "
                    f"{plan.shortfall}, observed yield {plan.observed_yield:.2f}, "
                    f"fetching ~{plan.total_quota} more"
                )

                config = dict(job.config or {})
                config["_topup"] = {
                    "round": plan.round_number,
                    "quota": plan.quota,
                    "accepted_after_last_round": accepted,
                    "promoted_after_last_round": promoted_total,
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
                    redispatch.append((source_value, str(task.id)))

        # -- After the transaction has committed. --------------------------
        if plan is None:
            # No further rounds, so any overshoot is final: trim back to
            # exactly target_count and evaluate completion. Trimming is
            # deliberately deferred to here rather than run after every round,
            # because cutting the surplus between rounds would destroy the
            # margin the next round is trying to build.
            from app.tasks.filtering_tasks import trim_excess_assets

            trim_excess_assets(job_id)
            evaluate_job_completion.delay(job_id)
            return

        ProgressPublisher(get_redis()).publish(
            job_id,
            {
                "type": "topup_round",
                "round": plan.round_number,
                "max_rounds": settings.topup_max_rounds,
                "shortfall": plan.shortfall,
                "observed_yield": round(plan.observed_yield, 3),
            },
        )

        for source_value, task_id in redispatch:
            _resume_task(job_id, task_id, source_value, {})
    finally:
        clear_job_context()


@celery_app.task(name="app.tasks.orchestration_tasks.evaluate_job_completion", bind=True)
def evaluate_job_completion(self, job_id: str):
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
            from app.domain.enums import JobCompletionReason

            job_repo.set_status(job_uuid, JobStatus.CANCELLED, completion_reason=JobCompletionReason.CANCELLED)
            cancellation.clear_active_tasks(job_id)
            ProgressPublisher(get_redis()).publish(job_id, {"type": "job_status", "status": "cancelled"})
            clear_job_context()
            return

        snapshot = JobSnapshot(
            target_count=job.target_count,
            deadline_at=job.deadline_at,
            accepted_count=job.accepted_count,
            all_sources_terminal=task_repo.all_terminal_for_job(job_uuid),
        )
        reason = JobCompletionEvaluator.should_stop(snapshot)
        if reason is None:
            clear_job_context()
            return

        success = JobCompletionEvaluator.is_success(reason, job.accepted_count, job.target_count)
        event = "complete" if success else ("fail" if job.accepted_count == 0 else "complete_partial")
        new_status = JobStateMachine.transition(job.status, event)
        job_repo.set_status(job_uuid, new_status, completion_reason=reason)
        cancellation.clear_target_reached(job_id)
        cancellation.clear_active_tasks(job_id)
        ProgressPublisher(get_redis()).publish(
            job_id, {"type": "job_status", "status": new_status.value, "completion_reason": reason.value}
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
        from app.tasks.nas_tasks import schedule_offload_for_job

        schedule_offload_for_job(job_id)

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
            task_repo.set_status(task.id, JobSourceTaskStatus.STALLED)
            task_repo.increment_counts(task.id, errors=0)  # no-op, keeps intent explicit
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
                        AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED) >= min(
                            job.config.get("candidate_window", settings.download_candidate_window),
                            job.target_count or settings.download_candidate_window,
                        )
                    )
                    or (
                        AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED) > 0
                        and CancellationService(get_redis()).is_target_reached(str(job.id))
                    )
                )
                and not get_redis().exists(f"job:{job.id}:filtering_in_progress")
                # Re-driving is idempotent but not free, and the pass takes a while
                # to bump updated_at: without this it would be re-driven every scan.
                and get_redis().set(f"watchdog:redrive:{job.id}", "1", nx=True, ex=300)
            ):
                # Post-ingestion stall: either every source finished, or an eligible batch of
                # PROMOTED assets is waiting while sources are slow/exhausted, but the
                # filtering -> dedup -> completion chain died or was never dispatched.
                #
                # Re-driving is safe to repeat: run_relevance_filter only
                # selects PROMOTED assets, so already-classified assets are
                # not reconsidered, and finalize_relevance_filter derives its
                # counts from the DB. A job whose assets are all already
                # classified therefore passes straight through to dedup and
                # completion rather than redoing any inference.
                promoted_count = AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED)
                logger.warning(
                    f"Watchdog: job {job.id} has {promoted_count} PROMOTED assets waiting "
                    f"(all_terminal={task_repo.all_terminal_for_job(job.id)}) and is idle since {job.updated_at}; "
                    f"re-driving filtering"
                )
                redis_client = get_redis()
                redis_client.delete(f"job:{job.id}:filtering_dispatched")
                from app.tasks.filtering_tasks import run_relevance_filter

                run_relevance_filter.delay(str(job.id))


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

    task_path = _SOURCE_DISCOVERY_TASKS.get(source)
    if task_path is None:
        logger.warning(f"Watchdog: no connector registered for source_type={source_type}")
        return

    celery_app.send_task(task_path, args=[job_id, job_source_task_id])
