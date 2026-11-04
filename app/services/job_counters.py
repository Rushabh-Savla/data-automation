"""Reconciling the denormalised progress counters against the assets themselves.

The counters on Job and JobSourceTask are a cache. The assets table is the
truth. Keeping them in step by hand — one increment per event, scattered
across ingestion, filtering, dedup and trim — is what produced the numbers
the UI was showing:

  * The per-source row said "Accepted 754, Rejected 0" while the job header
    said "107 / 500 accepted, 647 rejected". Both described the same 754
    assets. Ingestion incremented BOTH the source and job rollups
    (_helpers.build_on_progress), but every downstream correction —
    relevance rejection, dedup, trim — only ever adjusted the job row, so
    the per-source columns were frozen at their pre-filter values and could
    never sum to the header.
  * job.discovered_count sat at 0 while its source task reported 16, because
    discovery only ever incremented the task rollup.
  * Re-running a stage double-counted: a batch redelivered after a worker
    restart decremented accepted_count twice for the same asset, and a job
    with 754 assets reported 797 rejections.

Recomputing from a GROUP BY makes all three classes of bug unrepresentable:
the numbers are derived, so they cannot drift, cannot double-count, and
cannot disagree between the two levels. Stages call reconcile() once when
they finish instead of hand-incrementing per asset — fewer writes, and the
result is correct regardless of how many times a stage ran.

Ingestion deliberately keeps its incremental updates: they drive the live
progress feed and the target/fair-share stop flags, which need a running
count during the download rather than a total at the end. reconcile() then
corrects whatever those approximations drifted to.
"""

import logging
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models.asset import Asset
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import (
    ASSET_ACCEPTED_STATUSES,
    RELEVANCE_ACCEPTED_STATUSES,
    AssetStatus,
)

logger = logging.getLogger(__name__)

# Statuses that mean "this asset survived filtering and is accepted toward the target".
# Strictly: {RELEVANCE_PASSED, READY_FOR_UPLOAD, UPLOADING, NAS_VERIFIED, COMPLETED}.
# Unverified NAS uploads (READY_FOR_UPLOAD, UPLOADING) count toward candidate target progress,
# but are NEVER counted as NAS-verified or completed.
_ACCEPTED = tuple(RELEVANCE_ACCEPTED_STATUSES)
_REJECTED = (AssetStatus.RELEVANCE_REJECTED, AssetStatus.FAILED)
_DUPLICATE = (AssetStatus.DUPLICATE,)


def _counts_by_status(db: Session, job_id: uuid.UUID) -> dict[str, int]:
    rows = db.execute(
        select(Asset.status, func.count())
        .where(Asset.job_id == job_id)
        .group_by(Asset.status)
    ).all()
    return {str(getattr(status, "value", status)): count for status, count in rows}


def _counts_by_task_and_status(db: Session, job_id: uuid.UUID) -> dict[uuid.UUID, dict[str, int]]:
    rows = db.execute(
        select(Asset.job_source_task_id, Asset.status, func.count())
        .where(Asset.job_id == job_id)
        .group_by(Asset.job_source_task_id, Asset.status)
    ).all()
    out: dict[uuid.UUID, dict[str, int]] = {}
    for task_id, status, count in rows:
        out.setdefault(task_id, {})[str(getattr(status, "value", status))] = count
    return out


def _sum(counts: dict[str, int], statuses) -> int:
    return sum(counts.get(s.value, 0) for s in statuses)


def reconcile(db: Session, job_id: uuid.UUID) -> dict[str, int]:
    """Recomputes the job's and each source task's counters from its assets.

    Returns the job-level totals it wrote, so a caller can log or publish
    them without a second query. Does not commit — the caller's session
    scope owns that.
    """
    job = db.get(Job, job_id)
    if job is None:
        return {}

    per_task = _counts_by_task_and_status(db, job_id)
    for task in db.execute(
        select(JobSourceTask).where(JobSourceTask.job_id == job_id)
    ).scalars():
        counts = per_task.get(task.id, {})
        task.accepted_count = _sum(counts, _ACCEPTED)
        task.rejected_count = _sum(counts, _REJECTED)
        # downloaded = every asset row this source ever created; a row only
        # exists once its file landed.
        task.downloaded_count = sum(counts.values())

    totals = _counts_by_status(db, job_id)
    job.accepted_count = _sum(totals, _ACCEPTED)
    job.rejected_count = _sum(totals, _REJECTED)
    job.duplicate_count = _sum(totals, _DUPLICATE)
    job.downloaded_count = sum(totals.values())
    # discovered_count is NOT derived here: discovery counts candidate items
    # (videos, dataset entries) that may each yield many assets or none, so
    # it has no expression in terms of asset rows. It is rolled up from the
    # source tasks instead, which is where discovery records it.
    job.discovered_count = sum(
        t.discovered_count
        for t in db.execute(
            select(JobSourceTask).where(JobSourceTask.job_id == job_id)
        ).scalars()
    )

    result = {
        "accepted": job.accepted_count,
        "relevance_accepted": totals.get(AssetStatus.RELEVANCE_PASSED.value, 0),
        "upload_pending": totals.get(AssetStatus.READY_FOR_UPLOAD.value, 0),
        "uploading": totals.get(AssetStatus.UPLOADING.value, 0),
        "nas_verified": totals.get(AssetStatus.NAS_VERIFIED.value, 0),
        "completed": totals.get(AssetStatus.COMPLETED.value, 0),
        "review_pending": totals.get(AssetStatus.REVIEW_PENDING.value, 0),
        "retry_pending": totals.get(AssetStatus.RETRY_PENDING.value, 0),
        "rejected": job.rejected_count,
        "duplicate": job.duplicate_count,
        "downloaded": job.downloaded_count,
        "discovered": job.discovered_count,
    }
    logger.info(f"Reconciled counters for job {job_id}: {result}")
    return result
