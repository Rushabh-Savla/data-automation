"""Source priority definitions and strict sequential scheduling state machine.

Enforces strict single-source execution in canonical priority order:
1. WEB
2. KAGGLE
3. ROBOFLOW
4. COCO
5. YOUTUBE
6. DAVIS 2017
7. VISUAL GENOME
8. SUN RGB-D
9. OPEN IMAGES V7
10. PASCAL VOC
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import JobSourceTaskStatus, SourceType

logger = logging.getLogger(__name__)

CANONICAL_SOURCE_ORDER: list[SourceType] = [
    SourceType.WEB,            # 1
    SourceType.KAGGLE,         # 2
    SourceType.ROBOFLOW,       # 3
    SourceType.COCO,           # 4
    SourceType.YOUTUBE,        # 5
    SourceType.DAVIS,          # 6
    SourceType.VISUAL_GENOME,  # 7
    SourceType.SUN_RGBD,       # 8
    SourceType.OPEN_IMAGES,    # 9
    SourceType.PASCAL_VOC,     # 10
]


def get_ordered_job_sources(job_sources: list[str | SourceType] | None) -> list[SourceType]:
    """Returns canonical sources filtered by job configuration, preserving relative order."""
    if not job_sources:
        return list(CANONICAL_SOURCE_ORDER)
    configured_set = {
        s.value if hasattr(s, "value") else str(s)
        for s in job_sources
    }
    return [s for s in CANONICAL_SOURCE_ORDER if s.value in configured_set]


def is_source_round_complete(task: JobSourceTask | None) -> tuple[bool, str]:
    """Evaluates whether a source task has genuinely completed all eligible work for the current round.

    Returns (is_complete, reason).
    """
    if task is None:
        return False, "Not initialized"

    # Terminal states are always complete regardless of leftover unconsumed items
    if task.status in (
        JobSourceTaskStatus.COMPLETED,
        JobSourceTaskStatus.SOURCE_EXHAUSTED,
        JobSourceTaskStatus.COMPLETED_PARTIAL,
        JobSourceTaskStatus.CANCELLED,
    ):
        return True, "Inventory fully processed for current round"

    if task.status == JobSourceTaskStatus.FAILED:
        return True, f"Failed: {task.error_message or 'Permanent failure recorded'}"

    # Per-source fair-share cap: if this source has hit its cap for the current round,
    # it is complete for this round and must yield to the next source in sequential priority.
    from app.core.redis_client import get_redis
    from app.services.cancellation_service import CancellationService
    try:
        if CancellationService(get_redis()).is_source_cap_reached(str(task.job_id), str(task.id)):
            return True, "Fair-share source cap reached for current round"
    except Exception:
        pass

    # Active processing states are never complete (unless capped above)
    if task.status in (
        JobSourceTaskStatus.DISCOVERING,
        JobSourceTaskStatus.DOWNLOADING,
        JobSourceTaskStatus.STANDARDIZING,
    ):
        return False, f"Actively {getattr(task.status, 'value', task.status)}"

    cp = task.checkpoint or {}
    pending = cp.get("_pending_items") or {}
    completed = set(cp.get("_completed_items", []))
    unconsumed = [k for k in pending.keys() if k not in completed]

    # If waiting for candidate window capacity, it is still active
    if task.status == JobSourceTaskStatus.WINDOW_WAITING:
        return False, "Waiting for candidate window capacity"

    # If unconsumed inventory exists, the source is still incomplete
    if unconsumed:
        return False, f"{len(unconsumed)} pending items remaining in inventory"

    # If in partial or discovery completed status, check if discovery has remaining pages
    if task.status == JobSourceTaskStatus.DISCOVERY_COMPLETED:
        # Check if there are unconsumed items or if pagination continues
        cursor = cp.get("cursor") or cp.get("next_cursor")
        if cursor and not cp.get("discovery_exhausted", False):
            return False, "Additional discovery pages available"
        return True, "All discovered items processed"

    if task.status == JobSourceTaskStatus.PARTIALLY_PROCESSED:
        if unconsumed:
            return False, f"{len(unconsumed)} unconsumed items remaining"
        return True, "Partially processed; inventory consumed for round"

    if task.status == JobSourceTaskStatus.PENDING:
        # A pending task is waiting its turn in the sequential execution order
        return False, "Pending execution in sequence"

    return True, f"Status {getattr(task.status, 'value', task.status)}"


@dataclass
class SourceProgressStats:
    source_type: str
    status: str
    discovered_count: int = 0
    total_inventoried: int = 0
    processed_count: int = 0
    pending_count: int = 0
    accepted_count: int = 0
    rejected_count: int = 0
    error_count: int = 0
    completion_percentage: float = 0.0
    waiting_reason: str = ""
    is_active: bool = False
    is_complete: bool = False


@dataclass
class SequentialExecutionPlan:
    current_round: int
    active_source: SourceType | None
    active_source_position: str
    active_source_index: int
    total_sources: int
    completed_sources: list[str] = field(default_factory=list)
    next_source_in_queue: str | None = None
    all_sources_complete: bool = False
    source_stats: dict[str, SourceProgressStats] = field(default_factory=dict)
    waiting_reason: str = ""


def evaluate_sequential_execution_plan(
    job: Job,
    tasks: list[JobSourceTask],
) -> SequentialExecutionPlan:
    """Computes the strict single active source, sequence position, and round state."""
    ordered_sources = get_ordered_job_sources(job.sources)
    total_count = len(ordered_sources)
    task_map: dict[str, JobSourceTask] = {
        t.source_type: t for t in tasks
    }

    current_round = int(job.config.get("topup_round", 1)) if job.config else 1
    completed_sources: list[str] = []
    source_stats: dict[str, SourceProgressStats] = {}

    job_status_val = getattr(getattr(job, "status", None), "value", getattr(job, "status", None))
    job_is_terminal = job_status_val in ("completed", "failed", "cancelled", "completed_partial")

    active_source: SourceType | None = None
    active_pos_str = f"1/{total_count}"
    active_idx = 0
    next_source_name: str | None = None
    active_reason = ""

    for idx, src in enumerate(ordered_sources):
        src_val = src.value
        task = task_map.get(src_val)

        # Extract counts safely handling uninitialized/None attributes
        discovered = (task.discovered_count or 0) if task else 0
        accepted = (task.accepted_count or 0) if task else 0
        rejected = (task.rejected_count or 0) if task else 0
        errors = (task.error_count or 0) if task else 0
        status_val = getattr(task.status, "value", task.status) if task and task.status else JobSourceTaskStatus.PENDING.value

        cp = task.checkpoint if task and task.checkpoint else {}
        pending_dict = cp.get("_pending_items") or {} if cp else {}
        completed_set = set(cp.get("_completed_items", [])) if cp else set()
        unconsumed_count = len([k for k in pending_dict.keys() if k not in completed_set])

        total_inv = len(pending_dict) if pending_dict else discovered
        processed = len(completed_set) if pending_dict else (accepted + rejected)
        pct = round((processed / total_inv * 100.0), 1) if total_inv > 0 else (100.0 if task and task.status == JobSourceTaskStatus.COMPLETED else 0.0)

        is_done, reason = is_source_round_complete(task)
        if job_is_terminal:
            is_done = True
            reason = f"Job {job_status_val}"

        if is_done:
            completed_sources.append(src_val)
        elif active_source is None and not job_is_terminal:
            active_source = src
            active_idx = idx
            active_pos_str = f"{idx + 1}/{total_count}"
            active_reason = reason
            if idx + 1 < total_count:
                next_source_name = ordered_sources[idx + 1].value

        stats = SourceProgressStats(
            source_type=src_val,
            status=status_val,
            discovered_count=discovered,
            total_inventoried=total_inv,
            processed_count=processed,
            pending_count=unconsumed_count,
            accepted_count=accepted,
            rejected_count=rejected,
            error_count=errors,
            completion_percentage=pct,
            waiting_reason=reason if not is_done else "Complete for round",
            is_active=(src == active_source and not job_is_terminal),
            is_complete=is_done,
        )
        source_stats[src_val] = stats

    all_done = (active_source is None or job_is_terminal)

    if all_done:
        active_pos_str = f"{total_count}/{total_count}"
        active_reason = f"Job is {job_status_val}" if job_is_terminal else "All configured sources completed current round"

    return SequentialExecutionPlan(
        current_round=current_round,
        active_source=active_source,
        active_source_position=active_pos_str,
        active_source_index=active_idx,
        total_sources=total_count,
        completed_sources=completed_sources,
        next_source_in_queue=next_source_name,
        all_sources_complete=all_done,
        source_stats=source_stats,
        waiting_reason=active_reason,
    )
