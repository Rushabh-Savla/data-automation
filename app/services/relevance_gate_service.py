"""Early Visual Relevance Admission Gate Service.

Evaluates transient candidate images before they can be staged to persistent
disk or written to PostgreSQL as assets. Dispatches to the GPU worker on queue
'filtering' and caches scores in Redis by SHA256 fingerprint and query.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from PIL import Image

from app.core.redis_client import get_redis

logger = logging.getLogger(__name__)


def evaluate_transient_candidate(
    image_path: Path | str,
    query: str,
    threshold: float = 0.40,
    fingerprint: str | None = None,
    min_resolution: tuple[int, int] | None = None,
    timeout: float = 30.0,
) -> tuple[str, float | None]:
    """Evaluates a transient candidate image before staging.

    Returns:
        (outcome, score) where outcome is 'accept', 'reject', or 'error',
        and score is the float confidence score or None.
    """
    path_obj = Path(image_path)
    if not path_obj.exists() or path_obj.stat().st_size == 0:
        logger.debug("Transient candidate file missing or empty: %s", image_path)
        return "reject", None

    # Step 1: Validate image integrity and resolution
    try:
        with Image.open(path_obj) as img:
            w, h = img.size
            if min_resolution is not None:
                min_w, min_h = min_resolution
                if w < min_w or h < min_h:
                    logger.debug(
                        "Candidate %s rejected: resolution (%d, %d) < minimum (%d, %d)",
                        image_path,
                        w,
                        h,
                        min_w,
                        min_h,
                    )
                    return "reject", None
            img.verify()
    except Exception as e:
        logger.warning("Corrupt or invalid candidate image at %s: %s", image_path, e)
        return "reject", None

    # Step 2: Check Redis score cache by SHA256 fingerprint
    if fingerprint:
        try:
            from app.filtering.sam3_filter import compute_sam3_cache_key
            r = get_redis()
            if r is not None:
                cache_key = compute_sam3_cache_key(fingerprint, query)
                cached = r.get(cache_key)
                if cached is not None:
                    cached_score = float(cached)
                    outcome = "accept" if cached_score >= threshold else "reject"
                    return outcome, cached_score
        except Exception as e:
            logger.debug("Cache lookup failed: %s", e)

    # Step 3: Run SAM 3.1 inference on GPU worker
    try:
        from app.tasks.filtering_tasks import evaluate_transient_candidate_task

        # If Celery is configured in eager mode (e.g. during tests), call directly
        from app.tasks.celery_app import celery_app
        if celery_app.conf.task_always_eager:
            res = evaluate_transient_candidate_task.apply(
                args=[str(path_obj.resolve()), query, threshold, fingerprint]
            )
            return res.result

        async_res = evaluate_transient_candidate_task.apply_async(
            args=[str(path_obj.resolve()), query, threshold, fingerprint],
            queue="filtering",
        )
        outcome, score = async_res.get(timeout=timeout, disable_sync_subtasks=False)
        return outcome, score

    except Exception as exc:
        logger.warning(
            "Early relevance evaluation via worker-gpu failed or timed out for %s: %s",
            image_path,
            exc,
        )
        return "error", None

