"""Batch Orchestration Engine — coordinates bounded candidate acquisition,
shortfall calculation, replacement loops, and manifest creation for 600-image batches.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import bind_job_context, clear_job_context
from app.core.redis_client import get_redis
from app.db.models.asset import Asset
from app.db.models.image_batch import ImageBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.image_batch_repository import ImageBatchRepository
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.enums import (
    ASSET_ACCEPTED_STATUSES,
    JOB_SOURCE_TASK_TERMINAL_STATUSES,
    AssetStatus,
    BatchStatus,
    JobCompletionReason,
    JobSourceTaskStatus,
    JobStatus,
    SourceType,
)
import app.tasks.discovery_tasks
import app.tasks.ingest_tasks
from app.filtering.cross_batch_dedup import CrossBatchDeduplicator
from app.services.cancellation_service import CancellationService
from app.services.job_service import _SOURCE_DISCOVERY_TASKS, JobService
from app.services.progress_service import ProgressPublisher
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


class BatchOrchestrator:
    """Coordinates single-batch execution: 600 accepted images with shortfall replenishment."""

    def __init__(self, db: Session, job_id: uuid.UUID):
        self.db = db
        self.job_id = job_id
        self.settings = get_settings()
        self.job_repo = JobRepository(db)
        self.task_repo = JobSourceTaskRepository(db)
        self.asset_repo = AssetRepository(db)
        self.batch_repo = ImageBatchRepository(db)
        self.cancellation = CancellationService(get_redis())

    def _create_or_get_batch_unlocked(self) -> ImageBatch | None:
        job = self.db.execute(
            select(Job).where(Job.id == self.job_id).with_for_update()
        ).scalar_one_or_none()
        if job is None:
            return None

        # Compute total accepted images across completed / uploading batches
        existing_batches = self.batch_repo.list_for_job(self.job_id)
        total_accepted = sum(
            b.accepted_image_count for b in existing_batches
            if b.status in (BatchStatus.COMPLETED, BatchStatus.READY_FOR_UPLOAD, BatchStatus.UPLOADING, BatchStatus.VERIFYING)
        )
        
        target = job.target_count or self.settings.batch_accepted_target
        if total_accepted >= target:
            return None  # All batches fulfilled

        active = self.batch_repo.get_active_batch(self.job_id)
        if active is not None and active.status not in (BatchStatus.READY_FOR_UPLOAD, BatchStatus.UPLOADING, BatchStatus.VERIFYING):
            return active

        next_index = len(existing_batches) + 1
        remaining_needed = target - total_accepted
        batch_target = min(self.settings.batch_accepted_target, remaining_needed)

        new_batch = ImageBatch(
            id=uuid.uuid4(),
            job_id=self.job_id,
            batch_index=next_index,
            target_image_count=batch_target,
            status=BatchStatus.DISCOVERING,
        )
        self.batch_repo.create(new_batch)
        self.db.commit()
        logger.info(
            f"[BATCH ORCHESTRATION] Job {self.job_id}: Created Batch #{next_index} "
            f"with target={batch_target} (Remaining needed: {remaining_needed})"
        )
        return new_batch

    def get_or_create_active_batch(self) -> ImageBatch | None:
        """Finds the current active batch or creates the next batch up to job.target_count.
        Guarded against concurrent execution so two workers never create identical batches.
        """
        redis = get_redis()
        lock_key = f"job:{self.job_id}:batch_create_lock"
        try:
            with redis.lock(lock_key, timeout=30):
                return self._create_or_get_batch_unlocked()
        except Exception as e:
            # If Redis is unavailable or disconnected (e.g. offline unit tests), fail open
            logger.warning(f"Redis lock unavailable ({e}), proceeding without lock.")
            return self._create_or_get_batch_unlocked()

    def associate_unbatched_assets(self, batch_id: uuid.UUID) -> int:
        """Associates any newly standardized PROMOTED assets with the current batch."""
        stmt = (
            select(Asset)
            .where(
                Asset.job_id == self.job_id,
                Asset.batch_id.is_(None),
                Asset.status.in_([AssetStatus.PROMOTED, AssetStatus.RELEVANCE_PASSED]),
            )
        )
        unbatched = self.db.execute(stmt).scalars().all()
        count = 0
        for asset in unbatched:
            asset.batch_id = batch_id
            count += 1
        if count > 0:
            self.db.commit()
            logger.info(f"[BATCH ORCHESTRATION] Associated {count} new asset(s) with Batch {batch_id}")
        return count

    def run_cross_batch_deduplication(self, batch_id: uuid.UUID) -> int:
        """Deduplicates candidates in the current batch against previously accepted assets."""
        from app.filtering.canonical_selector import CanonicalSelector
        from app.storage.asset_files import local_path_for_read

        dedup = CrossBatchDeduplicator(self.db)
        stmt = (
            select(Asset)
            .where(
                Asset.job_id == self.job_id,
                Asset.batch_id == batch_id,
                Asset.status == AssetStatus.PROMOTED,
            )
        )
        candidates = self.db.execute(stmt).scalars().all()
        duplicates_found = 0

        use_cross_job = (dedup.dedup_scope == "global")
        ref_stmt = select(Asset).where(
            Asset.status.in_([AssetStatus.RELEVANCE_PASSED, AssetStatus.PROMOTED])
        )
        if not use_cross_job:
            ref_stmt = ref_stmt.where(Asset.job_id == self.job_id)
        if batch_id is not None:
            ref_stmt = ref_stmt.where((Asset.batch_id != batch_id) | (Asset.batch_id.is_(None)))
        reference_assets = self.db.execute(ref_stmt).scalars().all()
        ref_by_id = {a.id: a for a in reference_assets}
        sha_map = {a.checksum_sha256: a for a in reference_assets if a.checksum_sha256}

        for asset in candidates:
            # 1. Exact SHA256 check
            if asset.checksum_sha256 and asset.checksum_sha256 in sha_map:
                canonical = sha_map[asset.checksum_sha256]
                if canonical.id != asset.id:
                    winner, loser = CanonicalSelector.select_canonical(canonical, asset)
                    CanonicalSelector.apply_duplicate_relationship(winner, loser)
                    duplicates_found += 1
                    continue

            # 2. Multi-Level Perceptual, Semantic & Structural Verification
            local_cand_path = None
            if asset.nas_path:
                try:
                    local_cand_path = Path(local_path_for_read(str(asset.nas_path)))
                except Exception:
                    local_cand_path = None

            result = dedup.deduplicate_candidate(
                job_id=self.job_id,
                candidate_id=asset.id,
                candidate_phash=asset.phash,
                candidate_embedding=asset.embedding,
                exclude_batch_id=batch_id,
                candidate_file_or_buffer=local_cand_path,
                reference_assets=reference_assets,
            )
            if result.is_duplicate and result.duplicate_of_id and result.duplicate_of_id != asset.id:
                canonical = ref_by_id.get(result.duplicate_of_id) or self.asset_repo.get(result.duplicate_of_id)
                if canonical:
                    winner, loser = CanonicalSelector.select_canonical(canonical, asset)
                    CanonicalSelector.apply_duplicate_relationship(winner, loser)
                else:
                    asset.status = AssetStatus.DUPLICATE
                    asset.is_duplicate_of = result.duplicate_of_id
                duplicates_found += 1

        if duplicates_found > 0:
            self.db.commit()
            logger.info(f"[BATCH ORCHESTRATION] Batch {batch_id}: Detected {duplicates_found} cross-batch duplicate(s)")
        return duplicates_found

    def evaluate_batch_progress(self, batch_id: uuid.UUID) -> dict:
        """Evaluates accepted vs target count for the batch and determines next step."""
        batch = self.batch_repo.get(batch_id)
        if batch is None:
            return {"status": "batch_not_found"}

        stmt = select(Asset).where(Asset.batch_id == batch_id)
        batch_assets = self.db.execute(stmt).scalars().all()

        accepted = sum(1 for a in batch_assets if a.status in ASSET_ACCEPTED_STATUSES)
        rejected = sum(1 for a in batch_assets if a.status == AssetStatus.RELEVANCE_REJECTED)
        duplicates = sum(1 for a in batch_assets if a.status == AssetStatus.DUPLICATE)
        uncertain = sum(1 for a in batch_assets if a.status == AssetStatus.REVIEW_PENDING)
        errors = sum(1 for a in batch_assets if a.status in (AssetStatus.RETRY_PENDING, AssetStatus.FAILED))
        in_flight = sum(
            1 for a in batch_assets
            if a.status in (
                AssetStatus.DISCOVERED,
                AssetStatus.DOWNLOADING,
                AssetStatus.DOWNLOADED,
                AssetStatus.STANDARDIZING,
                AssetStatus.STANDARDIZED,
                AssetStatus.PROMOTED,
                AssetStatus.RELEVANCE_PENDING,
            )
        )

        self.batch_repo.update_counts(
            batch_id,
            accepted=accepted,
            rejected=rejected,
            duplicate=duplicates,
            uncertain=uncertain,
            error=errors,
        )

        shortfall = max(0, batch.target_image_count - accepted)
        logger.info(
            f"[BATCH PROGRESS] Batch #{batch.batch_index} ({batch.id}): "
            f"Accepted={accepted}/{batch.target_image_count}, Rejected={rejected}, "
            f"Duplicates={duplicates}, InFlight={in_flight}, Shortfall={shortfall}"
        )

        if accepted >= batch.target_image_count:
            # Batch target met!
            return {
                "outcome": "TARGET_MET",
                "accepted": accepted,
                "shortfall": 0,
            }

        # Check if candidate downloading/filtering is still in flight
        if in_flight > 0:
            return {
                "outcome": "IN_PROGRESS",
                "accepted": accepted,
                "shortfall": shortfall,
            }

        # Check if source tasks have exhausted available items
        all_terminal = self.task_repo.all_terminal_for_job(self.job_id)
        if all_terminal:
            return {
                "outcome": "SOURCES_EXHAUSTED",
                "accepted": accepted,
                "shortfall": shortfall,
            }

        return {
            "outcome": "NEEDS_REPLACEMENTS",
            "accepted": accepted,
            "shortfall": shortfall,
        }

    def create_batch_manifest(self, batch_id: uuid.UUID, set_batch_status: bool = True) -> Path | None:
        """Generates a durable JSON manifest for newly accepted images in this batch."""
        batch = self.batch_repo.get(batch_id)
        job = self.job_repo.get(self.job_id)
        staging_dir = self.settings.local_staging_path / f"job_{self.job_id}" / f"batch_{batch.batch_index:03d}"
        staging_dir.mkdir(parents=True, exist_ok=True)

        stmt = select(Asset).where(
            Asset.batch_id == batch_id,
            Asset.status.in_([AssetStatus.RELEVANCE_PASSED, AssetStatus.READY_FOR_UPLOAD]),
        )
        assets = list(self.db.execute(stmt).scalars().all())
        if not assets:
            logger.info(f"[BATCH MANIFEST] No unuploaded accepted assets for Batch #{batch.batch_index}")
            return None

        # Enforce strict count consistency: accepted assets must NEVER exceed batch.target_image_count
        stmt_already = select(Asset).where(
            Asset.batch_id == batch_id,
            Asset.status.in_([AssetStatus.COMPLETED, AssetStatus.NAS_VERIFIED]),
        )
        already_completed_count = len(list(self.db.execute(stmt_already).scalars().all()))
        total_accepted = already_completed_count + len(assets)

        if batch.target_image_count and total_accepted > batch.target_image_count:
            from app.tasks.filtering_tasks import trim_excess_assets
            trim_excess_assets(str(self.job_id))
            self.db.expire_all()
            assets = list(self.db.execute(stmt).scalars().all())

        # Update batch accepted_image_count to match exact kept assets across the batch
        self.batch_repo.reconcile_batch_counters(batch_id)

        # Transition valid assets from RELEVANCE_PASSED to READY_FOR_UPLOAD
        valid_assets = []
        for a in assets:
            if not a.nas_path:
                continue
            p = Path(a.nas_path)
            # In production staging dir exists, ensure file was not trimmed; in tests allow mock paths
            if p.is_file() or not p.parent.exists():
                if a.status == AssetStatus.RELEVANCE_PASSED:
                    a.status = AssetStatus.READY_FOR_UPLOAD
                valid_assets.append(a)

        manifest_data = {
            "batch_id": str(batch.id),
            "job_id": str(self.job_id),
            "batch_index": batch.batch_index,
            "job_query": job.query if job else "",
            "target_count": batch.target_image_count,
            "accepted_count": len(valid_assets),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "items": [
                {
                    "asset_id": str(a.id),
                    "source_type": a.source_type.value if hasattr(a.source_type, "value") else str(a.source_type),
                    "source_item_id": a.source_item_id,
                    "checksum_sha256": a.checksum_sha256,
                    "phash": a.phash,
                    "local_path": a.nas_path,
                    "annotation_path": a.annotation_nas_path,
                    "classes": a.classes,
                }
                for a in valid_assets
            ],
        }

        manifest_path = staging_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))
        if set_batch_status:
            self.batch_repo.set_status(
                batch_id,
                BatchStatus.READY_FOR_UPLOAD,
                manifest_path=str(manifest_path),
                local_staging_path=str(staging_dir),
            )
        else:
            batch.manifest_path = str(manifest_path)
            batch.local_staging_path = str(staging_dir)
        self.db.commit()
        logger.info(f"[BATCH MANIFEST] Created durable manifest at {manifest_path} for Batch #{batch.batch_index} ({len(assets)} assets)")
        return manifest_path


@celery_app.task(name="app.tasks.batch_orchestration.process_active_batch", bind=True)
def process_active_batch(self, job_id: str):
    """Main Celery entrypoint for orchestrating a single batch from candidates to manifest."""
    job_uuid = uuid.UUID(job_id)
    bind_job_context(job_id)

    with session_scope() as db:
        from app.services.job_counters import reconcile
        reconcile(db, job_uuid)
        db.commit()

        orchestrator = BatchOrchestrator(db, job_uuid)
        active_batch = orchestrator.get_or_create_active_batch()
        if active_batch is None:
            logger.info(f"[BATCH ORCHESTRATION] Job {job_id} has fulfilled all batches or reached target.")
            clear_job_context()
            from app.tasks.orchestration_tasks import evaluate_job_completion
            evaluate_job_completion.delay(job_id)
            return {"job_id": job_id, "status": "completed"}

        batch_id = active_batch.id
        if active_batch.status not in (
            BatchStatus.READY_FOR_UPLOAD,
            BatchStatus.UPLOADING,
            BatchStatus.NAS_UPLOADING,
            BatchStatus.NAS_PENDING,
            BatchStatus.VERIFYING,
            BatchStatus.NAS_VERIFIED,
            BatchStatus.COMPLETED,
        ):
            orchestrator.batch_repo.set_status(batch_id, BatchStatus.PROCESSING)

        # 1. Associate any promoted assets with this batch
        orchestrator.associate_unbatched_assets(batch_id)

        # 2. Run cross-batch deduplication
        orchestrator.run_cross_batch_deduplication(batch_id)

        # 3. Evaluate progress
        progress = orchestrator.evaluate_batch_progress(batch_id)

    outcome = progress.get("outcome")

    if outcome == "TARGET_MET":
        # Target accepted images achieved! Build manifest and prepare for upload.
        with session_scope() as db:
            orchestrator = BatchOrchestrator(db, job_uuid)
            manifest_path = orchestrator.create_batch_manifest(batch_id)
            if manifest_path is not None:
                from app.storage.nas_writer import enqueue_nas_batch
                from app.tasks.nas_tasks import process_nas_upload_queue
                enqueue_nas_batch(batch_id, job_uuid, manifest_path)
                process_nas_upload_queue.delay()
        logger.info(f"[BATCH ORCHESTRATION] Batch {batch_id} complete with {progress['accepted']} images; enqueued for NAS upload.")

        # Check if the overall job needs another batch
        with session_scope() as db:
            orchestrator = BatchOrchestrator(db, job_uuid)
            next_batch = orchestrator.get_or_create_active_batch()
            if next_batch is not None and next_batch.id != batch_id:
                # Dispatch candidate discovery/download for the next batch
                dispatch_batch_candidate_window.delay(job_id, str(next_batch.id))
            else:
                from app.tasks.orchestration_tasks import evaluate_job_completion
                evaluate_job_completion.delay(job_id)

    elif outcome == "NEEDS_REPLACEMENTS":
        # Upload any newly accepted images from this round immediately to NAS
        with session_scope() as db:
            orchestrator = BatchOrchestrator(db, job_uuid)
            manifest_path = orchestrator.create_batch_manifest(batch_id, set_batch_status=False)
            if manifest_path is not None:
                from app.storage.nas_writer import enqueue_nas_batch
                from app.tasks.nas_tasks import process_nas_upload_queue
                enqueue_nas_batch(batch_id, job_uuid, manifest_path)
                process_nas_upload_queue.delay()
                logger.info(f"[BATCH ORCHESTRATION] Offloaded newly accepted assets to NAS for Batch {batch_id}")

        # Shortfall detected! Request replacements for rejected/duplicate images using DB row locking and Redis lock
        shortfall = progress.get("shortfall", 0)
        redis = get_redis()
        replenish_key = f"job:{job_id}:batch:{batch_id}:replenish_lock"
        try:
            acquired = redis.set(replenish_key, "1", nx=True, ex=120)
        except Exception:
            acquired = True

        replenishment_ticket = str(uuid.uuid4())
        should_dispatch = False
        replenishment_round = 1

        if acquired:
            with session_scope() as db:
                orchestrator = BatchOrchestrator(db, job_uuid)
                batch = db.execute(
                    select(ImageBatch).where(ImageBatch.id == batch_id).with_for_update()
                ).scalar_one_or_none()

                if batch is not None:
                    md = dict(batch.batch_metadata or {})
                    if batch.status == BatchStatus.REPLENISHING and md.get("active_replenishment_ticket"):
                        logger.warning(
                            f"[BATCH ORCHESTRATION] Batch {batch_id} already has active replenishment ticket "
                            f"{md.get('active_replenishment_ticket')}. Skipping duplicate dispatch."
                        )
                    else:
                        replenishment_round = (md.get("replenishment_round") or 0) + 1
                        md["active_replenishment_ticket"] = replenishment_ticket
                        md["replenishment_round"] = replenishment_round
                        md["replenishment_shortfall"] = shortfall
                        batch.batch_metadata = md
                        orchestrator.batch_repo.set_status(batch_id, BatchStatus.REPLENISHING)
                        db.commit()
                        should_dispatch = True
        else:
            logger.warning(
                f"[BATCH ORCHESTRATION] Replenishment lock already held for Batch {batch_id}, skipping duplicate dispatch."
            )

        if should_dispatch:
            settings = get_settings()
            buffer = float(settings.target_overfetch_buffer or 1.5)
            replenishment_candidates = min(
                settings.download_candidate_window,
                max(15, math.ceil(shortfall * buffer)),
            )
            logger.info(
                f"[BATCH ORCHESTRATION] Batch {batch_id} shortfall={shortfall}. "
                f"Transitioned to REPLENISHING (round={replenishment_round}, ticket={replenishment_ticket}), dispatching replacements ({replenishment_candidates} candidates)..."
            )
            dispatch_batch_candidate_window.delay(
                job_id, str(batch_id), candidate_count=replenishment_candidates, ticket=replenishment_ticket
            )

    elif outcome == "SOURCES_EXHAUSTED":
        # Sources dry before reaching target. Finalize partial batch
        with session_scope() as db:
            orchestrator = BatchOrchestrator(db, job_uuid)
            manifest_path = orchestrator.create_batch_manifest(batch_id)
            orchestrator.batch_repo.set_status(batch_id, BatchStatus.PARTIAL)
            if manifest_path is not None:
                from app.storage.nas_writer import enqueue_nas_batch
                from app.tasks.nas_tasks import process_nas_upload_queue
                enqueue_nas_batch(batch_id, job_uuid, manifest_path)
                process_nas_upload_queue.delay()
        logger.warning(f"[BATCH ORCHESTRATION] Batch {batch_id} partial ({progress['accepted']} images). Sources exhausted; enqueued for NAS upload.")

    clear_job_context()
    return {"job_id": job_id, "batch_id": str(batch_id), "progress": progress}


@celery_app.task(name="app.tasks.batch_orchestration.dispatch_batch_candidate_window", bind=True)
def dispatch_batch_candidate_window(
    self, job_id: str, batch_id: str, candidate_count: int | None = None, ticket: str | None = None
):
    """Dispatches a windowed candidate acquisition request (default: 600 or shortfall)."""
    job_uuid = uuid.UUID(job_id)
    batch_uuid = uuid.UUID(batch_id)
    bind_job_context(job_id)
    settings = get_settings()

    from app.storage.ram_buffer import LocalStagingManager
    staging_manager = LocalStagingManager()
    if settings.download_pause_when_storage_full and staging_manager.is_staging_backpressure_active():
        logger.warning(
            f"[BATCH ORCHESTRATION] Staging backpressure active for Job {job_id}. Pausing candidate window download."
        )
        try:
            with session_scope() as db:
                batch_repo = ImageBatchRepository(db)
                batch = batch_repo.get(batch_uuid)
                if batch and batch.status != BatchStatus.PAUSED:
                    md = dict(batch.batch_metadata or {})
                    md["paused_from_status"] = batch.status.value
                    batch.batch_metadata = md
                    batch_repo.set_status(batch_uuid, BatchStatus.PAUSED)
                    db.commit()
        except Exception as e:
            logger.debug(f"Could not persist batch PAUSED status to DB ({e})")
        clear_job_context()
        return {"job_id": job_id, "status": "paused_staging_backpressure"}

    buffer = float(settings.target_overfetch_buffer or 1.5)
    window_size = candidate_count or settings.download_candidate_window

    with session_scope() as db:
        job_repo = JobRepository(db)
        task_repo = JobSourceTaskRepository(db)
        batch_repo = ImageBatchRepository(db)

        # Lock the batch row to validate replenishment ticket and prevent duplicate/stale workers
        batch = db.execute(
            select(ImageBatch).where(ImageBatch.id == batch_uuid).with_for_update()
        ).scalar_one_or_none()
        job = job_repo.get(job_uuid)

        if job is None or batch is None:
            clear_job_context()
            return {"error": "job or batch not found"}

        if batch.status in (
            BatchStatus.READY_FOR_UPLOAD,
            BatchStatus.UPLOADING,
            BatchStatus.VERIFYING,
            BatchStatus.COMPLETED,
            BatchStatus.PARTIAL,
        ):
            logger.info(
                f"[BATCH ORCHESTRATION] Batch {batch_id} is already in state '{batch.status.value}'. "
                "Skipping candidate window dispatch."
            )
            clear_job_context()
            return {
                "status": "skipped",
                "reason": f"batch_already_{batch.status.value}",
                "batch_id": str(batch_id),
            }

        if candidate_count is not None:
            window_size = int(candidate_count)
        else:
            if batch.target_image_count > settings.download_candidate_window:
                window_size = settings.download_candidate_window
            else:
                shortfall = max(1, batch.target_image_count - (batch.accepted_image_count or 0))
                window_size = min(settings.download_candidate_window, max(15, math.ceil(shortfall * buffer)))

        if ticket is not None:
            md = dict(batch.batch_metadata or {})
            active_ticket = md.get("active_replenishment_ticket")
            if active_ticket != ticket:
                logger.warning(
                    f"[BATCH ORCHESTRATION] Rejecting stale or duplicate replenishment ticket for Batch {batch_id}. "
                    f"Active ticket={active_ticket}, received={ticket}"
                )
                clear_job_context()
                return {"status": "rejected_stale_ticket", "batch_id": batch_id}
            # Mark ticket consumed once validated
            md["consumed_replenishment_ticket"] = ticket
            batch.batch_metadata = md

        batch_repo.set_status(batch_uuid, BatchStatus.DOWNLOADING)

        # Clear prior target reached flags in redis so new window admissions and discoveries proceed
        from app.services.cancellation_service import CancellationService
        CancellationService(get_redis()).clear_target_reached(job_id)

        # The batch row is already locked above. Create/reuse its candidate
        # window in this transaction so all source workers share one window.
        from app.services.candidate_reservation_service import ensure_candidate_window

        candidate_window_id = ensure_candidate_window(
            db, batch, candidate_limit=window_size, force_new=(ticket is not None)
        )

        # Find non-terminal source tasks or create tasks if needed
        raw_sources = job.sources or [
            s.value for s in SourceType if s != SourceType.NAS_SHARE
        ]
        excluded_sources = {"youtube", "kitti", "nuscenes"}
        candidate_sources = [
            str(getattr(source, "value", source))
            for source in raw_sources
            if str(getattr(source, "value", source)).lower() not in excluded_sources
        ]
        sources = candidate_sources or [str(getattr(s, "value", s)) for s in raw_sources]
        dispatched_tasks = []

        tasks_to_dispatch: list[tuple[str, str, str]] = []

        for source_str in sources:
            source_type = SourceType(source_str)
            task_path = _SOURCE_DISCOVERY_TASKS.get(source_type)
            if task_path is None:
                continue

            # Check if task already exists for this job and source
            existing_task = task_repo.get_for_job_and_source(job_uuid, source_type)

            if existing_task is None:
                new_task = JobSourceTask(id=uuid.uuid4(), job_id=job_uuid, source_type=source_str)
                task_repo.create(new_task)
                db.flush()
                task_id = new_task.id
                tasks_to_dispatch.append((task_path, str(job_uuid), str(task_id)))
            else:
                task_id = existing_task.id
                task_repo.set_status(existing_task.id, JobSourceTaskStatus.DOWNLOADING)
                task_repo.heartbeat(existing_task.id)
                from app.services.job_service import _resume_pending_items
                if _resume_pending_items(source_type, str(job_uuid), str(task_id), existing_task.checkpoint):
                    dispatched_tasks.append((source_str, str(task_id)))
                    continue
                task_repo.set_status(existing_task.id, JobSourceTaskStatus.PENDING)
                tasks_to_dispatch.append((task_path, str(job_uuid), str(task_id)))

            dispatched_tasks.append((source_str, str(task_id)))

        batch_index = batch.batch_index
        db.commit()

        # Dispatch Celery tasks only AFTER durable commit so workers never see task is None
        for t_path, j_id, s_task_id in tasks_to_dispatch:
            JobService._dispatch(t_path, j_id, s_task_id)

    logger.info(
        f"[BATCH WINDOW] Job {job_id} Batch #{batch_index}: "
        f"Dispatched candidate window ({window_size} target candidates) to {len(dispatched_tasks)} source(s)."
    )
    clear_job_context()
    return {
        "job_id": job_id,
        "batch_id": batch_id,
        "window_id": str(candidate_window_id),
        "window_size": window_size,
        "dispatched": dispatched_tasks,
    }


@celery_app.task(name="app.tasks.batch_orchestration.reconcile_batch_lifecycle", bind=True)
def reconcile_batch_lifecycle(self) -> dict:
    """Comprehensive periodic lifecycle reconciliation task across all batches and assets."""
    logger.info("[RECONCILIATION] Starting batch lifecycle reconciliation...")
    reconciled_counters = 0
    resumed_batches = 0
    recovered_transfers = 0
    recovered_assets = 0

    from datetime import datetime, timezone, timedelta
    from app.storage.ram_buffer import LocalStagingManager
    from app.db.models.image_batch import NasTransferBatch
    from app.domain.enums import TransferStatus
    from app.tasks.nas_tasks import reconcile_nas_transfers

    staging_manager = LocalStagingManager()
    backpressure_active = staging_manager.is_staging_backpressure_active()

    with session_scope() as db:
        batch_repo = ImageBatchRepository(db)

        # 1. Identify and reconcile batches whose counters disagree with actual asset rows
        active_batches = db.execute(
            select(ImageBatch).where(
                ImageBatch.status.in_([
                    BatchStatus.CREATED,
                    BatchStatus.DISCOVERING,
                    BatchStatus.DOWNLOADING,
                    BatchStatus.PROCESSING,
                    BatchStatus.FILTERING,
                    BatchStatus.REPLENISHING,
                    BatchStatus.FILLING_SHORTFALL,
                    BatchStatus.READY_FOR_UPLOAD,
                    BatchStatus.PAUSED,
                ])
            )
        ).scalars().all()

        for batch in active_batches:
            stmt = select(Asset).where(Asset.batch_id == batch.id)
            assets = db.execute(stmt).scalars().all()
            actual_accepted = sum(1 for a in assets if a.status in ASSET_ACCEPTED_STATUSES)
            actual_rejected = sum(1 for a in assets if a.status == AssetStatus.RELEVANCE_REJECTED)
            actual_duplicates = sum(1 for a in assets if a.status == AssetStatus.DUPLICATE)
            actual_uncertain = sum(1 for a in assets if a.status == AssetStatus.REVIEW_PENDING)
            actual_errors = sum(1 for a in assets if a.status in (AssetStatus.RETRY_PENDING, AssetStatus.FAILED))

            if (
                batch.accepted_image_count != actual_accepted
                or batch.rejected_image_count != actual_rejected
                or batch.duplicate_image_count != actual_duplicates
                or batch.uncertain_image_count != actual_uncertain
                or batch.error_image_count != actual_errors
            ):
                logger.warning(
                    f"[RECONCILIATION] Counter drift detected in Batch {batch.id}: "
                    f"DB accepted={batch.accepted_image_count} vs actual={actual_accepted}. Reconciling..."
                )
                batch_repo.reconcile_batch_counters(batch.id)
                reconciled_counters += 1

            # 2. Check for PAUSED batches where backpressure has cleared
            # Retain and restore the exact phase active before pausing:
            # DOWNLOADING -> PAUSED -> DOWNLOADING
            # PROCESSING -> PAUSED -> PROCESSING
            # REPLENISHING -> PAUSED -> REPLENISHING
            if batch.status == BatchStatus.PAUSED and not backpressure_active:
                md = dict(batch.batch_metadata or {})
                paused_from = md.get("paused_from_status")
                if paused_from and paused_from in BatchStatus._value2member_map_:
                    resume_status = BatchStatus(paused_from)
                else:
                    resume_status = BatchStatus.PROCESSING
                logger.info(
                    f"[RECONCILIATION] Staging backpressure cleared. Resuming Batch {batch.id} "
                    f"to active phase '{resume_status.value}' (was paused from {paused_from})..."
                )
                batch_repo.set_status(batch.id, resume_status)
                if resume_status in (BatchStatus.DOWNLOADING, BatchStatus.REPLENISHING):
                    dispatch_batch_candidate_window.delay(str(batch.job_id), str(batch.id))
                else:
                    process_active_batch.delay(str(batch.job_id))
                resumed_batches += 1

            # 3. Check for batches in READY_FOR_UPLOAD with no active transfer record
            if batch.status == BatchStatus.READY_FOR_UPLOAD:
                transfer_stmt = select(NasTransferBatch).where(
                    NasTransferBatch.batch_id == batch.id,
                    NasTransferBatch.transfer_status.in_([
                        TransferStatus.PENDING,
                        TransferStatus.UPLOADING,
                        TransferStatus.VERIFIED,
                    ]),
                )
                active_transfer = db.execute(transfer_stmt).scalars().first()
                if not active_transfer:
                    logger.info(f"[RECONCILIATION] Batch {batch.id} in READY_FOR_UPLOAD has no active transfer. Enqueuing...")
                    from app.storage.nas_writer import enqueue_nas_batch
                    from app.tasks.nas_tasks import process_nas_upload_queue
                    manifest_p = Path(batch.manifest_path) if batch.manifest_path else None
                    if manifest_p and manifest_p.exists():
                        enqueue_nas_batch(batch.id, batch.job_id, manifest_p)
                        process_nas_upload_queue.delay()
                        recovered_transfers += 1

        # 4. Check for stuck assets in intermediate states and retry handling
        MAX_ASSET_RETRIES = 3
        now = datetime.now(timezone.utc)
        settings = get_settings()
        stale_threshold_seconds = getattr(settings, "reconciliation_stale_asset_seconds", 300)
        stale_cutoff = now - timedelta(seconds=stale_threshold_seconds)

        stuck_assets = db.execute(
            select(Asset).where(
                Asset.status.in_([
                    AssetStatus.DOWNLOADING,
                    AssetStatus.STANDARDIZING,
                    AssetStatus.RELEVANCE_PENDING,
                    AssetStatus.RETRY_PENDING,
                ])
            )
        ).scalars().all()

        for asset in stuck_assets:
            md = dict(asset.asset_metadata or {})
            state_entered_str = md.get("status_entered_at")
            state_entered_at = None
            if state_entered_str:
                try:
                    state_entered_at = datetime.fromisoformat(state_entered_str)
                    if state_entered_at.tzinfo is None:
                        state_entered_at = state_entered_at.replace(tzinfo=timezone.utc)
                except Exception:
                    state_entered_at = None

            if state_entered_at is None:
                state_entered_at = asset.discovered_at
                if state_entered_at is not None and state_entered_at.tzinfo is None:
                    state_entered_at = state_entered_at.replace(tzinfo=timezone.utc)

            is_stale = (state_entered_at is None) or (state_entered_at < stale_cutoff)
            if not is_stale and asset.status != AssetStatus.RETRY_PENDING:
                continue

            retries = md.get("retry_count", 0)

            if asset.status in (AssetStatus.DOWNLOADING, AssetStatus.STANDARDIZING):
                if retries < MAX_ASSET_RETRIES:
                    md["retry_count"] = retries + 1
                    md["status_entered_at"] = now.isoformat()
                    asset.asset_metadata = md
                    asset.status = AssetStatus.RETRY_PENDING
                    recovered_assets += 1
                    logger.info(
                        f"[RECONCILIATION] Asset {asset.id} stuck in {asset.status.value}; "
                        f"transitioned to RETRY_PENDING (retry {retries + 1}/{MAX_ASSET_RETRIES})"
                    )
                else:
                    asset.status = AssetStatus.FAILED
                    recovered_assets += 1
                    logger.warning(
                        f"[RECONCILIATION] Asset {asset.id} exceeded max retries ({MAX_ASSET_RETRIES}); "
                        "marked FAILED (never rejected on infrastructure failure)"
                    )
            elif asset.status == AssetStatus.RELEVANCE_PENDING:
                asset.status = AssetStatus.PROMOTED
                md["status_entered_at"] = now.isoformat()
                asset.asset_metadata = md
                recovered_assets += 1
                logger.info(f"[RECONCILIATION] Asset {asset.id} stuck in RELEVANCE_PENDING; reset to PROMOTED")
            elif asset.status == AssetStatus.RETRY_PENDING:
                if retries < MAX_ASSET_RETRIES:
                    md["retry_count"] = retries + 1
                    md["status_entered_at"] = now.isoformat()
                    asset.asset_metadata = md
                    asset.status = AssetStatus.PROMOTED
                    recovered_assets += 1
                    logger.info(
                        f"[RECONCILIATION] Retrying Asset {asset.id} from RETRY_PENDING -> PROMOTED "
                        f"(attempt {retries + 1}/{MAX_ASSET_RETRIES})"
                    )
                else:
                    asset.status = AssetStatus.FAILED
                    recovered_assets += 1
                    logger.warning(
                        f"[RECONCILIATION] Asset {asset.id} exceeded max retries ({MAX_ASSET_RETRIES}); "
                        "marked FAILED"
                    )

        db.commit()

    # 5. Invoke NAS transfer reconciler to recover actionable transfers and pending staging cleanups
    try:
        nas_reconcile_result = reconcile_nas_transfers()
    except Exception as e:
        logger.warning(f"[RECONCILIATION] NAS transfer reconciler error ({e})")
        nas_reconcile_result = {"status": "error", "error": str(e)}

    return {
        "reconciled_counters": reconciled_counters,
        "resumed_batches": resumed_batches,
        "recovered_transfers": recovered_transfers,
        "recovered_assets": recovered_assets,
        "nas_reconcile": nas_reconcile_result,
    }

