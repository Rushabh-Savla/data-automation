"""Regression tests for Pipeline Audit fixes.

Covers:
1. Accepted image double counting and tier progression in check_and_advance_job().
2. Job startup transaction-safe Celery task dispatch (after-commit execution).
3. NAS upload safety, active transfer wait for partial completion, lock respect, and non-marking of in-flight assets as FAILED.
4. Duplicate NAS offload prevention (excluding batched assets).
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from celery.exceptions import Retry

from app.db.models.asset import Asset
from app.db.models.candidate_window import CandidateWindow
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import (
    AssetStatus,
    BatchStatus,
    JobSourceTaskStatus,
    JobStatus,
    SourceSelectionMode,
    SourceType,
    TransferStatus,
)
from app.services.job_counters import reconcile
from app.services.job_service import JobService
from app.tasks.nas_tasks import (
    consolidate_job_dataset_on_nas,
    schedule_offload_for_job,
)
from app.tasks.orchestration_tasks import (
    check_and_advance_job,
    evaluate_job_completion,
)


class _FakeRedis:
    def __init__(self):
        self._data = {}
        self._lists = {}
        self._sets = {}

    def get(self, key):
        return self._data.get(key)

    def exists(self, key):
        return key in self._data or key in self._lists or key in self._sets

    def sadd(self, key, *members):
        self._sets.setdefault(key, set()).update(members)

    def smembers(self, key):
        return self._sets.get(key, set())

    def srem(self, key, *members):
        s = self._sets.get(key, set())
        for m in members:
            s.discard(m)

    def llen(self, key):
        return len(self._lists.get(key, []))

    def publish(self, channel, message):
        return 1

    def lpop(self, key):
        lst = self._lists.get(key, [])
        return lst.pop(0) if lst else None

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self._data:
            return False
        self._data[key] = value
        return True

    def delete(self, *keys):
        for k in keys:
            self._data.pop(k, None)
            self._lists.pop(k, None)

    def rpush(self, key, *values):
        self._lists.setdefault(key, []).extend(values)

    def lrange(self, key, start, end):
        lst = self._lists.get(key, [])
        if end == -1:
            return lst[start:]
        return lst[start : end + 1]

    def lock(self, name, timeout=None, blocking=True):
        class _Lock:
            def __init__(self, r, k):
                self.r = r
                self.k = k

            def acquire(self, blocking=False):
                if self.k in self.r._data:
                    return False
                self.r._data[self.k] = "1"
                return True

            def release(self):
                self.r._data.pop(self.k, None)

            def __enter__(self):
                self.acquire(blocking=True)
                return self

            def __exit__(self, *args):
                self.release()

        return _Lock(self, name)


class _SessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.session.rollback()
        else:
            self.session.commit()


# =========================================================================
# PHASE 3: ACCEPTED IMAGE COUNTING & TIER PROGRESSION REGRESSION TESTS
# =========================================================================

class TestDoubleCountingRegression:
    def test_target_1000_with_550_accepted_does_not_satisfy_target(self, db_session, monkeypatch):
        """Regression test: 550 accepted assets must not be counted twice (550+550=1100).

        Target is 1000, so effective_accepted must be 550 and target_reached must be False.
        """
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_double_count_550",
            query="car",
            status=JobStatus.RUNNING,
            target_count=1000,
            accepted_count=550,
            config={
                "_orchestration_tier": 1,
                "_orchestration_pending_tiers": [["youtube"], ["kaggle"]],
            },
        )
        db_session.add(job)

        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.COMPLETED,
            accepted_count=550,
        )
        db_session.add(task)

        # 550 RELEVANCE_PASSED assets
        for i in range(550):
            db_session.add(
                Asset(
                    id=uuid.uuid4(),
                    job_id=job_id,
                    job_source_task_id=task.id,
                    source_type=SourceType.WEB,
                    source_item_id=f"web_{i}",
                    status=AssetStatus.RELEVANCE_PASSED,
                )
            )
        db_session.commit()

        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", lambda: _SessionContext(db_session))

        eval_completion_dispatched = []
        monkeypatch.setattr(
            "app.tasks.orchestration_tasks.evaluate_job_completion.delay",
            lambda jid: eval_completion_dispatched.append(jid),
        )

        resumed_tasks = []
        monkeypatch.setattr(
            "app.tasks.orchestration_tasks._resume_task",
            lambda jid, tid, st, cp: resumed_tasks.append((st, tid)),
        )
        dispatched_tasks = []
        monkeypatch.setattr(
            "app.tasks.celery_app.celery_app.send_task",
            lambda name, args=None, **kwargs: dispatched_tasks.append((name, args)),
        )

        check_and_advance_job(str(job_id))

        # Because 550 < 1000, evaluate_job_completion must NOT be called for target reached
        assert len(eval_completion_dispatched) == 0

        # Subsequent tier (youtube) MUST be scheduled because target (1000) was NOT reached!
        db_session.refresh(job)
        assert job.config.get("_orchestration_tier") == 2

    def test_target_reached_exactly(self, db_session, monkeypatch):
        """When accepted_count reaches exactly target_count (1000 == 1000), target_reached is True."""
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_exact_1000",
            query="car",
            status=JobStatus.RUNNING,
            target_count=1000,
            accepted_count=1000,
            config={},
        )
        db_session.add(job)
        db_session.commit()

        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", lambda: _SessionContext(db_session))

        eval_completion_dispatched = []
        monkeypatch.setattr(
            "app.tasks.orchestration_tasks.evaluate_job_completion.delay",
            lambda jid: eval_completion_dispatched.append(jid),
        )

        check_and_advance_job(str(job_id))
        assert len(eval_completion_dispatched) == 1

    def test_target_exceeded(self, db_session, monkeypatch):
        """When accepted_count exceeds target_count (1050 > 1000), target_reached is True."""
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_exceeded_1000",
            query="car",
            status=JobStatus.RUNNING,
            target_count=1000,
            accepted_count=1050,
            config={},
        )
        db_session.add(job)
        db_session.commit()

        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", lambda: _SessionContext(db_session))

        eval_completion_dispatched = []
        monkeypatch.setattr(
            "app.tasks.orchestration_tasks.evaluate_job_completion.delay",
            lambda jid: eval_completion_dispatched.append(jid),
        )

        check_and_advance_job(str(job_id))
        assert len(eval_completion_dispatched) == 1

    def test_multiple_relevance_passed_statuses_not_double_counted(self, db_session):
        """reconcile() counts each RELEVANCE_ACCEPTED status exactly once."""
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_multi_status",
            query="car",
            status=JobStatus.RUNNING,
            target_count=500,
            accepted_count=0,
            config={},
        )
        db_session.add(job)

        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.DOWNLOADING,
        )
        db_session.add(task)

        # Insert 1 asset in each accepted status
        statuses = [
            AssetStatus.RELEVANCE_PASSED,
            AssetStatus.READY_FOR_UPLOAD,
            AssetStatus.UPLOADING,
            AssetStatus.NAS_VERIFIED,
            AssetStatus.COMPLETED,
        ]
        for s in statuses:
            db_session.add(
                Asset(
                    id=uuid.uuid4(),
                    job_id=job_id,
                    job_source_task_id=task.id,
                    source_type=SourceType.WEB,
                    source_item_id=f"item_{s.value}",
                    status=s,
                )
            )
        db_session.commit()

        counts = reconcile(db_session, job_id)
        assert counts["accepted"] == 5
        db_session.refresh(job)
        assert job.accepted_count == 5


# =========================================================================
# PHASE 4: JOB STARTUP TRANSACTION SAFETY REGRESSION TESTS
# =========================================================================

class TestJobStartupTransactionSafety:
    def test_tasks_dispatched_after_commit_only(self, db_session, monkeypatch):
        """Celery tasks must NOT be dispatched before transaction commit, and MUST dispatch after commit."""
        import app.tasks.discovery_tasks

        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_tx_safety",
            query="dog",
            status=JobStatus.DRAFT,
            target_count=100,
            sources=[SourceType.WEB.value],
            config={},
        )
        db_session.add(job)
        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.PENDING,
        )
        db_session.add(task)
        db_session.commit()

        dispatched_tasks = []
        monkeypatch.setattr("app.services.job_service.get_redis", lambda: _FakeRedis())
        monkeypatch.setattr(
            "app.tasks.celery_app.celery_app.send_task",
            lambda name, args=None, **kwargs: dispatched_tasks.append((name, args)),
        )

        # Start a transaction and call start()
        db_session.begin()
        service = JobService(db_session)
        service.start(job_id)

        # BEFORE commit: nothing dispatched to Celery!
        assert len(dispatched_tasks) == 0

        # AFTER commit: task dispatched!
        db_session.commit()
        assert len(dispatched_tasks) == 1
        assert dispatched_tasks[0][0] == "app.tasks.discovery_tasks.discover_web"
        assert dispatched_tasks[0][1][0] == str(job_id)

    def test_tasks_cancelled_on_transaction_rollback(self, db_session, monkeypatch):
        """If transaction rolls back, deferred tasks are discarded and never sent."""
        import app.tasks.discovery_tasks

        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_tx_rollback",
            query="dog",
            status=JobStatus.DRAFT,
            target_count=100,
            sources=[SourceType.WEB.value],
            config={},
        )
        db_session.add(job)
        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.PENDING,
        )
        db_session.add(task)
        db_session.commit()

        dispatched_tasks = []
        monkeypatch.setattr("app.services.job_service.get_redis", lambda: _FakeRedis())
        monkeypatch.setattr(
            "app.tasks.celery_app.celery_app.send_task",
            lambda name, args=None, **kwargs: dispatched_tasks.append((name, args)),
        )

        db_session.begin()
        service = JobService(db_session)
        service.start(job_id)
        assert len(dispatched_tasks) == 0

        db_session.rollback()
        assert len(dispatched_tasks) == 0


# =========================================================================
# PHASE 1 & 2: NAS UPLOAD SAFETY & CONSOLIDATION REGRESSION TESTS
# =========================================================================

class TestNasConsolidationSafety:
    def test_consolidation_defers_when_active_transfers_exist(self, db_session, monkeypatch):
        """Consolidation must defer with status 'deferred' when active transfers exist."""
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_consolidation_safety",
            query="bike",
            status=JobStatus.COMPLETED,
            target_count=50,
            accepted_count=50,
            config={"nas_storage_path": "ai_data_collection/test"},
        )
        db_session.add(job)

        batch = ImageBatch(
            id=uuid.uuid4(),
            job_id=job_id,
            batch_index=1,
            target_image_count=50,
            status=BatchStatus.UPLOADING,
        )
        db_session.add(batch)

        transfer = NasTransferBatch(
            id=uuid.uuid4(),
            job_id=job_id,
            batch_id=batch.id,
            transfer_status=TransferStatus.UPLOADING,
            remote_path="test/batch_001",
        )
        db_session.add(transfer)
        db_session.commit()

        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.tasks.nas_tasks.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks.nas_tasks.session_scope", lambda: _SessionContext(db_session))
        monkeypatch.setattr("app.tasks.nas_tasks.is_storage_active", lambda: True)
        monkeypatch.setattr("app.tasks.nas_tasks.get_active_storage_params", lambda: MagicMock())

        with pytest.raises(Retry):
            consolidate_job_dataset_on_nas(str(job_id))

    def test_consolidation_defers_when_nas_writer_lock_held(self, db_session, monkeypatch):
        """Consolidation must defer when the NAS writer lock is held by an active transfer."""
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_writer_lock_respect",
            query="bike",
            status=JobStatus.COMPLETED,
            target_count=50,
            accepted_count=50,
            config={"nas_storage_path": "ai_data_collection/test"},
        )
        db_session.add(job)
        db_session.commit()

        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.tasks.nas_tasks.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks.nas_tasks.session_scope", lambda: _SessionContext(db_session))
        monkeypatch.setattr("app.tasks.nas_tasks.is_storage_active", lambda: True)
        monkeypatch.setattr("app.tasks.nas_tasks.get_active_storage_params", lambda: MagicMock())

        # Mock NasWriterLock.acquire to return False (lock held)
        monkeypatch.setattr("app.storage.nas_writer.NasWriterLock.acquire", lambda *a, **k: False)

        with pytest.raises(Retry):
            consolidate_job_dataset_on_nas(str(job_id))

    def test_partial_completion_waits_for_active_uploads(self, db_session, monkeypatch):
        """evaluate_job_completion must wait for active uploads even on partial completion."""
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_partial_wait",
            query="bike",
            status=JobStatus.RUNNING,
            target_count=100,
            accepted_count=40,  # partial!
            config={"nas_storage_path": "ai_data_collection/test"},
        )
        db_session.add(job)

        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.COMPLETED,
            accepted_count=40,
        )
        db_session.add(task)

        # Add asset so reconcile keeps accepted_count > 0 -> complete_partial
        asset = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=task.id,
            source_type=SourceType.WEB.value,
            source_item_id="item_1",
            status=AssetStatus.RELEVANCE_PASSED,
            original_name="sample.jpg",
            file_format="jpg",
        )
        db_session.add(asset)

        # Active transfer in flight
        transfer = NasTransferBatch(
            id=uuid.uuid4(),
            job_id=job_id,
            batch_id=uuid.uuid4(),
            transfer_status=TransferStatus.UPLOADING,
            remote_path="test/batch_001",
        )
        db_session.add(transfer)
        db_session.commit()

        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks._helpers.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", lambda: _SessionContext(db_session))
        monkeypatch.setattr("app.services.nas_config_service.is_storage_active", lambda: True)
        monkeypatch.setattr("app.tasks.nas_tasks.process_nas_upload_queue.delay", lambda *a, **k: None)
        monkeypatch.setattr("app.tasks.nas_tasks.reconcile_nas_transfers", lambda *a, **k: {})

        retried = []
        def fake_retry(self_obj, countdown=5, max_retries=120):
            retried.append(countdown)
            return Exception("retried")

        monkeypatch.setattr(
            "celery.app.task.Task.retry",
            fake_retry,
        )

        with pytest.raises(Exception, match="retried"):
            evaluate_job_completion(str(job_id))

        assert len(retried) == 1
        # Job must NOT have transitioned to terminal COMPLETED_PARTIAL yet!
        db_session.refresh(job)
        assert job.status == JobStatus.RUNNING

    def test_schedule_offload_excludes_batched_assets(self, db_session, monkeypatch):
        """schedule_offload_for_job must ignore assets that already belong to an ImageBatch."""
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            name="test_exclude_batched",
            query="cat",
            status=JobStatus.RUNNING,
            target_count=50,
            accepted_count=5,
            config={"nas_storage_path": "ai_data_collection/test"},
        )
        db_session.add(job)

        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.COMPLETED,
        )
        db_session.add(task)

        batch_id = uuid.uuid4()
        # Asset 1 has batch_id (batched)
        batched_asset = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=task.id,
            batch_id=batch_id,
            source_type=SourceType.WEB,
            source_item_id="item_1",
            status=AssetStatus.READY_FOR_UPLOAD,
            nas_path="/work/staging/1.jpg",
        )
        # Asset 2 has NO batch_id (unbatched)
        unbatched_asset = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=task.id,
            batch_id=None,
            source_type=SourceType.WEB,
            source_item_id="item_2",
            status=AssetStatus.READY_FOR_UPLOAD,
            nas_path="/work/staging/2.jpg",
        )
        db_session.add_all([batched_asset, unbatched_asset])
        db_session.commit()

        fake_redis = _FakeRedis()
        fake_redis.rpush(f"job:{job_id}:offload_pending", str(batched_asset.id), str(unbatched_asset.id))
        monkeypatch.setattr("app.tasks.nas_tasks.get_redis", lambda: fake_redis)
        monkeypatch.setattr("app.tasks.nas_tasks.session_scope", lambda: _SessionContext(db_session))
        monkeypatch.setattr("app.tasks.nas_tasks.is_storage_active", lambda: True)
        monkeypatch.setattr("app.tasks.nas_tasks._cooldown_active", lambda r: False)

        offloaded_chunks = []
        monkeypatch.setattr(
            "app.tasks.nas_tasks.offload_asset_batch.delay",
            lambda chunk: offloaded_chunks.append(chunk),
        )

        schedule_offload_for_job(str(job_id))

        # Only the unbatched asset should have been passed to offload_asset_batch
        assert len(offloaded_chunks) == 1
        assert offloaded_chunks[0] == [str(unbatched_asset.id)]
