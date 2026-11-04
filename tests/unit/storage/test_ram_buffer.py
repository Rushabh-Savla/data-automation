"""Unit tests for Stage 3: Bounded RAM Buffering, MemoryManager, and Local SSD Staging."""

import io
import os
import shutil
import threading
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image

from app.connectors.base import RawAssetDraft
from app.core.config import Settings
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.asset_repository import AssetRepository
from app.domain.enums import AssetStatus, JobSourceTaskStatus, SourceType
from app.services.standardization_service import StandardizationService
from app.storage.local_filesystem import LocalFilesystemStorage
from app.storage.ram_buffer import (
    DEFAULT_MAX_SPOOL_BYTES,
    LocalStagingManager,
    MemoryLease,
    MemoryManager,
    RAMBudgetExceededError,
    SpooledCandidateBuffer,
    StagingBackpressureError,
)


def _create_sample_jpeg_bytes(width: int = 100, height: int = 100, color: str = "blue") -> bytes:
    """Helper to generate valid JPEG image bytes in memory."""
    buf = io.BytesIO()
    img = Image.new("RGB", (width, height), color=color)
    img.save(buf, format="JPEG")
    return buf.getvalue()


class TestRAMQuotaEnforcement:
    def test_quota_allocation_and_release(self):
        """Allocations within quota succeed; exceeding quota raises error; release reclaims quota."""
        max_ram = 10 * 1024 * 1024  # 10MB
        manager = MemoryManager(max_bytes=max_ram)

        # Allocate 6MB
        lease1 = manager.acquire(6 * 1024 * 1024, timeout_s=1.0)
        assert lease1.size_bytes == 6 * 1024 * 1024
        assert manager.get_allocated_bytes() == 6 * 1024 * 1024

        # Attempt to allocate another 6MB (would exceed 10MB limit) -> timeout error
        with pytest.raises(RAMBudgetExceededError):
            manager.acquire(6 * 1024 * 1024, timeout_s=0.2)

        # Release first lease
        lease1.release()
        assert manager.get_allocated_bytes() == 0

        # Now 6MB allocation succeeds
        lease2 = manager.acquire(6 * 1024 * 1024, timeout_s=0.5)
        assert manager.get_allocated_bytes() == 6 * 1024 * 1024
        lease2.release()
        assert manager.get_allocated_bytes() == 0

    def test_concurrent_allocation_and_release(self):
        """Concurrent worker threads allocate and release from the shared pool without exceeding budget."""
        max_ram = 8 * 1024 * 1024  # 8MB
        manager = MemoryManager(max_bytes=max_ram)
        peak_allocated = 0
        lock = threading.Lock()
        errors = []

        def worker_task():
            nonlocal peak_allocated
            for _ in range(15):
                size = 1 * 1024 * 1024  # 1MB
                try:
                    with manager.reserve(size, timeout_s=2.0) as lease:
                        with lock:
                            current = manager.get_allocated_bytes()
                            if current > peak_allocated:
                                peak_allocated = current
                            if current > max_ram:
                                errors.append(f"Exceeded max_ram: {current} > {max_ram}")
                        time.sleep(0.01)
                except Exception as e:
                    errors.append(str(e))

        threads = [threading.Thread(target=worker_task) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors during concurrency: {errors}"
        assert peak_allocated <= max_ram
        assert manager.get_allocated_bytes() == 0


class TestSpooledCandidateBuffer:
    def test_in_memory_threshold(self):
        """Small images stay in memory while large images roll over to disk."""
        threshold = 50 * 1024  # 50KB threshold for testing
        manager = MemoryManager(max_bytes=10 * 1024 * 1024)

        # 1. Small image (under 50KB)
        small_data = _create_sample_jpeg_bytes(width=80, height=80)
        assert len(small_data) < threshold

        with SpooledCandidateBuffer(memory_manager=manager, max_spool_bytes=threshold) as buf:
            buf.write(small_data)
            assert not buf.is_rolled_to_disk
            w, h = buf.validate_image(min_w=64, min_h=64)
            assert (w, h) == (80, 80)
            sha = buf.compute_sha256()
            assert len(sha) == 64

        # 2. Large data (exceeds 50KB)
        large_data = b"X" * (80 * 1024)  # 80KB
        with SpooledCandidateBuffer(memory_manager=manager, max_spool_bytes=threshold) as buf:
            buf.write(large_data)
            assert buf.is_rolled_to_disk
            assert len(buf.get_bytes()) == len(large_data)

    def test_memory_cleanup_after_exceptions(self):
        """Memory leases and underlying temporary files are cleanly released even on uncaught errors."""
        manager = MemoryManager(max_bytes=10 * 1024 * 1024)
        assert manager.get_allocated_bytes() == 0

        with pytest.raises(RuntimeError):
            with SpooledCandidateBuffer(memory_manager=manager) as buf:
                buf.write(b"corrupt data")
                assert manager.get_allocated_bytes() > 0
                raise RuntimeError("Simulated failure during VLM inference or CUDA OOM")

        # Memory was released back to manager
        assert manager.get_allocated_bytes() == 0

    def test_image_processing_without_disk_reads(self):
        """Validates that dimensions, SHA256, pHash, and PIL images can be computed without disk files."""
        manager = MemoryManager(max_bytes=10 * 1024 * 1024)
        img_bytes = _create_sample_jpeg_bytes(width=128, height=128, color="red")

        with SpooledCandidateBuffer(memory_manager=manager) as buf:
            buf.write(img_bytes)

            # Verification
            w, h = buf.validate_image(min_w=64, min_h=64)
            assert w == 128 and h == 128

            # Hashes
            sha = buf.compute_sha256()
            phash = buf.compute_phash()
            assert sha is not None and len(sha) == 64
            assert phash is not None

            # PIL representation
            pil_img = buf.to_pil_image()
            assert pil_img.size == (128, 128)


class TestLocalStagingManager:
    def test_staging_and_backpressure_hysteresis(self, tmp_path):
        """High watermark (80%) pauses downloads; resume watermark (65%) resumes."""
        staging_dir = tmp_path / "staging"
        max_bytes = 100 * 1024  # 100KB for easy testing

        staging_mgr = LocalStagingManager(staging_path=staging_dir, max_bytes=max_bytes)
        job_id = uuid.uuid4()
        batch_idx = 1

        # 1. Initially empty -> no backpressure
        assert not staging_mgr.is_staging_backpressure_active()

        # 2. Add 85KB of files -> usage reaches 85% (>80% high watermark)
        staged_file = staging_mgr.stage_accepted_image(
            job_id=job_id,
            batch_index=batch_idx,
            asset_id=uuid.uuid4(),
            file_ext="jpg",
            source=b"A" * (85 * 1024),
        )
        assert staged_file.exists()
        assert staging_mgr.is_staging_backpressure_active() is True

        # 3. Delete some data down to 75KB (between 65% and 80%) -> hysteresis keeps backpressure active
        staged_file.unlink()
        staging_mgr.stage_accepted_image(
            job_id=job_id,
            batch_index=batch_idx,
            asset_id=uuid.uuid4(),
            file_ext="jpg",
            source=b"A" * (75 * 1024),
        )
        assert staging_mgr.is_staging_backpressure_active() is True

        # 4. Clean batch staging -> usage drops to 0% (<= 65% resume watermark) -> backpressure cleared
        staging_mgr.cleanup_batch_staging(job_id=job_id, batch_index=batch_idx)
        assert staging_mgr.is_staging_backpressure_active() is False

    def test_staging_isolation_across_multiple_jobs(self, tmp_path):
        """Different concurrent jobs stage to separate, isolated batch subdirectories."""
        staging_mgr = LocalStagingManager(staging_path=tmp_path / "staging")

        job_a = uuid.uuid4()
        job_b = uuid.uuid4()
        asset_a = uuid.uuid4()
        asset_b = uuid.uuid4()

        path_a = staging_mgr.stage_accepted_image(
            job_id=job_a,
            batch_index=1,
            asset_id=asset_a,
            file_ext="png",
            source=b"job_a_data",
        )
        path_b = staging_mgr.stage_accepted_image(
            job_id=job_b,
            batch_index=1,
            asset_id=asset_b,
            file_ext="jpg",
            source=b"job_b_data",
        )

        assert f"job_{job_a}" in str(path_a)
        assert f"job_{job_b}" in str(path_b)
        assert path_a.read_bytes() == b"job_a_data"
        assert path_b.read_bytes() == b"job_b_data"


class TestFailureRecovery:
    def test_reconcile_leases_reclaims_orphaned_memory(self):
        """If a worker process dies, reconcile_leases sweeps expired allocations."""
        manager = MemoryManager(max_bytes=10 * 1024 * 1024)

        # Mock Redis client holding an expired lease
        fake_redis = MagicMock()
        fake_redis.ping.return_value = True
        fake_redis.smembers.return_value = {b"lease_alive", b"lease_dead"}

        def mock_get(key):
            if "lease_alive" in key:
                return b"2097152"  # 2MB
            return None  # lease_dead has expired from Redis

        fake_redis.get.side_effect = mock_get

        with patch.object(manager, "_get_redis_safe", return_value=fake_redis):
            total_active = manager.reconcile_leases()
            # Cleaned up lease_dead and preserved 2MB
            fake_redis.srem.assert_called_once_with("storage:ram:active_leases", "lease_dead")
            assert total_active == 2097152


class TestStandardizationWithMemoryBuffer:
    def test_standardize_from_spooled_buffer(self, db_session, tmp_path):
        """StandardizationService processes draft with in-memory buffer without disk re-reads."""
        job_id = uuid.uuid4()
        task_id = uuid.uuid4()

        job = Job(id=job_id, query="mountain", target_count=100)
        task = JobSourceTask(id=task_id, job_id=job_id, source_type=SourceType.WEB, status=JobSourceTaskStatus.DOWNLOADING)
        db_session.add(job)
        db_session.add(task)
        db_session.commit()

        storage = LocalFilesystemStorage(tmp_path / "nas")
        asset_repo = AssetRepository(db_session)
        service = StandardizationService(asset_repo, storage, min_resolution=(64, 64))

        img_bytes = _create_sample_jpeg_bytes(width=120, height=120, color="green")
        manager = MemoryManager(max_bytes=10 * 1024 * 1024)

        with SpooledCandidateBuffer(memory_manager=manager) as buf:
            buf.write(img_bytes)

            draft = RawAssetDraft(
                source_item_id="mem_item_001",
                source_url="http://example.com/mem.jpg",
                dataset_identifier=None,
                license="CC0",
                local_tmp_path=Path("dummy_stream.jpg"),
                buffer=buf,
            )

            asset, created = service.standardize_and_promote(
                job_id=job_id,
                job_source_task_id=task_id,
                source_type=SourceType.WEB,
                draft=draft,
            )

            assert created is True
            assert asset.status == AssetStatus.PROMOTED
            assert asset.width == 120 and asset.height == 120
            assert asset.checksum_sha256 is not None
            assert asset.phash is not None
            assert Path(asset.nas_path).exists()


class TestStage3SecurityAndResilience:
    def test_ownership_validation_prevents_cross_release(self):
        """Worker A cannot release worker B's memory allocation."""
        manager_a = MemoryManager(max_bytes=10 * 1024 * 1024)
        manager_b = MemoryManager(max_bytes=10 * 1024 * 1024)
        assert manager_a.owner_id != manager_b.owner_id

        lease_a = manager_a.acquire(2 * 1024 * 1024)
        assert manager_a.get_allocated_bytes() == 2 * 1024 * 1024

        # Worker B attempts to release Worker A's lease with a forged lease object
        forged_lease = MemoryLease(
            lease_id=lease_a.lease_id,
            size_bytes=lease_a.size_bytes,
            created_at=lease_a.created_at,
            manager=manager_a,
            owner_id=manager_b.owner_id,  # wrong owner
        )
        with pytest.raises(PermissionError):
            manager_a.release(forged_lease)

        # Worker A releases properly
        assert lease_a.release() == 2 * 1024 * 1024
        assert manager_a.get_allocated_bytes() == 0

    def test_lease_renewal(self):
        """Long-running tasks can renew their lease TTL."""
        manager = MemoryManager(max_bytes=10 * 1024 * 1024)
        lease = manager.acquire(1 * 1024 * 1024)
        assert lease.renew(ttl_s=180) is True
        lease.release()

    def test_fail_closed_in_production_when_redis_unavailable(self):
        """When allow_local_fallback is False (production), Redis unavailability causes fail-closed behavior."""
        manager = MemoryManager(max_bytes=10 * 1024 * 1024, allow_local_fallback=False)
        with patch.object(manager, "_get_redis_safe", return_value=None):
            with pytest.raises(RAMBudgetExceededError, match="Failing closed"):
                manager.acquire(1024, timeout_s=0.1)

    def test_physical_disk_capacity_backpressure(self, tmp_path):
        """Backpressure triggers if physical filesystem is dangerously low (< 2GB free)."""
        staging_mgr = LocalStagingManager(staging_path=tmp_path / "staging", max_bytes=100 * 1024 * 1024)
        # Mock disk usage: only 500MB free (< 2GB physical floor)
        with patch("shutil.disk_usage", return_value=(1000 * 1024 * 1024, 999 * 1024 * 1024, 500 * 1024 * 1024)):
            assert staging_mgr.is_staging_backpressure_active() is True

    def test_application_restart_with_staged_batches(self, tmp_path):
        """Staged images and batch folders remain intact across application restarts."""
        staging_path = tmp_path / "staging"
        mgr1 = LocalStagingManager(staging_path=staging_path)

        job_id = uuid.uuid4()
        asset_id = uuid.uuid4()
        dest = mgr1.stage_accepted_image(
            job_id=job_id,
            batch_index=1,
            asset_id=asset_id,
            file_ext="jpg",
            source=b"persisted_payload",
        )
        assert dest.exists()

        # Simulate application restart: instantiate a new LocalStagingManager instance
        mgr2 = LocalStagingManager(staging_path=staging_path)
        batch_dir = mgr2.get_batch_staging_dir(job_id=job_id, batch_index=1)
        recovered_file = batch_dir / "images" / f"{asset_id}.jpg"

        assert recovered_file.exists()
        assert recovered_file.read_bytes() == b"persisted_payload"

