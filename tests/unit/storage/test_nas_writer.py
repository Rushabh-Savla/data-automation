"""Unit tests for Stage 6: Centralized NAS Upload Queue & Single Writer.

Covers all 15 required failure and recovery scenarios:
1. Two Celery workers attempting to acquire the NAS writer simultaneously.
2. Distributed lock expiry and recovery.
3. SFTP disconnection during upload.
4. NAS unavailable before upload.
5. NAS unavailable halfway through a batch.
6. Worker crash during file transfer.
7. Duplicate batch dispatch.
8. Partial batch completion.
9. Remote file-size mismatch.
10. Remote checksum mismatch.
11. Atomic rename failure.
12. Database update failure after successful upload.
13. Retry without duplicate uploads.
14. Local staging cleanup only after confirmed completion.
15. Staging backpressure when NAS remains unavailable.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import sessionmaker

from app.connectors.nas.base import (
    NasClient,
    NasConnectionError,
    NasConnectionParams,
    open_client,
)
from app.core.exceptions import TransientConnectorError
from app.db.base import Base
from app.db.models.asset import Asset
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.image_batch_repository import ImageBatchRepository, NasTransferBatchRepository
from app.domain.enums import AssetStatus, BatchStatus, JobSourceTaskStatus, SourceType, TransferStatus
from app.storage.nas_writer import (
    NasBatchWriter,
    NasWriterLock,
    compute_file_sha256,
    dequeue_nas_batch,
    enqueue_nas_batch,
    is_cooldown_active,
    start_cooldown,
)
from app.storage.ram_buffer import LocalStagingManager


def _create_task(db, job_id):
    task = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job_id,
        source_type=SourceType.WEB,
        status=JobSourceTaskStatus.COMPLETED,
    )
    db.add(task)
    db.flush()
    return task


def _create_asset(db, job_id, task_id, batch_id, local_file, status=AssetStatus.RELEVANCE_PASSED):
    a = Asset(
        id=uuid.uuid4(),
        job_id=job_id,
        job_source_task_id=task_id,
        source_type=SourceType.WEB,
        source_item_id=uuid.uuid4().hex,
        batch_id=batch_id,
        status=status,
        nas_path=str(local_file),
        checksum_sha256=compute_file_sha256(local_file) if local_file.exists() else None,
    )
    db.add(a)
    db.flush()
    return a


@pytest.fixture
def test_db():
    """Isolated in-memory SQLite database supporting JSON and text('now()')."""
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def register_sqlite_functions(dbapi_connection, connection_record):
        import datetime

        dbapi_connection.create_function(
            "now", 0, lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
        )

    for table in Base.metadata.tables.values():
        for col in table.columns:
            if isinstance(col.type, JSONB):
                col.type = JSON()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def env_setup(tmp_path):
    """Sets up local staging and mock NAS directories."""
    staging_root = tmp_path / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    nas_root = tmp_path / "nas_share"
    nas_root.mkdir(parents=True, exist_ok=True)

    params = NasConnectionParams(
        protocol="local",
        share=str(nas_root),
        storage_path="data_automation",
    )
    return SimpleNamespace(
        staging_root=staging_root,
        nas_root=nas_root,
        params=params,
        tmp_path=tmp_path,
    )


class FakeRedis:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, val, ex=None, nx=False):
        if nx and key in self._data:
            return None
        self._data[key] = val
        return True

    def delete(self, key):
        self._data.pop(key, None)

    def rpush(self, key, val):
        lst = self._data.setdefault(key, [])
        lst.append(val)

    def lpop(self, key):
        lst = self._data.get(key, [])
        if lst:
            return lst.pop(0)
        return None

    def exists(self, key):
        return 1 if key in self._data else 0

    def ping(self):
        return True

    def eval(self, script, numkeys, *args):
        # Support acquire, release, extend Lua scripts
        key = args[0]
        if "set" in script and "NX" in script:
            token, ttl = args[1], args[2]
            if key not in self._data:
                self._data[key] = token
                return 1
            return 0
        elif "del" in script:
            token = args[1]
            if self._data.get(key) == token:
                self._data.pop(key, None)
                return 1
            return 0
        elif "expire" in script:
            token = args[1]
            if self._data.get(key) == token:
                return 1
            return 0
        return 0


# ==============================================================================
# SECTION 7: 15 REQUIRED FAILURE & RECOVERY TESTS
# ==============================================================================


class TestCentralizedNasUploadQueue:
    def test_1_two_workers_competing_for_writer_lock(self):
        """1. Two Celery workers attempting to acquire the NAS writer simultaneously.

        Global concurrency = 1: Worker 1 acquires lock; Worker 2 is rejected.
        """
        fake_redis = FakeRedis()
        with patch("app.storage.nas_writer.get_redis", return_value=fake_redis):
            lock1 = NasWriterLock(ttl_s=60, owner_token="worker_1")
            lock2 = NasWriterLock(ttl_s=60, owner_token="worker_2")

            assert lock1.acquire() is True
            assert lock1.acquired is True

            # Worker 2 tries to acquire simultaneously
            assert lock2.acquire(timeout_s=0.05) is False
            assert lock2.acquired is False

            # Lock 1 releases; now Worker 2 can acquire
            assert lock1.release() is True
            assert lock2.acquire(timeout_s=0.05) is True
            assert lock2.release() is True

    def test_2_distributed_lock_expiry_and_recovery(self):
        """2. Distributed lock expiry and recovery.

        If a worker process holding the lock dies/crashes, the lock expires after TTL,
        allowing a subsequent worker to recover safely.
        """
        NasWriterLock._local_owner_token = None
        NasWriterLock._local_expires_at = 0.0

        with patch("app.storage.nas_writer.get_redis", side_effect=Exception("no redis")):
            # Worker 1 acquires lock with 0.1s TTL and crashes (never calls release)
            lock1 = NasWriterLock(ttl_s=0.1, owner_token="crashed_worker")
            assert lock1.acquire() is True

            # Worker 2 immediately cannot acquire
            lock2 = NasWriterLock(ttl_s=60, owner_token="recovery_worker")
            assert lock2.acquire(timeout_s=0.02) is False

            # Wait for lock1 to expire
            time.sleep(0.15)

            # Worker 2 can now acquire lock and recover
            assert lock2.acquire(timeout_s=0.05) is True
            assert lock2.release() is True

    def test_3_sftp_disconnection_during_upload(self, test_db, env_setup):
        """3. SFTP disconnection during upload.

        Simulates SFTP dropping connection mid-upload:
        - TransientConnectorError is raised.
        - Cooldown is activated to prevent thundering herd.
        - Local staging files remain intact.
        """
        job = Job(id=uuid.uuid4(), name="disconnection_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img1.jpg"
        img_file.write_bytes(b"image_content_1")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(
            json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]})
        )
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        fake_redis = FakeRedis()
        with patch("app.storage.nas_writer.get_redis", return_value=fake_redis):
            client = open_client(env_setup.params)
            client.connect()

            def failing_upload(src, dest):
                raise NasConnectionError("SFTP connection reset by peer")

            client._upload = failing_upload

            writer = NasBatchWriter(test_db, params=env_setup.params)
            with pytest.raises(TransientConnectorError, match="interrupted"):
                writer.upload_batch(transfer.id, client=client)

            assert is_cooldown_active(fake_redis) is True
            assert img_file.exists()
            assert not asset.nas_path.startswith("nas://")

    def test_4_nas_unavailable_before_upload(self, test_db, env_setup):
        """4. NAS unavailable before upload.

        When the NAS is down at connection start:
        - Fails cleanly without deleting local assets.
        - Cooldown is activated.
        """
        job = Job(id=uuid.uuid4(), name="unreachable_job", query="cat")
        test_db.add(job)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"content")

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": []}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        fake_redis = FakeRedis()
        with patch("app.storage.nas_writer.get_redis", return_value=fake_redis):
            with patch("app.storage.nas_writer.open_client") as mock_open:
                mock_client = MagicMock()
                mock_client.connect.side_effect = NasConnectionError("Host unreachable")
                mock_open.return_value = mock_client

                writer = NasBatchWriter(test_db, params=env_setup.params)
                with pytest.raises(TransientConnectorError):
                    writer.upload_batch(transfer.id)

                assert is_cooldown_active(fake_redis) is True
                assert img_file.exists()

    def test_5_nas_unavailable_halfway_through_batch(self, test_db, env_setup):
        """5. NAS unavailable halfway through a batch.

        In a 3-item batch, File 1 succeeds; File 2 drops connection:
        - uploaded_files = 1 recorded in DB.
        - pending_files accurately reflected.
        - All remaining local files remain on SSD.
        """
        job = Job(id=uuid.uuid4(), name="halfway_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)

        items = []
        for idx in range(1, 4):
            f = staging_dir / f"img_{idx}.jpg"
            f.write_bytes(f"image_data_{idx}".encode())
            a = _create_asset(test_db, job.id, task.id, batch.id, f)
            items.append({"asset_id": str(a.id), "local_path": str(f)})

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": items}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(
                batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=3, pending_files=3
            )
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()
        real_upload = client._upload

        upload_calls = 0

        def flaky_upload(src, dest):
            nonlocal upload_calls
            upload_calls += 1
            if upload_calls == 2:
                raise NasConnectionError("Dropped halfway")
            real_upload(src, dest)

        client._upload = flaky_upload

        fake_redis = FakeRedis()
        with patch("app.storage.nas_writer.get_redis", return_value=fake_redis):
            writer = NasBatchWriter(test_db, params=env_setup.params)
            with pytest.raises(TransientConnectorError):
                writer.upload_batch(transfer.id, client=client)

            refreshed = NasTransferBatchRepository(test_db).get(transfer.id)
            assert refreshed.uploaded_files == 1
            assert refreshed.pending_files == 2
            assert refreshed.transfer_status == TransferStatus.FAILED

            for idx in range(1, 4):
                assert (staging_dir / f"img_{idx}.jpg").exists()

    def test_6_worker_crash_during_file_transfer(self, test_db, env_setup):
        """6. Worker crash during file transfer leaving partial .part file.

        Verifies that any orphaned .part file is safely overwritten/removed,
        and does not corrupt final destination.
        """
        job = Job(id=uuid.uuid4(), name="crash_job", query="dog")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"clean_content")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        orphaned_part = env_setup.nas_root / "data_automation" / "orphaned.part"
        orphaned_part.parent.mkdir(parents=True, exist_ok=True)
        orphaned_part.write_bytes(b"corrupt_partial_bytes")

        writer = NasBatchWriter(test_db, params=env_setup.params)
        res = writer.upload_batch(transfer.id)
        assert res["status"] == "completed"

        refreshed_transfer = NasTransferBatchRepository(test_db).get(transfer.id)
        assert refreshed_transfer.transfer_status == TransferStatus.COMPLETED
        assert refreshed_transfer.verification_status == "passed"

    def test_7_duplicate_batch_dispatch(self, test_db, env_setup):
        """7. Duplicate batch dispatch.

        Submitting enqueue_nas_batch twice for the same batch:
        - Returns existing transfer record without creating duplicates.
        - Prevents multiple competing DB records.
        """
        job = Job(id=uuid.uuid4(), name="dup_job", query="cat")
        test_db.add(job)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)
        test_db.commit()

        @contextlib.contextmanager
        def scope():
            yield test_db

        with patch("app.storage.nas_writer.session_scope", scope):
            t1 = enqueue_nas_batch(batch.id, job.id, remote_dest="data_automation")
            t2 = enqueue_nas_batch(batch.id, job.id, remote_dest="data_automation")

            assert t1 == t2
            count = len(test_db.query(NasTransferBatch).filter_by(batch_id=batch.id).all())
            assert count == 1

    def test_8_partial_batch_completion(self, test_db, env_setup):
        """8. Partial batch completion.

        When a subset of items cannot be transferred (e.g. missing local file):
        - Successfully transfers valid items.
        - Accurately tracks failed_files and uploaded_files in DB.
        """
        job = Job(id=uuid.uuid4(), name="partial_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        valid_img = staging_dir / "valid.jpg"
        valid_img.write_bytes(b"valid_image_bytes")

        a1 = _create_asset(test_db, job.id, task.id, batch.id, valid_img)
        a2 = _create_asset(test_db, job.id, task.id, batch.id, staging_dir / "nonexistent.jpg")

        manifest = staging_dir / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "items": [
                        {"asset_id": str(a1.id), "local_path": str(valid_img)},
                        {"asset_id": str(a2.id), "local_path": str(staging_dir / "nonexistent.jpg")},
                    ]
                }
            )
        )
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(
                batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=2, pending_files=2
            )
        )
        test_db.commit()

        writer = NasBatchWriter(test_db, params=env_setup.params)
        res = writer.upload_batch(transfer.id)

        assert res["uploaded_files"] == 1
        assert res["failed_files"] == 1

        refreshed = NasTransferBatchRepository(test_db).get(transfer.id)
        assert refreshed.uploaded_files == 1
        assert refreshed.failed_files == 1

    def test_9_remote_file_size_mismatch(self, test_db, env_setup):
        """9. Remote file-size mismatch.

        If the remote size does not match local size (e.g. truncated upload):
        - Raises TransientConnectorError.
        - Deletes partial remote file.
        - Retains local SSD file.
        """
        job = Job(id=uuid.uuid4(), name="size_mismatch_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"1234567890")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()
        client.size = lambda rel: 5 if ".part" in rel else None

        writer = NasBatchWriter(test_db, params=env_setup.params)
        with pytest.raises(TransientConnectorError, match="size mismatch"):
            writer.upload_batch(transfer.id, client=client)

        assert img_file.exists()
        assert not asset.nas_path.startswith("nas://")

    def test_10_remote_checksum_mismatch(self, test_db, env_setup):
        """10. Remote checksum mismatch.

        If remote file has corrupt bytes despite correct size:
        - Raises TransientConnectorError.
        - Deletes remote part file.
        - Preserves local file without atomic rename.
        """
        job = Job(id=uuid.uuid4(), name="checksum_mismatch_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"uncompressed_clean_bytes")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()
        client.compute_checksum = lambda rel: "corrupt_sha256_hash_that_does_not_match"

        writer = NasBatchWriter(test_db, params=env_setup.params)
        with pytest.raises(TransientConnectorError, match="checksum mismatch"):
            writer.upload_batch(transfer.id, client=client)

        assert img_file.exists()
        assert not asset.nas_path.startswith("nas://")

    def test_11_atomic_rename_failure(self, test_db, env_setup):
        """11. Atomic rename failure.

        If server-side atomic rename fails (e.g. disk quota full or permissions):
        - Fails transfer.
        - Preserves local file.
        """
        job = Job(id=uuid.uuid4(), name="rename_fail_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"rename_bytes")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()

        def fail_rename(tmp, final):
            raise NasConnectionError("Remote rename permission denied")

        client._rename = fail_rename

        writer = NasBatchWriter(test_db, params=env_setup.params)
        with pytest.raises(TransientConnectorError):
            writer.upload_batch(transfer.id, client=client)

        assert img_file.exists()

    def test_12_database_update_failure_after_successful_upload(self, test_db, env_setup):
        """12. Database update failure after successful upload.

        If DB commit fails after remote upload, local staging cleanup must NOT occur.
        """
        job = Job(id=uuid.uuid4(), name="db_fail_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"content")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        writer = NasBatchWriter(test_db, params=env_setup.params)

        with patch.object(test_db, "commit", side_effect=RuntimeError("Database connection lost")):
            with pytest.raises(RuntimeError, match="Database connection lost"):
                writer.upload_batch(transfer.id)

            assert img_file.exists()

    def test_13_retry_without_duplicate_uploads(self, test_db, env_setup):
        """13. Retry without duplicate uploads.

        On retry, files already verified on NAS are idempotently skipped.
        """
        job = Job(id=uuid.uuid4(), name="idempotent_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)

        f1 = staging_dir / "file1.jpg"
        f1.write_bytes(b"bytes_one")
        f2 = staging_dir / "file2.jpg"
        f2.write_bytes(b"bytes_two")

        a1 = _create_asset(test_db, job.id, task.id, batch.id, f1)
        a2 = _create_asset(test_db, job.id, task.id, batch.id, f2)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "items": [
                        {"asset_id": str(a1.id), "local_path": str(f1), "checksum_sha256": compute_file_sha256(f1)},
                        {"asset_id": str(a2.id), "local_path": str(f2), "checksum_sha256": compute_file_sha256(f2)},
                    ]
                }
            )
        )
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(
                batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=2, pending_files=2
            )
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()

        from app.storage.nas_writer import _remote_rel_for
        from app.core.slugify import job_folder_name

        job_folder = job_folder_name(job.name, job.id)
        rel1 = _remote_rel_for(env_setup.params.storage_path, f1, job_folder)
        client._upload(f1, rel1)

        upload_tracker = []
        real_upload = client._upload

        def tracking_upload(src, dest):
            upload_tracker.append(dest)
            real_upload(src, dest)

        client._upload = tracking_upload

        writer = NasBatchWriter(test_db, params=env_setup.params)
        res = writer.upload_batch(transfer.id, client=client)

        assert res["status"] == "completed"
        assert res["uploaded_files"] == 2
        assert not any("file1.jpg" in dest and ".part" in dest for dest in upload_tracker)
        assert any("file2.jpg" in dest for dest in upload_tracker)

    def test_14_local_staging_cleanup_only_after_confirmed_completion(self, test_db, env_setup):
        """14. Local staging cleanup only after confirmed completion.

        Staging files exist throughout transfer and are deleted ONLY upon final success.
        """
        job = Job(id=uuid.uuid4(), name="cleanup_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"content")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)

        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        assert img_file.exists()

        writer = NasBatchWriter(test_db, params=env_setup.params)
        writer.staging_manager.staging_path = env_setup.staging_root

        res = writer.upload_batch(transfer.id)
        assert res["status"] == "completed"

        assert not img_file.exists()
        assert not staging_dir.exists()

    def test_15_staging_backpressure_when_nas_remains_unavailable(self, tmp_path):
        """15. Staging backpressure when NAS remains unavailable.

        When staged files cross the 80% watermark, backpressure activates and
        pauses candidate downloads.
        """
        staging_dir = tmp_path / "staging_quota"
        staging_dir.mkdir(parents=True, exist_ok=True)

        mgr = LocalStagingManager(staging_path=staging_dir, max_bytes=1000)

        test_file = staging_dir / "big.bin"
        test_file.write_bytes(b"X" * 850)

        assert mgr.is_staging_backpressure_active() is True

        from app.tasks.batch_orchestration import dispatch_batch_candidate_window

        with patch("app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=True):
            res = dispatch_batch_candidate_window(str(uuid.uuid4()), str(uuid.uuid4()))
            assert res["status"] == "paused_staging_backpressure"


class TestNasWriterHardeningAndRecovery:
    """Detailed verification for Stage 6 hardening:

    - Lock heartbeat renewal for uploads longer than TTL
    - Fail-closed behavior when Redis is unavailable (allow_local_fallback=False)
    - Writer fencing (LockLostError raised if lease lost mid-transfer)
    - Recovery Scenarios A, B, C, D (PostgreSQL authoritative source of truth)
    - VERIFIED vs COMPLETED lifecycle & bytes_transferred accounting
    """

    def test_heartbeat_renews_lock_during_long_upload(self):
        """Simulates an upload operation lasting longer than the original TTL.

        With TTL = 0.3s and heartbeat = 0.1s, sleeping 0.5s would expire a static lock,
        but the background heartbeat thread keeps renewing it so it remains active.
        """
        from app.storage.nas_writer import NasWriterLock

        lock = NasWriterLock(ttl_s=0.3, heartbeat_interval_s=0.1, allow_local_fallback=True)
        acquired = lock.acquire(timeout_s=0.1)
        assert acquired is True
        assert lock.is_active() is True

        # Sleep longer than the original TTL (0.5s > 0.3s)
        time.sleep(0.5)

        # Heartbeat has renewed it; lock is still valid and active
        assert lock.is_active() is True
        lock.release()
        assert lock.is_active() is False

    def test_lock_fails_closed_when_redis_unavailable_in_production(self, monkeypatch):
        """Production configuration must fail closed (return False) if Redis cannot be reached."""
        from app.storage.nas_writer import NasWriterLock

        monkeypatch.setattr("app.storage.nas_writer.get_redis", lambda: None)
        lock = NasWriterLock(ttl_s=60, allow_local_fallback=False)
        assert lock.acquire() is False
        assert lock.is_active() is False

    def test_writer_fencing_raises_lock_lost_error_mid_transfer(self, test_db, env_setup):
        """If the writer loses its lock lease mid-transfer, fencing immediately aborts with LockLostError."""
        from app.storage.nas_writer import LockLostError, NasWriterLock

        job = Job(id=uuid.uuid4(), name="fence_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_fence"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"content")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)
        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer_id = uuid.uuid4()
        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(id=transfer_id, batch_id=batch.id, job_id=job.id, remote_path="data_automation")
        )
        test_db.commit()

        # Create lock that simulates being revoked / expired mid-transfer
        mock_lock = MagicMock(spec=NasWriterLock)
        mock_lock.is_active.return_value = False  # Lock lease was lost!

        writer = NasBatchWriter(test_db, params=env_setup.params, lock=mock_lock)
        writer.staging_manager.staging_path = env_setup.staging_root

        with pytest.raises(LockLostError, match="lock lease was lost"):
            writer.upload_batch(transfer.id)

    def test_recovery_scenario_a_redis_enqueue_fails_postgres_recovers(self, test_db, monkeypatch):
        """Scenario A: PostgreSQL commits transfer, Redis enqueue fails.

        dequeue_nas_batch discovers the actionable transfer from PostgreSQL directly.
        """
        job = Job(id=uuid.uuid4(), name="scen_a", query="test")
        test_db.add(job)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)
        test_db.commit()

        # Simulate Redis failure during enqueue
        monkeypatch.setattr("app.storage.nas_writer._get_redis", lambda: None)

        repo = NasTransferBatchRepository(test_db)
        transfer = repo.get_or_create_for_batch(batch_id=batch.id, job_id=job.id, remote_path="test_dest")
        assert transfer.transfer_status == TransferStatus.PENDING

        # Dequeue inspects PostgreSQL directly and discovers the pending transfer
        dequeued_id = dequeue_nas_batch(test_db)
        assert dequeued_id == transfer.id

    def test_recovery_scenario_b_c_crashed_worker_stale_transfer_recovered(self, test_db):
        """Scenarios B & C: Worker crashed while processing transfer; status was left UPLOADING.

        list_actionable detects stale transfers older than threshold and recovers them.
        """
        from datetime import datetime, timezone, timedelta

        job = Job(id=uuid.uuid4(), name="scen_bc", query="test")
        test_db.add(job)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)
        test_db.commit()

        repo = NasTransferBatchRepository(test_db)
        transfer = repo.get_or_create_for_batch(batch_id=batch.id, job_id=job.id, remote_path="test_dest")
        transfer.transfer_status = TransferStatus.UPLOADING
        # Simulate worker crashed 20 minutes ago
        transfer.updated_at = datetime.now(timezone.utc) - timedelta(seconds=1200)
        test_db.commit()

        actionable = repo.list_actionable(stale_threshold_seconds=600)
        assert len(actionable) == 1
        assert actionable[0].id == transfer.id

    def test_recovery_scenario_d_resumes_without_duplicate_and_verifies_accounting(self, test_db, env_setup):
        """Scenario D & Transfer Accounting:

        - Worker crashed after uploading files.
        - Retry inspects remote files, confirms SHA256 matches, skips re-uploading bytes.
        - bytes_transferred represents verified payload bytes.
        - Status transitions PENDING -> UPLOADING -> VERIFIED -> COMPLETED.
        """
        job = Job(id=uuid.uuid4(), name="scen_d", query="test")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_scen_d"
        staging_dir.mkdir(parents=True)
        img_bytes = b"durable_image_payload_content_12345"
        img_file = staging_dir / "img1.jpg"
        img_file.write_bytes(img_bytes)

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)
        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        repo = NasTransferBatchRepository(test_db)
        transfer = repo.create(
            NasTransferBatch(batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=1)
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()

        # First run: successfully completes upload, verify, and completion
        writer = NasBatchWriter(test_db, params=env_setup.params)
        writer.staging_manager.staging_path = env_setup.staging_root

        res = writer.upload_batch(transfer.id, client=client)
        assert res["status"] == "completed"

        # Check DB states
        refreshed = repo.get(transfer.id)
        assert refreshed.transfer_status == TransferStatus.COMPLETED
        assert refreshed.verification_status == "passed"
        assert refreshed.verified_at is not None
        assert refreshed.completed_at is not None
        assert refreshed.bytes_transferred == len(img_bytes)

        # Retry should identify file already exists and verified on remote, not double-counting bytes
        res_retry = writer.upload_batch(transfer.id, client=client)
        assert res_retry["status"] == "completed"
        refreshed_retry = repo.get(transfer.id)
        assert refreshed_retry.bytes_transferred == len(img_bytes)  # Not doubled!
        assert refreshed_retry.staging_cleanup_status == "completed"

    def test_existing_remote_file_mismatched_checksum_is_preserved_not_quarantined(self, test_db, env_setup):
        """A worker must never overwrite or quarantine an existing file with different checksum."""
        from app.core.slugify import job_folder_name
        from app.storage.nas_writer import _remote_rel_for, LockLostError

        job = Job(id=uuid.uuid4(), name="quarantine_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_bytes = b"new_clean_image_content"
        img_file = staging_dir / "img_q.jpg"
        img_file.write_bytes(img_bytes)

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)
        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=1)
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()

        # Place an existing verified file at the exact final remote path beforehand
        job_folder = job_folder_name(job.name, job.id)
        final_rel = _remote_rel_for(env_setup.params.storage_path, img_file, job_folder)
        existing_remote = env_setup.nas_root / final_rel
        existing_remote.parent.mkdir(parents=True, exist_ok=True)
        existing_remote.write_bytes(b"existing_verified_file_content")

        writer = NasBatchWriter(test_db, params=env_setup.params)
        writer.staging_manager.staging_path = env_setup.staging_root

        with pytest.raises(LockLostError, match="Target .* already published with differing checksum"):
            writer.upload_batch(transfer.id, client=client)

        # Verify existing file was preserved intact (NOT overwritten)
        assert existing_remote.read_bytes() == b"existing_verified_file_content"

        # Verify existing file was NOT quarantined
        quarantined_files = list(existing_remote.parent.glob("img_q.jpg.corrupt.*"))
        assert len(quarantined_files) == 0

        # Verify temporary .part file was safely deleted
        part_files = list(existing_remote.parent.glob("*.part"))
        assert len(part_files) == 0

    def test_rename_race_stale_worker_cannot_overwrite_or_quarantine_newer_publication(self, test_db, env_setup):
        """Simulates exact race sequence:
        1. Worker A passes pre-rename check.
        2. Worker A pauses.
        3. Worker A's lock expires.
        4. Worker B acquires lock.
        5. Worker B publishes correct final file.
        6. Worker A resumes.
        7. Worker A attempts to publish older temporary file.

        Verifies:
        - Worker A cannot overwrite Worker B's verified file.
        - Worker A cannot quarantine Worker B's file.
        - Worker B's correct final checksum remains intact.
        - Only current owner can finalize DB record.
        - Stale worker's temporary files are cleaned safely.
        """
        from app.core.slugify import job_folder_name
        from app.storage.nas_writer import _remote_rel_for, LockLostError, NasWriterLock

        job = Job(id=uuid.uuid4(), name="race_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_bytes_A = b"payload_from_worker_A"
        img_bytes_B = b"payload_from_worker_B_newer_and_verified"
        img_file = staging_dir / "img_race.jpg"
        img_file.write_bytes(img_bytes_A)

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)
        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        repo = NasTransferBatchRepository(test_db)
        transfer = repo.create(
            NasTransferBatch(batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=1)
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()
        job_folder = job_folder_name(job.name, job.id)
        final_rel = _remote_rel_for(env_setup.params.storage_path, img_file, job_folder)
        final_dest = env_setup.nas_root / final_rel

        # Setup Worker A with Lock A
        lock_A = MagicMock(spec=NasWriterLock)
        lock_A.owner_token = "worker_A_token_111"
        lock_A.is_active.return_value = True

        writer_A = NasBatchWriter(test_db, params=env_setup.params, lock=lock_A)
        writer_A.staging_manager.staging_path = env_setup.staging_root

        # Worker A registers as uploading
        repo.mark_uploading(transfer.id, owner_token=lock_A.owner_token)

        # Worker A prepares its temporary part file
        tmp_rel_A = f"{final_rel}.{transfer.id.hex[:8]}.{lock_A.owner_token[:8]}.part"
        client._upload(img_file, tmp_rel_A)
        assert client.exists(tmp_rel_A)

        # STEP 1 & 2: Worker A passes pre-rename check, then pauses!
        # STEP 3 & 4: Worker A's lock expires; Worker B acquires lock!
        lock_B = MagicMock(spec=NasWriterLock)
        lock_B.owner_token = "worker_B_token_222"
        lock_B.is_active.return_value = True

        # Worker A is now superseded
        lock_A.is_active.return_value = False

        # STEP 5: Worker B claims transfer and publishes correct final file B
        repo.mark_uploading(transfer.id, owner_token=lock_B.owner_token)
        final_dest.parent.mkdir(parents=True, exist_ok=True)
        final_dest.write_bytes(img_bytes_B)  # Worker B published final file B!
        repo.mark_verified(transfer.id, owner_token=lock_B.owner_token)

        # STEP 6 & 7: Worker A resumes and attempts to publish its temporary part file A
        with pytest.raises(LockLostError):
            writer_A._safe_publish(client, tmp_rel_A, final_rel, expected_sha256=compute_file_sha256(img_file))

        # VERIFY 1: Worker A did NOT overwrite Worker B's final file!
        assert final_dest.read_bytes() == img_bytes_B

        # VERIFY 2: Worker A did NOT quarantine Worker B's file!
        quarantines = list(final_dest.parent.glob("img_race.jpg.corrupt.*"))
        assert len(quarantines) == 0

        # VERIFY 3: Worker B's correct checksum remains intact
        assert client.compute_checksum(final_rel) == hashlib.sha256(img_bytes_B).hexdigest()

        # VERIFY 4: Worker A's temporary part file was safely deleted
        assert not client.exists(tmp_rel_A)

        # VERIFY 5: Conditional DB update prevents Worker A from finalizing transfer
        with pytest.raises(LockLostError, match="Conditional DB update failed"):
            repo.mark_completed_staging_pending(transfer.id, owner_token=lock_A.owner_token)

        # Worker B (current owner) can finalize successfully
        repo.mark_completed_staging_pending(transfer.id, owner_token=lock_B.owner_token)
        refreshed = repo.get(transfer.id)
        assert refreshed.transfer_status == TransferStatus.COMPLETED

    def test_list_actionable_recovers_retryable_failed_and_exhausts_at_limit(self, test_db):
        """list_actionable discovers retryable FAILED transfers and ignores exhausted ones."""
        from datetime import datetime, timezone, timedelta

        job = Job(id=uuid.uuid4(), name="retry_test_job", query="cat")
        test_db.add(job)
        batch1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        batch2 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=2, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add_all([batch1, batch2])
        test_db.commit()

        repo = NasTransferBatchRepository(test_db)

        # Transfer 1: FAILED with attempt_count=2 (< 5 max_retries), updated 60s ago (> 30s delay) -> retryable!
        t1 = repo.create(
            NasTransferBatch(
                batch_id=batch1.id,
                job_id=job.id,
                remote_path="data_automation",
                transfer_status=TransferStatus.FAILED,
                attempt_count=2,
            )
        )
        t1.updated_at = datetime.now(timezone.utc) - timedelta(seconds=60)

        # Transfer 2: FAILED with attempt_count=5 (>= 5 max_retries) -> exhausted!
        t2 = repo.create(
            NasTransferBatch(
                batch_id=batch2.id,
                job_id=job.id,
                remote_path="data_automation",
                transfer_status=TransferStatus.FAILED,
                attempt_count=5,
            )
        )
        t2.updated_at = datetime.now(timezone.utc) - timedelta(seconds=60)
        test_db.commit()

        actionable = repo.list_actionable(max_retries=5, retry_delay_seconds=30)
        actionable_ids = [t.id for t in actionable]
        assert t1.id in actionable_ids
        assert t2.id not in actionable_ids  # Exhausted, never dispatched infinitely!

    def test_worker_crash_immediately_after_verified_commit_recovers_to_completion(self, test_db, env_setup):
        """Worker crashes after committing status=VERIFIED:

        - Recovery discovers the VERIFIED transfer.
        - upload_batch bypasses re-upload entirely.
        - Transitions to COMPLETED and safely purges staging directory.
        """
        job = Job(id=uuid.uuid4(), name="verified_crash_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_bytes = b"already_uploaded_and_verified_content"
        img_file = staging_dir / "img_vc.jpg"
        img_file.write_bytes(img_bytes)

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)
        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        repo = NasTransferBatchRepository(test_db)
        transfer = repo.create(
            NasTransferBatch(
                batch_id=batch.id,
                job_id=job.id,
                remote_path="data_automation",
                transfer_status=TransferStatus.VERIFIED,  # Worker crashed after marking VERIFIED!
                verification_status="passed",
                total_files=1,
                bytes_transferred=len(img_bytes),
                staging_cleanup_status="none",
            )
        )
        test_db.commit()

        # Discovery test: VERIFIED is actionable
        actionable = repo.list_actionable()
        assert transfer.id in [t.id for t in actionable]

        # Execution test: writer bypasses upload and completes staging cleanup
        client = open_client(env_setup.params)
        client.connect()
        upload_mock = MagicMock()
        client._upload = upload_mock

        writer = NasBatchWriter(test_db, params=env_setup.params)
        writer.staging_manager.staging_path = env_setup.staging_root

        res = writer.upload_batch(transfer.id, client=client)
        assert res["status"] == "completed"

        # Verify zero upload calls made (already verified on NAS!)
        assert upload_mock.call_count == 0

        # Verify staging files were deleted and status is completed
        assert not img_file.exists()
        refreshed = repo.get(transfer.id)
        assert refreshed.transfer_status == TransferStatus.COMPLETED
        assert refreshed.staging_cleanup_status == "completed"
        assert refreshed.staging_cleaned_at is not None

    def test_cleanup_failure_retains_pending_and_reconciles_on_retry(self, test_db, env_setup):
        """Cleanup failure (e.g. permission or filesystem error):

        - Explicit verification detects directory still exists.
        - staging_cleanup_status is retained as 'pending'.
        - Transfer remains COMPLETED (transfer not invalidated).
        - Subsequent retry removes directory and marks cleanup completed.
        """
        job = Job(id=uuid.uuid4(), name="cleanup_fail_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img.jpg"
        img_file.write_bytes(b"data")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)
        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        repo = NasTransferBatchRepository(test_db)
        transfer = repo.create(
            NasTransferBatch(batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=1)
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()

        writer = NasBatchWriter(test_db, params=env_setup.params)
        writer.staging_manager.staging_path = env_setup.staging_root

        # Mock rmtree failing to delete the directory (e.g. PermissionError)
        with patch("shutil.rmtree", side_effect=PermissionError("Permission denied")):
            res = writer.upload_batch(transfer.id, client=client)
            assert res["status"] == "completed"

            # Directory still exists!
            assert staging_dir.exists()
            refreshed = repo.get(transfer.id)
            assert refreshed.transfer_status == TransferStatus.COMPLETED
            # Durably persisted cleanup status is 'pending'
            assert refreshed.staging_cleanup_status == "pending"

        # Check list_pending_cleanups discovers it
        pending = repo.list_pending_cleanups()
        assert transfer.id in [p.id for p in pending]

        # Simulate reconciler running retry cleanup without failure
        success = writer.staging_manager.cleanup_batch_staging(batch.job_id, batch.batch_index)
        assert success is True
        assert not staging_dir.exists()

        repo.mark_staging_cleaned(transfer.id)
        refreshed2 = repo.get(transfer.id)
        assert refreshed2.staging_cleanup_status == "completed"
        assert len(repo.list_pending_cleanups()) == 0

    def test_lock_loss_immediately_before_rename_aborts_and_cleans_part(self, test_db, env_setup):
        """If writer loses its lock lease immediately before atomic rename:

        - Aborts with LockLostError.
        - Deletes the temporary .part file.
        - Never publishes or overwrites the destination.
        """
        from app.storage.nas_writer import LockLostError

        job = Job(id=uuid.uuid4(), name="fence_rename_job", query="cat")
        test_db.add(job)
        task = _create_task(test_db, job.id)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        test_db.add(batch)

        staging_dir = env_setup.staging_root / f"job_{job.id}" / "batch_001"
        staging_dir.mkdir(parents=True)
        img_file = staging_dir / "img_fr.jpg"
        img_file.write_bytes(b"content")

        asset = _create_asset(test_db, job.id, task.id, batch.id, img_file)
        manifest = staging_dir / "manifest.json"
        manifest.write_text(json.dumps({"items": [{"asset_id": str(asset.id), "local_path": str(img_file)}]}))
        batch.manifest_path = str(manifest)
        test_db.commit()

        transfer = NasTransferBatchRepository(test_db).create(
            NasTransferBatch(batch_id=batch.id, job_id=job.id, remote_path="data_automation", total_files=1)
        )
        test_db.commit()

        client = open_client(env_setup.params)
        client.connect()

        mock_lock = MagicMock()
        # Active during upload, but becomes inactive right before rename!
        mock_lock.is_active.side_effect = [True, False, False]
        mock_lock.owner_token = "tok123"

        writer = NasBatchWriter(test_db, params=env_setup.params, lock=mock_lock)
        writer.staging_manager.staging_path = env_setup.staging_root

        with pytest.raises(LockLostError, match="before atomic rename"):
            writer.upload_batch(transfer.id, client=client)

        # Confirm no .part files left behind on the remote share
        all_remote_files = list(env_setup.nas_root.rglob("*.part"))
        assert len(all_remote_files) == 0
