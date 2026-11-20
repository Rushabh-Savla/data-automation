import contextlib
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

import app.tasks.nas_tasks as nt
from app.connectors.nas import NasAuthError, NasConnectionError, NasConnectionParams, open_client
from app.core.exceptions import TransientConnectorError
from app.domain.enums import AssetStatus
from app.storage import asset_files


class FakeDb:
    def __init__(self, asset):
        self.asset = asset

    def execute(self, stmt):
        asset = self.asset
        # _job_folder_for_asset's select(Job.name) query: no job name on file in
        # this fake DB, so job_folder_name() falls back to the plain job id,
        # matching the `world.job` string the remote-path assertions expect.
        if list(getattr(stmt, "selected_columns", {}).keys()) == ["name"]:
            return SimpleNamespace(scalar_one_or_none=lambda: None)
        return SimpleNamespace(
            scalar_one_or_none=lambda: asset,
            scalars=lambda: iter(getattr(self, "ids", [])),
            all=lambda: list(getattr(self, "rows", [])),
        )


class FakeRepo:
    def __init__(self, db):
        self.db = db

    def get(self, asset_id):
        return self.db.asset


@pytest.fixture
def world(tmp_path, monkeypatch):
    nas = tmp_path / "nas"
    nas.mkdir()
    nas_root = tmp_path / "nas_root"
    job = str(uuid.uuid4())
    local_img = nas_root / job / "coco" / "images" / "x.jpg"
    local_img.parent.mkdir(parents=True)
    local_img.write_bytes(b"pretend-jpeg")
    local_ann = nas_root / job / "coco" / "annotations" / "x.txt"
    local_ann.parent.mkdir(parents=True)
    local_ann.write_text("0 0.5 0.5 1 1")

    params = NasConnectionParams(protocol="local", share=str(nas), storage_path="data_automation")
    asset = SimpleNamespace(
        id=uuid.uuid4(), status=AssetStatus.RELEVANCE_PASSED,
        nas_path=str(local_img), annotation_nas_path=str(local_ann), job_id=uuid.UUID(job),
    )
    db = FakeDb(asset)

    @contextlib.contextmanager
    def scope():
        yield db

    monkeypatch.setattr(nt, "session_scope", scope)
    monkeypatch.setattr(nt, "AssetRepository", FakeRepo)
    monkeypatch.setattr(nt, "get_active_storage_params", lambda: params)
    monkeypatch.setattr(nt, "get_settings", lambda: SimpleNamespace(nas_root=nas_root))
    monkeypatch.setattr(asset_files, "get_saved_params", lambda: params)
    monkeypatch.setattr(
        asset_files, "get_settings", lambda: SimpleNamespace(work_root=tmp_path / "work", nas_cache_max_gb=1.0)
    )
    return SimpleNamespace(
        nas=nas, job=job, params=params, asset=asset, db=db, local_img=local_img, local_ann=local_ann,
        monkeypatch=monkeypatch, tmp=tmp_path,
    )


class TestOffload:
    def test_moves_a_kept_image_to_the_nas_and_frees_local_disk(self, world):
        assert nt._offload(world.asset.id) == "offloaded"
        remote_rel = f"data_automation/{world.job}/coco/images/x.jpg"
        assert (world.nas / remote_rel).read_bytes() == b"pretend-jpeg"
        assert world.asset.nas_path == f"nas://{remote_rel}"
        assert not world.local_img.exists()

    def test_annotation_moves_with_the_image(self, world):
        nt._offload(world.asset.id)
        assert world.asset.annotation_nas_path == f"nas://data_automation/{world.job}/coco/annotations/x.txt"
        assert (world.nas / f"data_automation/{world.job}/coco/annotations/x.txt").exists()
        assert not world.local_ann.exists()

    def test_local_thumbnail_is_cleaned_up_too(self, world):
        thumb = world.local_img.with_name("x.thumb.jpg")
        thumb.write_bytes(b"t")
        nt._offload(world.asset.id)
        assert not thumb.exists()

    def test_the_offloaded_image_can_be_read_back_through_asset_files(self, world):
        nt._offload(world.asset.id)
        assert asset_files.materialize(world.asset.nas_path).read_bytes() == b"pretend-jpeg"

    def test_no_nas_storage_means_nothing_happens(self, world):
        world.monkeypatch.setattr(nt, "get_active_storage_params", lambda: None)
        assert nt._offload(world.asset.id) == "no_nas_storage"
        assert world.local_img.exists() and not world.asset.nas_path.startswith("nas://")

    @pytest.mark.parametrize("status", [AssetStatus.PROMOTED, AssetStatus.RELEVANCE_REJECTED])
    def test_only_kept_images_are_offloaded(self, world, status):
        world.asset.status = status
        assert nt._offload(world.asset.id) == "skipped"
        assert world.local_img.exists()
        assert not (world.nas / "data_automation").exists()

    def test_already_offloaded_asset_is_left_alone(self, world):
        world.asset.nas_path = "nas://data_automation/j/coco/images/x.jpg"
        assert nt._offload(world.asset.id) == "skipped"

    def test_missing_asset_row_is_skipped(self, world):
        world.db.asset = None
        assert nt._offload(uuid.uuid4()) == "skipped"

    def test_local_file_already_gone_is_reported_not_crashed(self, world):
        world.local_img.unlink()
        assert nt._offload(world.asset.id) == "local_missing"


class TestOffloadFailures:
    def _break_nas(self, world, exc):
        class Broken:
            def __init__(self, params):
                pass

            def __enter__(self):
                raise exc

            def __exit__(self, *a):
                return None

        world.monkeypatch.setattr(nt, "open_client", Broken)

    def test_nas_down_keeps_the_local_file_and_asks_for_a_retry(self, world):
        self._break_nas(world, NasConnectionError("host unreachable"))
        with pytest.raises(TransientConnectorError):
            nt._offload(world.asset.id)
        assert world.local_img.exists()
        assert not world.asset.nas_path.startswith("nas://")

    def test_rejected_login_keeps_the_file_without_retrying(self, world):
        self._break_nas(world, NasAuthError("bad password"))
        assert nt._offload(world.asset.id) == "failed"
        assert world.local_img.exists() and not world.asset.nas_path.startswith("nas://")

    def test_a_copy_that_does_not_verify_is_never_trusted(self, world):
        class Lying:
            def __init__(self, params):
                self.inner = open_client(params)

            def __enter__(self):
                self.inner.connect()
                return self

            def __exit__(self, *a):
                self.inner.close()

            def upload(self, local, rel):
                self.inner.upload(local, rel)

            def size(self, rel):
                return 1  # wrong on purpose

            def delete(self, rel):
                self.inner.delete(rel)

        world.monkeypatch.setattr(nt, "open_client", Lying)
        with pytest.raises(TransientConnectorError, match="did not verify"):
            nt._offload(world.asset.id)
        assert world.local_img.exists() and not world.asset.nas_path.startswith("nas://")
        assert not (world.nas / f"data_automation/{world.job}/coco/images/x.jpg").exists(), "no unverified copy left behind"

    def test_rejected_while_uploading_undoes_the_nas_copy(self, world):
        original_upload_verified = nt._upload_verified

        def upload_then_get_rejected(client, local_file, rel):
            original_upload_verified(client, local_file, rel)
            world.asset.status = AssetStatus.RELEVANCE_REJECTED  # the trim/dedup lands mid-upload

        world.monkeypatch.setattr(nt, "_upload_verified", upload_then_get_rejected)
        assert nt._offload(world.asset.id) == "raced"
        assert not (world.nas / f"data_automation/{world.job}/coco/images/x.jpg").exists()
        assert not world.asset.nas_path.startswith("nas://")

    def test_deleted_while_uploading_undoes_the_nas_copy(self, world):
        original_upload_verified = nt._upload_verified

        def upload_then_vanish(client, local_file, rel):
            original_upload_verified(client, local_file, rel)
            world.db.asset = None

        world.monkeypatch.setattr(nt, "_upload_verified", upload_then_vanish)
        assert nt._offload(world.asset.id) == "raced"
        assert not (world.nas / f"data_automation/{world.job}/coco/images/x.jpg").exists()


class TestRemotePathLayout:
    def test_mirrors_the_local_staging_layout_under_the_storage_folder(self, world):
        rel = nt._remote_rel_for("data_automation", world.local_img)
        assert rel == f"data_automation/{world.job}/coco/images/x.jpg"

    def test_files_outside_nas_root_still_get_a_sane_path(self, world, tmp_path):
        odd = tmp_path / "elsewhere" / "job9" / "coco" / "images" / "y.jpg"
        assert nt._remote_rel_for("out", odd) == "out/job9/coco/images/y.jpg"


class TestScheduleOffloadForJob:
    """schedule_offload_for_job: drains Redis list → dispatches chunks after all
    relevance batches are done. Reuses all existing cooldown / NAS-active guards."""

    def _make_redis(self, stored: list[str]):
        """Minimal fake Redis that backs lrange / delete with a list."""
        class FakeRedis:
            def __init__(self, data):
                self._data = {f"job:j1:offload_pending": list(data)}

            def lrange(self, key, start, end):
                return [v.encode() for v in self._data.get(key, [])]

            def delete(self, key):
                self._data.pop(key, None)

            def get(self, key):
                return None  # no cooldown

        return FakeRedis(stored)

    def test_dispatches_all_ids_in_chunks(self, monkeypatch):
        sent = []
        monkeypatch.setattr(nt, "MAX_NAS_OFFLOAD_BATCH_SIZE", 20)
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: False)
        monkeypatch.setattr(nt, "get_redis", lambda: self._make_redis([str(i) for i in range(25)]))
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: sent.append(list(ids)))
        nt.schedule_offload_for_job("j1")
        # 25 IDs with chunk size 20 → 2 batches
        assert len(sent) == 2
        assert len(sent[0]) == 20
        assert len(sent[1]) == 5
        # All IDs covered, no repeats
        flat = [x for chunk in sent for x in chunk]
        assert sorted(flat, key=int) == [str(i) for i in range(25)]

    def test_deduplicates_ids_pushed_by_redelivered_batches(self, monkeypatch):
        sent = []
        ids = ["a", "b", "a", "c", "b"]  # duplicates from redelivery
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: False)
        monkeypatch.setattr(nt, "get_redis", lambda: self._make_redis(ids))
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda x: sent.extend(x))
        nt.schedule_offload_for_job("j1")
        assert sorted(sent) == ["a", "b", "c"]

    def test_is_noop_when_no_ids_are_pending(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: False)
        monkeypatch.setattr(nt, "get_redis", lambda: self._make_redis([]))
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: pytest.fail("must not dispatch"))
        nt.schedule_offload_for_job("j1")  # must not raise or dispatch

    def test_is_noop_when_no_nas_is_active(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: False)
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: pytest.fail("must not dispatch"))
        nt.schedule_offload_for_job("j1")

    def test_skips_dispatch_during_cooldown_without_raising(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: True)
        monkeypatch.setattr(nt, "get_redis", lambda: self._make_redis(["x", "y"]))
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: pytest.fail("must not dispatch"))
        nt.schedule_offload_for_job("j1")  # IDs stay RELEVANCE_PASSED; sweeper retries

    def test_never_raises_into_the_caller(self, monkeypatch):
        def boom():
            raise RuntimeError("redis down")

        monkeypatch.setattr(nt, "is_storage_active", boom)
        nt.schedule_offload_for_job("j1")  # must swallow

    def test_redis_key_is_deleted_after_drain(self, monkeypatch):
        deleted = []

        class TrackingRedis:
            def lrange(self, key, *_):
                return [b"a", b"b"]

            def delete(self, key):
                deleted.append(key)

            def get(self, key):
                return None

        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: False)
        monkeypatch.setattr(nt, "get_redis", lambda: TrackingRedis())
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: None)
        nt.schedule_offload_for_job("myjob")
        assert "job:myjob:offload_pending" in deleted


class TestScheduling:
    def test_schedule_offload_dispatches_one_batch_task(self, monkeypatch):
        sent = []
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: False)
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: sent.append(ids))
        nt.schedule_offload(["a", "b"])
        assert sent == [["a", "b"]]

    def test_schedule_offload_is_a_noop_without_a_nas(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: False)
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: pytest.fail("must not dispatch"))
        nt.schedule_offload(["a"])
        nt.schedule_offload([])

    def test_schedule_offload_skips_while_the_nas_is_cooling_down(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: True)
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: pytest.fail("must not dispatch"))
        nt.schedule_offload(["a"])

    def test_schedule_offload_never_raises_into_the_filter(self, monkeypatch):
        def broken():
            raise RuntimeError("db down")

        monkeypatch.setattr(nt, "is_storage_active", broken)
        nt.schedule_offload(["a"])  # must swallow

    def test_sweeper_dispatches_local_kept_assets_only_when_a_nas_is_active(self, world, monkeypatch):
        sent = []
        world.db.ids = [uuid.uuid4(), uuid.uuid4()]
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: False)
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: sent.extend(ids))
        assert nt.sweep_unoffloaded_assets() == 2 and len(sent) == 2
        monkeypatch.setattr(nt, "is_storage_active", lambda: False)
        assert nt.sweep_unoffloaded_assets() == 0

    def test_sweeper_skips_while_the_nas_is_cooling_down(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        monkeypatch.setattr(nt, "_cooldown_active", lambda r: True)
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: pytest.fail("must not dispatch"))
        assert nt.sweep_unoffloaded_assets() == 0


class TestOffloadTaskLock:
    def test_a_second_concurrent_offload_of_the_same_asset_is_refused(self, monkeypatch):
        held = set()

        class FakePipeline:
            def __init__(self):
                self._ops: list[str] = []

            def set(self, key, val, nx=False, ex=None):
                self._ops.append(key)
                return self

            def execute(self):
                results = []
                for key in self._ops:
                    if key in held:
                        results.append(None)
                    else:
                        held.add(key)
                        results.append(True)
                return results

        class R:
            def get(self, key):
                return None

            def set(self, key, val, nx=False, ex=None):
                if key in held:
                    return None
                held.add(key)
                return True

            def delete(self, key):
                held.discard(key)

            def pipeline(self):
                return FakePipeline()

        monkeypatch.setattr(nt, "get_redis", lambda: R())
        monkeypatch.setattr(nt, "get_active_storage_params", lambda: object())
        monkeypatch.setattr(nt, "open_client", lambda params: contextlib.nullcontext(object()))
        monkeypatch.setattr(nt, "_prefetch_job_folders", lambda db, ids: {})
        monkeypatch.setattr(nt, "session_scope", lambda: contextlib.nullcontext(object()))
        calls = []
        monkeypatch.setattr(
            nt, "_offload", lambda a, client=None, params=None, job_folder_cache=None: calls.append(a) or "offloaded"
        )
        aid = str(uuid.uuid4())
        held.add(f"nas_offload:{aid}")
        assert nt.offload_asset_batch.run([aid]) == {aid: "in_flight"} and calls == []
        held.clear()
        assert nt.offload_asset_batch.run([aid]) == {aid: "offloaded"}
        assert held == set(), "the lock must be released when the work is done"


class _WrappedClient:
    """A real local-NAS client that lets a test interfere at chosen moments."""

    def __init__(self, params):
        self.inner = open_client(params)

    def __enter__(self):
        self.inner.connect()
        return self

    def __exit__(self, *a):
        self.inner.close()

    def size(self, rel):
        return self.inner.size(rel)

    def delete(self, rel):
        self.inner.delete(rel)

    def upload(self, local, rel):
        self.inner.upload(local, rel)


class TestNoLeftoversOnTheNas:
    def _remote(self, world, name="images/x.jpg"):
        return world.nas / f"data_automation/{world.job}/coco/{name}"

    def test_local_file_vanishing_mid_upload_leaves_nothing_on_the_nas(self, world):
        class VanishingLocal(_WrappedClient):
            def upload(self, local, rel):
                self.inner.upload(local, rel)
                Path(local).unlink()  # the asset was rejected and its local file removed meanwhile

        world.monkeypatch.setattr(nt, "open_client", VanishingLocal)
        assert nt._offload(world.asset.id) == "local_missing"
        assert not self._remote(world).exists()
        assert not world.asset.nas_path.startswith("nas://")

    def test_a_connection_error_after_a_partial_upload_removes_the_partial_file(self, world):
        class DropsAfterUpload(_WrappedClient):
            def size(self, rel):
                raise NasConnectionError("connection reset")

        world.monkeypatch.setattr(nt, "open_client", DropsAfterUpload)
        with pytest.raises(TransientConnectorError):
            nt._offload(world.asset.id)
        assert not self._remote(world).exists()
        assert world.local_img.exists()

    def test_annotation_failure_removes_the_image_copy_too(self, world):
        class AnnotationFails(_WrappedClient):
            def upload(self, local, rel):
                if "annotations" in rel:
                    raise NasConnectionError("connection reset")
                self.inner.upload(local, rel)

        world.monkeypatch.setattr(nt, "open_client", AnnotationFails)
        with pytest.raises(TransientConnectorError):
            nt._offload(world.asset.id)
        assert not self._remote(world).exists()

    def test_rejected_between_our_commit_and_the_recheck_removes_the_copy(self, world):
        # A trim that read the asset before we committed deleted only its old local path.
        world.monkeypatch.setattr(nt, "_still_kept", lambda asset_uuid, remote: False)
        assert nt._offload(world.asset.id) == "raced_after_commit"
        assert not self._remote(world).exists()
        assert not self._remote(world, "annotations/x.txt").exists()
        assert not world.local_img.exists() and not world.local_ann.exists()

    def test_a_kept_image_survives_the_recheck(self, world):
        assert nt._offload(world.asset.id) == "offloaded"
        assert self._remote(world).exists()
        assert nt._still_kept(world.asset.id, world.asset.nas_path)

    def test_still_kept_is_false_once_the_asset_is_rejected_or_gone(self, world):
        nt._offload(world.asset.id)
        remote = world.asset.nas_path
        world.asset.status = AssetStatus.RELEVANCE_REJECTED
        assert not nt._still_kept(world.asset.id, remote)
        world.asset.status = AssetStatus.RELEVANCE_PASSED
        assert not nt._still_kept(world.asset.id, "nas://somewhere/else.jpg")
        world.db.asset = None
        assert not nt._still_kept(world.asset.id, remote)


class TestLeakSweeper:
    def _leak(self, world, status):
        """An asset that was offloaded and then rejected, whose NAS files were left behind."""
        nt._offload(world.asset.id)
        world.asset.status = status
        img, ann = world.asset.nas_path, world.asset.annotation_nas_path
        world.db.rows = [(world.asset.id, img, ann)]
        return img, ann

    @pytest.mark.parametrize("status", [AssetStatus.RELEVANCE_REJECTED, AssetStatus.DUPLICATE])
    def test_removes_the_nas_copies_of_images_that_are_no_longer_kept(self, world, status):
        img, ann = self._leak(world, status)
        assert (world.nas / img[len("nas://"):]).exists()
        assert nt.sweep_leaked_nas_files() == 1
        assert not (world.nas / img[len("nas://"):]).exists()
        assert not (world.nas / ann[len("nas://"):]).exists()
        assert world.asset.nas_path is None and world.asset.annotation_nas_path is None

    def test_is_idempotent_when_the_files_are_already_gone(self, world):
        img, ann = self._leak(world, AssetStatus.RELEVANCE_REJECTED)
        (world.nas / img[len("nas://"):]).unlink()
        assert nt.sweep_leaked_nas_files() == 1
        assert world.asset.nas_path is None

    def test_defers_and_keeps_the_record_when_the_nas_is_unreachable(self, world):
        img, ann = self._leak(world, AssetStatus.RELEVANCE_REJECTED)
        world.monkeypatch.setattr(asset_files, "get_saved_params", lambda: None)
        assert nt.sweep_leaked_nas_files() == 0
        assert world.asset.nas_path == img and world.asset.annotation_nas_path == ann, "must retry next run"

    def test_a_reinstated_asset_keeps_its_paths(self, world):
        img, ann = self._leak(world, AssetStatus.RELEVANCE_REJECTED)
        original_delete = asset_files.delete

        def delete_then_reinstate(path, strict=False):
            original_delete(path, strict=strict)
            world.asset.status = AssetStatus.RELEVANCE_PASSED

        world.monkeypatch.setattr(asset_files, "delete", delete_then_reinstate)
        nt.sweep_leaked_nas_files()
        assert world.asset.nas_path == img

    def test_nothing_to_do_returns_zero(self, world):
        world.db.rows = []
        assert nt.sweep_leaked_nas_files() == 0


class TestProcessNasUploadQueue:
    def test_skips_when_no_active_storage(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: False)
        res = nt.process_nas_upload_queue.run()
        assert res == {"status": "skipped", "reason": "no_active_storage"}

    def test_skips_during_cooldown(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        from app.storage.nas_writer import is_cooldown_active

        monkeypatch.setattr("app.storage.nas_writer.is_cooldown_active", lambda: True)
        res = nt.process_nas_upload_queue.run()
        assert res == {"status": "skipped", "reason": "cooldown_active"}

    def test_skips_when_lock_held_by_another_worker(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        from app.storage.nas_writer import is_cooldown_active, NasWriterLock

        monkeypatch.setattr("app.storage.nas_writer.is_cooldown_active", lambda: False)
        # Mock lock acquisition failing (another worker active)
        monkeypatch.setattr(NasWriterLock, "acquire", lambda self, timeout_s=0.0: False)

        res = nt.process_nas_upload_queue.run()
        assert res == {"status": "skipped", "reason": "concurrency_lock_held"}

    def test_processes_batches_sequentially_when_lock_acquired(self, monkeypatch):
        monkeypatch.setattr(nt, "is_storage_active", lambda: True)
        from app.storage.nas_writer import is_cooldown_active, NasWriterLock, NasBatchWriter

        monkeypatch.setattr("app.storage.nas_writer.is_cooldown_active", lambda: False)
        monkeypatch.setattr(NasWriterLock, "acquire", lambda self, timeout_s=0.0: True)
        monkeypatch.setattr(NasWriterLock, "release", lambda self: True)
        monkeypatch.setattr(NasWriterLock, "extend", lambda self, ttl: True)

        params = NasConnectionParams(protocol="local", share="/tmp/fake")
        monkeypatch.setattr(nt, "get_active_storage_params", lambda: params)
        monkeypatch.setattr(nt, "open_client", lambda p: contextlib.nullcontext(object()))

        t_ids = [uuid.uuid4(), uuid.uuid4()]
        processed = []

        def mock_dequeue(*args, **kwargs):
            return t_ids.pop(0) if t_ids else None

        monkeypatch.setattr("app.storage.nas_writer.dequeue_nas_batch", mock_dequeue)
        monkeypatch.setattr(nt, "session_scope", lambda: contextlib.nullcontext(object()))
        monkeypatch.setattr(NasBatchWriter, "upload_batch", lambda self, tid, client=None: processed.append(tid))

        res = nt.process_nas_upload_queue.run()
        assert res == {"status": "completed", "processed_batches": 2}
        assert len(processed) == 2


class TestReconcileNasTransfers:
    def test_reconcile_skips_when_no_storage_active(self, monkeypatch):
        monkeypatch.setattr("app.services.nas_config_service.is_storage_active", lambda: False)
        res = nt.reconcile_nas_transfers.run()
        assert res == {"status": "skipped", "reason": "no_active_storage"}

    def test_reconcile_idle_when_no_actionable_transfers(self, monkeypatch):
        monkeypatch.setattr("app.services.nas_config_service.is_storage_active", lambda: True)
        mock_repo = SimpleNamespace(list_actionable=lambda **kw: [], list_pending_cleanups=lambda **kw: [])
        monkeypatch.setattr(nt, "session_scope", lambda: contextlib.nullcontext(object()))
        monkeypatch.setattr("app.db.repositories.image_batch_repository.NasTransferBatchRepository", lambda db: mock_repo)

        res = nt.reconcile_nas_transfers.run()
        assert res == {"status": "idle", "actionable_count": 0, "cleaned_count": 0}

    def test_reconcile_dispatches_queue_when_actionable_transfers_found(self, monkeypatch):
        monkeypatch.setattr("app.services.nas_config_service.is_storage_active", lambda: True)
        mock_transfer = SimpleNamespace(id=uuid.uuid4())
        mock_repo = SimpleNamespace(list_actionable=lambda **kw: [mock_transfer], list_pending_cleanups=lambda **kw: [])
        monkeypatch.setattr(nt, "session_scope", lambda: contextlib.nullcontext(object()))
        monkeypatch.setattr("app.db.repositories.image_batch_repository.NasTransferBatchRepository", lambda db: mock_repo)

        dispatched = []
        monkeypatch.setattr(nt.process_nas_upload_queue, "delay", lambda *a, **kw: dispatched.append(True))

        res = nt.reconcile_nas_transfers.run()
        assert res == {"status": "dispatched", "actionable_count": 1, "cleaned_count": 0}
        assert len(dispatched) == 1
