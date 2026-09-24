from types import SimpleNamespace

import app.services.standardization_service as ss


def _asset(nas_path, annotation=None):
    return SimpleNamespace(id="a1", nas_path=nas_path, annotation_nas_path=annotation)


def test_a_stale_local_path_is_replaced_by_the_offloaded_nas_path_before_deleting(monkeypatch):
    """The trim loaded the asset while it was still local; the upload committed a nas:// path
    afterwards. The delete must act on the current path or the NAS copy is left behind."""
    asset = _asset("/local/staging/x.jpg", "/local/staging/x.txt")

    class Session:
        def refresh(self, obj, attribute_names):
            assert set(attribute_names) == {"nas_path", "annotation_nas_path"}
            obj.nas_path, obj.annotation_nas_path = "nas://data/j/coco/images/x.jpg", "nas://data/j/coco/annotations/x.txt"

    monkeypatch.setattr(ss, "object_session", lambda obj: Session())
    deleted = []
    monkeypatch.setattr(ss.asset_files, "delete", lambda path, strict=False: deleted.append(path))
    ss.delete_asset_files(asset)
    assert deleted == ["nas://data/j/coco/images/x.jpg", "nas://data/j/coco/annotations/x.txt"]


def test_an_asset_without_a_session_uses_the_paths_it_already_has(monkeypatch):
    asset = _asset("/local/staging/x.jpg")
    deleted = []
    monkeypatch.setattr(ss.asset_files, "delete", lambda path, strict=False: deleted.append(path))
    ss.delete_asset_files(asset)  # SimpleNamespace is not a mapped object: refresh must fail open
    assert deleted == ["/local/staging/x.jpg"]


def test_a_failing_refresh_never_stops_the_delete(monkeypatch):
    class Session:
        def refresh(self, *a, **k):
            raise RuntimeError("connection lost")

    monkeypatch.setattr(ss, "object_session", lambda obj: Session())
    deleted = []
    monkeypatch.setattr(ss.asset_files, "delete", lambda path, strict=False: deleted.append(path))
    ss.delete_asset_files(_asset("/local/x.jpg"))
    assert deleted == ["/local/x.jpg"]


def test_a_delete_failure_is_logged_not_raised(monkeypatch):
    def boom(path, strict=False):
        raise RuntimeError("nas exploded")

    monkeypatch.setattr(ss.asset_files, "delete", boom)
    ss.delete_asset_files(_asset("nas://data/x.jpg"))  # must not raise into the filtering pipeline
