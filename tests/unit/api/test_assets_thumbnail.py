from pathlib import Path

from app.api.v1.assets import _thumbnail_path_for


class TestThumbnailPathFor:
    def test_derives_sibling_thumb_path(self):
        assert _thumbnail_path_for(Path("/nas/job1/coco/images/coco_val2017_143961.jpg")) == Path(
            "/nas/job1/coco/images/coco_val2017_143961.thumb.jpg"
        )

    def test_path_with_no_extension(self):
        assert _thumbnail_path_for(Path("/nas/job1/asset_without_ext")) == Path(
            "/nas/job1/asset_without_ext.thumb.jpg"
        )

    def test_path_with_dots_in_stem(self):
        assert _thumbnail_path_for(Path("/nas/job1/frame.v2.final.jpg")) == Path(
            "/nas/job1/frame.v2.final.thumb.jpg"
        )

    def test_stays_in_same_directory(self):
        result = _thumbnail_path_for(Path("/nas/job1/davis/images/davis_dog-agility_00000.jpg"))
        assert result.parent == Path("/nas/job1/davis/images")
