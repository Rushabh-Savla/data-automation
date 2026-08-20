"""Tests for PascalVocConnector's ImageSets-based fast lookup (with XML-scan
fallback) and native VOC_XML fetch (parse_voc_xml reused verbatim)."""

from pathlib import Path

from PIL import Image

from app.connectors import pascal_voc_connector as voc_mod
from app.connectors.base import SourceItem
from app.connectors.pascal_voc_connector import PascalVocConnector, _match_classes
from app.domain.enums import SourceType
from app.services.format_conversion import parse_voc_xml

_VOC_XML = """<annotation>
  <size><width>500</width><height>375</height></size>
  <object><name>dog</name><bndbox><xmin>10</xmin><ymin>20</ymin><xmax>110</xmax><ymax>220</ymax></bndbox></object>
</annotation>"""


def _build_fake_devkit_with_imagesets(tmp_path: Path) -> Path:
    devkit_dir = tmp_path / "VOCdevkit" / "VOC2012"
    (devkit_dir / "Annotations").mkdir(parents=True)
    (devkit_dir / "JPEGImages").mkdir(parents=True)
    (devkit_dir / "ImageSets" / "Main").mkdir(parents=True)
    (devkit_dir / "Annotations" / "000001.xml").write_text(_VOC_XML)
    Image.new("RGB", (500, 375)).save(devkit_dir / "JPEGImages" / "000001.jpg")
    (devkit_dir / "ImageSets" / "Main" / "dog_trainval.txt").write_text("000001  1\n000002 -1\n")
    return devkit_dir


class TestMatchClasses:
    def test_matches_class_name(self):
        assert _match_classes("a dog running") == {"dog"}

    def test_no_match_returns_empty_set(self):
        assert _match_classes("xylophone") == set()


class TestDiscover:
    def test_uses_imagesets_for_lookup(self, monkeypatch, tmp_path):
        devkit_dir = _build_fake_devkit_with_imagesets(tmp_path)
        monkeypatch.setattr(voc_mod, "_ensure_dataset_cached", lambda: devkit_dir)

        connector = PascalVocConnector()
        page = connector.discover("dog", cursor=None, limit=200)

        assert page.error_reason is None
        image_ids = {iid for item in page.items for iid in item.external_ref["image_ids"]}
        assert image_ids == {"000001"}  # 000002 excluded (flag -1 = absent)

    def test_no_match_returns_error_reason(self, monkeypatch, tmp_path):
        devkit_dir = _build_fake_devkit_with_imagesets(tmp_path)
        monkeypatch.setattr(voc_mod, "_ensure_dataset_cached", lambda: devkit_dir)

        connector = PascalVocConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class TestFetch:
    def test_copies_native_voc_xml_annotation(self, monkeypatch, tmp_path):
        devkit_dir = _build_fake_devkit_with_imagesets(tmp_path)
        monkeypatch.setattr(voc_mod, "_ensure_dataset_cached", lambda: devkit_dir)

        item = SourceItem(
            source_type=SourceType.PASCAL_VOC,
            external_ref={"image_ids": ["000001"]},
            license_hint="test license",
            dataset_identifier="pascal-voc2012",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = PascalVocConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "voc_xml"
        assert draft.source_item_id == "voc2012_000001"

        width, height, boxes = parse_voc_xml(draft.annotation_tmp_path)
        assert (width, height) == (500, 375)
        assert len(boxes) == 1
        assert boxes[0].class_name == "dog"
        assert (boxes[0].xmin, boxes[0].ymin) == (10, 20)
