from pathlib import Path

from app.services.format_conversion import (
    BBox,
    ClassRegistry,
    boxes_to_coco_annotations,
    boxes_to_yolo_lines,
    parse_coco_boxes_for_image,
    parse_voc_xml,
    parse_yolo_txt,
)

VOC_XML = """<annotation>
  <size><width>200</width><height>100</height><depth>3</depth></size>
  <object>
    <name>hardhat</name>
    <bndbox><xmin>10</xmin><ymin>20</ymin><xmax>60</xmax><ymax>80</ymax></bndbox>
  </object>
</annotation>
"""


class TestClassRegistry:
    def test_assigns_stable_incrementing_ids(self):
        registry = ClassRegistry()
        assert registry.id_for("person") == 0
        assert registry.id_for("bag") == 1
        assert registry.id_for("person") == 0  # same name -> same id
        assert registry.names == ["person", "bag"]


class TestParseVocXml:
    def test_parses_size_and_boxes(self, tmp_path: Path):
        xml_path = tmp_path / "ann.xml"
        xml_path.write_text(VOC_XML)

        width, height, boxes = parse_voc_xml(xml_path)

        assert (width, height) == (200, 100)
        assert len(boxes) == 1
        assert boxes[0] == BBox(class_name="hardhat", xmin=10, ymin=20, xmax=60, ymax=80)


class TestBoxesToYoloLines:
    def test_normalizes_center_and_size(self):
        boxes = [BBox(class_name="hardhat", xmin=10, ymin=20, xmax=60, ymax=80)]
        registry = ClassRegistry()

        lines = boxes_to_yolo_lines(boxes, width=200, height=100, registry=registry)

        assert len(lines) == 1
        class_id, cx, cy, w, h = lines[0].split()
        assert class_id == "0"
        assert abs(float(cx) - 0.175) < 1e-6   # (10+60)/2 / 200
        assert abs(float(cy) - 0.5) < 1e-6      # (20+80)/2 / 100
        assert abs(float(w) - 0.25) < 1e-6      # (60-10)/200
        assert abs(float(h) - 0.6) < 1e-6       # (80-20)/100

    def test_yolo_round_trip_via_voc(self, tmp_path: Path):
        """VOC -> YOLO -> re-parsed-as-YOLO should reproduce the original box."""
        xml_path = tmp_path / "ann.xml"
        xml_path.write_text(VOC_XML)
        width, height, boxes = parse_voc_xml(xml_path)

        registry = ClassRegistry()
        lines = boxes_to_yolo_lines(boxes, width, height, registry)

        yolo_path = tmp_path / "ann.txt"
        yolo_path.write_text("\n".join(lines))

        reparsed = parse_yolo_txt(yolo_path, width, height, registry.names)
        assert len(reparsed) == 1
        b = reparsed[0]
        assert b.class_name == "hardhat"
        assert abs(b.xmin - 10) < 0.5
        assert abs(b.ymin - 20) < 0.5
        assert abs(b.xmax - 60) < 0.5
        assert abs(b.ymax - 80) < 0.5


class TestCocoConversion:
    def test_parse_and_convert_to_coco_annotations(self):
        coco_data = {
            "images": [{"id": 1, "width": 640, "height": 480}],
            "categories": [{"id": 5, "name": "person"}],
            "annotations": [{"image_id": 1, "category_id": 5, "bbox": [10, 10, 30, 40]}],
        }
        width, height, boxes = parse_coco_boxes_for_image(coco_data, coco_image_id=1)

        assert (width, height) == (640, 480)
        assert len(boxes) == 1
        assert boxes[0].class_name == "person"
        assert boxes[0].xmax == 40  # xmin(10) + w(30)
        assert boxes[0].ymax == 50  # ymin(10) + h(40)

    def test_boxes_to_coco_annotations_assigns_sequential_ids(self):
        registry = ClassRegistry()
        boxes = [
            BBox(class_name="a", xmin=0, ymin=0, xmax=10, ymax=10),
            BBox(class_name="b", xmin=5, ymin=5, xmax=15, ymax=15),
        ]
        anns, next_id = boxes_to_coco_annotations(boxes, image_id=1, next_annotation_id=1, registry=registry)

        assert [a["id"] for a in anns] == [1, 2]
        assert next_id == 3
        assert anns[0]["category_id"] == 0
        assert anns[1]["category_id"] == 1
        assert anns[0]["bbox"] == [0, 0, 10, 10]
