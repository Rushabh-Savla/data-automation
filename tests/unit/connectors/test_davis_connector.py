"""Tests for DavisConnector's mask->bbox reduction and sequence-name-based
matching (DAVIS has no semantic class vocabulary of its own)."""

from pathlib import Path

import numpy as np
from PIL import Image

from app.connectors import davis_connector as davis_mod
from app.connectors.base import SourceItem
from app.connectors.davis_connector import DavisConnector, _evenly_sample, _mask_to_boxes, _match_sequences
from app.domain.enums import SourceType


def _build_fake_dataset(tmp_path: Path) -> Path:
    davis_dir = tmp_path / "DAVIS"
    seq_img_dir = davis_dir / "JPEGImages" / "480p" / "dog"
    seq_mask_dir = davis_dir / "Annotations" / "480p" / "dog"
    seq_img_dir.mkdir(parents=True)
    seq_mask_dir.mkdir(parents=True)

    Image.new("RGB", (100, 80)).save(seq_img_dir / "00000.jpg")

    mask = np.zeros((80, 100), dtype=np.uint8)
    mask[10:30, 20:50] = 1  # object id 1: x in [20,49], y in [10,29]
    Image.fromarray(mask, mode="P").save(seq_mask_dir / "00000.png")

    return davis_dir


def _build_two_sequence_dataset(tmp_path: Path) -> Path:
    davis_dir = tmp_path / "DAVIS"
    for seq, frames in (("dog", 4), ("cat-girl", 5)):
        img_dir = davis_dir / "JPEGImages" / "480p" / seq
        img_dir.mkdir(parents=True)
        for i in range(frames):
            Image.new("RGB", (100, 80)).save(img_dir / f"{i:05d}.jpg")
    return davis_dir


def _build_multi_frame_dataset(tmp_path: Path, num_frames: int) -> Path:
    davis_dir = tmp_path / "DAVIS"
    seq_img_dir = davis_dir / "JPEGImages" / "480p" / "cat-girl"
    seq_img_dir.mkdir(parents=True)
    for i in range(num_frames):
        Image.new("RGB", (100, 80)).save(seq_img_dir / f"{i:05d}.jpg")
    return davis_dir


class TestEvenlySample:
    def test_returns_all_items_when_fewer_than_k(self):
        paths = [Path(f"{i}.jpg") for i in range(3)]
        assert _evenly_sample(paths, 10) == paths

    def test_spreads_across_the_full_range_not_just_the_start(self):
        paths = [Path(f"{i:05d}.jpg") for i in range(100)]
        sampled = _evenly_sample(paths, 10)
        assert len(sampled) == 10
        # Not a consecutive burst from the front — the last sampled frame
        # should be near the end of the sequence, not clustered at index 9.
        indices = [paths.index(p) for p in sampled]
        assert indices[0] == 0
        assert indices[-1] >= 90
        assert indices == sorted(indices)  # still chronological order

    def test_never_returns_duplicate_indices(self):
        paths = [Path(f"{i:05d}.jpg") for i in range(37)]
        sampled = _evenly_sample(paths, 10)
        assert len(sampled) == len(set(sampled))


class TestMatchSequences:
    def test_matches_sequence_name(self):
        assert _match_sequences("a dog running", ["dog", "car-roundabout"]) == {"dog"}

    def test_no_match_returns_empty_set(self):
        assert _match_sequences("xylophone", ["dog", "car-roundabout"]) == set()

    def test_multi_term_query_matches_each_term(self):
        # Regression: "car and person" was one substring, matching nothing.
        sequences = ["car-roundabout", "classic-car", "dancing", "dog", "boat"]
        assert _match_sequences("car and person", sequences) == {"car-roundabout", "classic-car", "dancing"}

    def test_plural_terms_match_name_tokens(self):
        assert _match_sequences("cars", ["car-shadow", "dogs-jump", "boat"]) == {"car-shadow"}
        assert _match_sequences("dogs", ["dog", "dogs-jump", "boat"]) == {"dog", "dogs-jump"}

    def test_person_maps_to_person_sequences_and_skips_missing_names(self):
        # "libby" is a dog and "color-run" a blur of hands, so neither is mapped;
        # "dancing" is. Mapped names absent from the cache are ignored.
        sequences = ["dancing", "libby", "color-run", "boat"]
        assert _match_sequences("person", sequences) == {"dancing"}
        assert _match_sequences("people", ["boat"]) == set()

    def test_cars_whose_names_do_not_say_so_are_mapped(self):
        assert _match_sequences("car", ["drift-turn", "rallye", "boat"]) == {"drift-turn", "rallye"}

    def test_activity_word_does_not_pull_in_unrelated_sequences(self):
        # "running" is an activity, not a subject: it must not drag in
        # "lady-running" and dilute the dog frames.
        sequences = ["dog", "lady-running", "boat"]
        assert _match_sequences("a dog running", sequences) == {"dog"}
        assert _match_sequences("running", sequences) == {"lady-running"}


class TestMaskToBoxes:
    def test_bounding_rect_of_labeled_region(self, tmp_path):
        mask_path = tmp_path / "mask.png"
        mask = np.zeros((80, 100), dtype=np.uint8)
        mask[10:30, 20:50] = 1
        Image.fromarray(mask, mode="P").save(mask_path)

        boxes = _mask_to_boxes(mask_path, "dog")
        assert len(boxes) == 1
        b = boxes[0]
        assert b.class_name == "dog"
        assert (b.xmin, b.ymin, b.xmax, b.ymax) == (20.0, 10.0, 50.0, 30.0)

    def test_background_only_mask_yields_no_boxes(self, tmp_path):
        mask_path = tmp_path / "empty.png"
        Image.fromarray(np.zeros((10, 10), dtype=np.uint8), mode="P").save(mask_path)
        assert _mask_to_boxes(mask_path, "dog") == []


class TestDiscover:
    def test_matches_sequence_name_and_shards(self, monkeypatch, tmp_path):
        davis_dir = _build_fake_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        connector = DavisConnector()
        page = connector.discover("a dog running", cursor=None, limit=200)

        assert page.error_reason is None
        frames = [f for item in page.items for f in item.external_ref["frames"]]
        assert frames == [{"sequence": "dog", "frame": "00000"}]

    def test_a_query_no_sequence_names_scans_instead_of_failing(self, monkeypatch, tmp_path):
        # DAVIS has ~90 sequence names, so most queries name none of them. Offer frames
        # anyway and let the relevance filter decide from the image.
        davis_dir = _build_fake_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        connector = DavisConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)

        assert page.error_reason is None
        assert [i.external_ref["unlabelled"] for i in page.items] == [True]
        assert [f["sequence"] for i in page.items for f in i.external_ref["frames"]] == ["dog"]

    def test_labelled_sequences_come_first_then_the_others_are_scanned(self, monkeypatch, tmp_path):
        davis_dir = _build_two_sequence_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        page = DavisConnector().discover("dog", cursor=None, limit=200)

        by_flag = {
            flag: sorted({f["sequence"] for i in page.items if i.external_ref["unlabelled"] is flag for f in i.external_ref["frames"]})
            for flag in (False, True)
        }
        assert by_flag == {False: ["dog"], True: ["cat-girl"]}

    def test_the_scan_never_repeats_a_matched_sequence(self, monkeypatch, tmp_path):
        davis_dir = _build_two_sequence_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        page = DavisConnector().discover("dog", cursor=None, limit=200)

        frames = [(f["sequence"], f["frame"]) for i in page.items for f in i.external_ref["frames"]]
        assert len(frames) == len(set(frames))

    def test_the_scan_is_bounded_by_the_limit(self, monkeypatch, tmp_path):
        davis_dir = _build_two_sequence_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        page = DavisConnector().discover("xylophone", cursor=None, limit=3)

        assert sum(len(i.external_ref["frames"]) for i in page.items) == 3

    def test_explicit_sequences_are_exact_and_never_scanned(self, monkeypatch, tmp_path):
        davis_dir = _build_two_sequence_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        page = DavisConnector().discover("anything", cursor=None, limit=200, explicit_categories=["dog"])

        assert {i.external_ref["unlabelled"] for i in page.items} == {False}

    def test_explicit_sequences_that_match_nothing_still_report_it(self, monkeypatch, tmp_path):
        davis_dir = _build_two_sequence_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        page = DavisConnector().discover("anything", cursor=None, limit=200, explicit_categories=["unicorn"])

        assert page.items == [] and "unicorn" in page.error_reason

    def test_synonyms_and_whole_words_reach_the_sequence(self):
        assert _match_sequences("puppy", ["dog", "boat"]) == {"dog"}
        assert _match_sequences("a business trip", ["bus", "boat"]) == set()  # "bus" is not in "business"

    def test_samples_frames_across_the_whole_sequence_not_consecutively(self, monkeypatch, tmp_path):
        # Regression test for the near-duplicate-flood bug: grabbing the
        # first N consecutive frames of a video is a wall of near-identical
        # images (confirmed in production — a real job's asset grid showed
        # dozens of frames 20-71+ from one "cat-girl" sequence). A capped
        # per-source budget (10, via app/tasks/_helpers.py::per_source_cap)
        # should come back spread across the full 100-frame sequence.
        davis_dir = _build_multi_frame_dataset(tmp_path, num_frames=100)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        connector = DavisConnector()
        page = connector.discover("cat-girl", cursor=None, limit=10)

        frame_numbers = sorted(int(f["frame"]) for item in page.items for f in item.external_ref["frames"])
        assert len(frame_numbers) == 10
        assert frame_numbers[0] < 5
        assert frame_numbers[-1] >= 90


class TestFetch:
    def test_converts_mask_to_yolo_box_labeled_by_sequence(self, monkeypatch, tmp_path):
        davis_dir = _build_fake_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        item = SourceItem(
            source_type=SourceType.DAVIS,
            external_ref={"frames": [{"sequence": "dog", "frame": "00000"}]},
            license_hint="test license",
            dataset_identifier="davis-2017-trainval-480p",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = DavisConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["dog"]
        assert draft.source_item_id == "davis_dog_00000"
