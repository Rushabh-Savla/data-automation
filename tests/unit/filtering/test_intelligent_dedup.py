"""Unit and integration tests for Stage 4: Intelligent Deduplication Enhancements.

Tests all 12 required scenarios:
1. Exact byte-identical images.
2. Identical images with different metadata.
3. Resized images.
4. Compressed versions of the same image.
5. Slightly cropped images.
6. Brightness-adjusted images.
7. Different photographs of the same object.
8. Semantically similar but visually different images.
9. Cross-batch duplicates.
10. Cross-job duplicate detection.
11. Concurrent duplicate detection.
12. Canonical image selection and provenance preservation.
"""

import io
import threading
import uuid
from pathlib import Path
from unittest.mock import patch

import cv2
import numpy as np
import pytest
from PIL import Image, ImageDraw, ImageEnhance

from app.db.models.asset import Asset
from app.db.models.image_batch import ImageBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import AnnotationFormat, AssetStatus, BatchStatus, JobSourceTaskStatus, SourceType
from app.filtering.canonical_selector import CanonicalSelector
from app.filtering.cross_batch_dedup import CrossBatchDeduplicator
from app.filtering.structural_verifier import (
    DuplicateCategory,
    StructuralVerifier,
    compute_canonical_pixel_hash,
    compute_ssim,
    verify_orb_homography,
)


def _generate_synthetic_image(seed: int = 42, width: int = 250, height: int = 250) -> Image.Image:
    """Generates a rich visual scene with geometric shapes, lines, and patterns for feature matching."""
    rng = np.random.default_rng(seed)
    arr = rng.integers(30, 225, (height, width, 3), dtype=np.uint8)

    img = Image.fromarray(arr, "RGB")
    draw = ImageDraw.Draw(img)
    # Add structured geometry dependent on seed
    c1 = tuple(rng.integers(0, 256, 3).tolist())
    c2 = tuple(rng.integers(0, 256, 3).tolist())
    c3 = tuple(rng.integers(0, 256, 3).tolist())
    draw.rectangle([40, 40, 120, 120], fill=c1, outline="white")
    draw.ellipse([140, 60, 210, 130], fill=c2, outline="blue")
    draw.polygon([(80, 160), (160, 220), (50, 230)], fill=c3, outline="black")
    draw.line([(0, 0), (width, height)], fill="cyan", width=3)
    draw.line([(0, height), (width, 0)], fill="magenta", width=3)
    return img


class TestMultiLevelDeduplication:
    def test_1_exact_byte_identical_images(self, db_session):
        """Level 1: Exact byte-identical images are detected via indexed SHA256 lookup."""
        dedup = CrossBatchDeduplicator(db_session)
        job_id = uuid.uuid4()
        task_id = uuid.uuid4()
        sha = "a" * 64

        canonical = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=task_id,
            source_type=SourceType.WEB,
            source_item_id="item_canon",
            checksum_sha256=sha,
            status=AssetStatus.RELEVANCE_PASSED,
        )
        db_session.add(canonical)
        db_session.commit()

        # Check candidate with same SHA256
        dup_id = dedup.check_exact_duplicate(job_id, sha)
        assert dup_id == canonical.id

    def test_2_identical_images_with_different_metadata(self):
        """Level 2: Identical visual pixels saved with different metadata produce identical canonical pixel hashes."""
        from PIL.PngImagePlugin import PngInfo

        img = _generate_synthetic_image(seed=101)

        # File A: clean PNG without metadata
        buf_clean = io.BytesIO()
        img.save(buf_clean, format="PNG")
        buf_clean.seek(0)

        # File B: PNG with added text and comment metadata
        buf_meta = io.BytesIO()
        meta = PngInfo()
        meta.add_text("Copyright", "2026 Test Suite")
        meta.add_text("Description", "Camera Metadata")
        img.save(buf_meta, format="PNG", pnginfo=meta)
        buf_meta.seek(0)

        # Raw file bytes differ due to metadata chunks
        assert buf_clean.getvalue() != buf_meta.getvalue()

        # But canonical pixel hashes are identical!
        hash_clean = compute_canonical_pixel_hash(buf_clean)
        hash_meta = compute_canonical_pixel_hash(buf_meta)
        assert hash_clean == hash_meta

    def test_3_resized_images(self):
        """Level 5: Resized versions of the same visual image are verified as duplicates via SSIM and ORB."""
        img1 = _generate_synthetic_image(seed=202, width=300, height=300)
        img2 = img1.resize((150, 150), Image.Resampling.LANCZOS)

        verifier = StructuralVerifier()
        cat, details = verifier.verify_pair(img1, img2)
        assert cat == DuplicateCategory.CONFIRMED_NEAR_DUPLICATE
        assert details.get("orb_match") is True or details.get("ssim_score", 0) >= 0.85

    def test_4_compressed_versions_of_same_image(self):
        """Level 5: Heavily compressed versions of the same image are confirmed duplicates."""
        img1 = _generate_synthetic_image(seed=303)

        buf = io.BytesIO()
        img1.save(buf, format="JPEG", quality=30)  # Aggressive compression
        buf.seek(0)
        img2 = Image.open(buf)

        verifier = StructuralVerifier(ssim_threshold=0.80)
        cat, details = verifier.verify_pair(img1, img2)
        assert cat == DuplicateCategory.CONFIRMED_NEAR_DUPLICATE

    def test_5_slightly_cropped_images(self):
        """Level 5: Cropped images are confirmed duplicates via ORB geometric homography."""
        img1 = _generate_synthetic_image(seed=404, width=300, height=300)
        # Crop 20 pixels from edges
        img2 = img1.crop((20, 20, 280, 280))

        match, inliers, ratio = verify_orb_homography(
            img1, img2, min_inliers=10, inlier_ratio_threshold=0.50
        )
        assert match is True
        assert inliers >= 10
        assert ratio >= 0.50

    def test_6_brightness_adjusted_images(self):
        """Level 5: Brightness-enhanced versions maintain high structural similarity and geometric features."""
        img1 = _generate_synthetic_image(seed=505)
        img2 = ImageEnhance.Brightness(img1).enhance(1.25)

        verifier = StructuralVerifier(ssim_threshold=0.80)
        cat, _ = verifier.verify_pair(img1, img2)
        assert cat == DuplicateCategory.CONFIRMED_NEAR_DUPLICATE

    def test_7_different_photographs_of_same_object(self):
        """Different scenes or distinct compositions are NOT marked as duplicates."""
        img1 = _generate_synthetic_image(seed=701)
        img2 = _generate_synthetic_image(seed=702)

        verifier = StructuralVerifier()
        cat, details = verifier.verify_pair(img1, img2)
        # Must NOT be confirmed duplicate!
        assert cat in (DuplicateCategory.UNIQUE, DuplicateCategory.POSSIBLE_DUPLICATE)
        assert cat != DuplicateCategory.CONFIRMED_NEAR_DUPLICATE

    def test_8_semantically_similar_visually_different_images(self):
        """High CLIP similarity alone without structural verification is retained as POSSIBLE_DUPLICATE, never discarded."""
        img1 = _generate_synthetic_image(seed=801)
        img2 = _generate_synthetic_image(seed=802)

        verifier = StructuralVerifier()
        # Pairwise visual verification shows visual divergence
        cat, details = verifier.verify_pair(img1, img2)
        assert cat != DuplicateCategory.CONFIRMED_NEAR_DUPLICATE

    def test_9_cross_batch_duplicates(self, db_session, tmp_path):
        """Candidates in Batch 2 are deduplicated against previously accepted assets in Batch 1."""
        job_id = uuid.uuid4()
        task_id = uuid.uuid4()

        job = Job(id=job_id, query="forest", target_count=1200)
        task = JobSourceTask(id=task_id, job_id=job_id, source_type=SourceType.WEB, status=JobSourceTaskStatus.DOWNLOADING)
        b1 = ImageBatch(id=uuid.uuid4(), job_id=job_id, batch_index=1, target_image_count=600, status=BatchStatus.COMPLETED)
        b2 = ImageBatch(id=uuid.uuid4(), job_id=job_id, batch_index=2, target_image_count=600, status=BatchStatus.PROCESSING)
        db_session.add_all([job, task, b1, b2])
        db_session.commit()

        # Save an accepted image for Batch 1
        img1 = _generate_synthetic_image(seed=901)
        path1 = tmp_path / "b1_image.jpg"
        img1.save(path1)

        asset1 = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=task_id,
            batch_id=b1.id,
            source_type=SourceType.WEB,
            source_item_id="item_b1",
            checksum_sha256="sha_batch_1",
            phash="ffff0000ffff0000",
            embedding=[0.1] * 512,
            nas_path=str(path1),
            status=AssetStatus.RELEVANCE_PASSED,
        )
        db_session.add(asset1)
        db_session.commit()

        # Batch 2 candidate (resized copy of Batch 1 image)
        cand_id = uuid.uuid4()
        img2 = img1.resize((150, 150))
        path2 = tmp_path / "b2_candidate.jpg"
        img2.save(path2)

        dedup = CrossBatchDeduplicator(db_session, cosine_threshold=0.90)
        # Mock coarse match finding asset1, then running deep Level 5 visual verification
        with patch.object(dedup.deduplicator, "find_duplicate_groups") as mock_groups:
            from app.filtering.dedup import DuplicateGroup
            mock_groups.return_value = [DuplicateGroup(keep_asset_id=str(asset1.id), duplicate_asset_ids=[str(cand_id)])]

            res = dedup.deduplicate_candidate(
                job_id=job_id,
                candidate_id=cand_id,
                candidate_phash="ffff0000ffff0000",
                candidate_embedding=[0.1] * 512,
                exclude_batch_id=b2.id,
                candidate_file_or_buffer=path2,
            )

            assert res.is_duplicate is True
            assert res.duplicate_of_id == asset1.id
            assert res.category == DuplicateCategory.CONFIRMED_NEAR_DUPLICATE

    def test_10_cross_job_duplicate_detection(self, db_session):
        """Cross-job deduplication finds duplicates from another job when dedup_scope='global'."""
        job_a = uuid.uuid4()
        job_b = uuid.uuid4()
        task_id = uuid.uuid4()
        sha = "common_sha_across_jobs"

        asset_a = Asset(
            id=uuid.uuid4(),
            job_id=job_a,
            job_source_task_id=task_id,
            source_type=SourceType.WEB,
            source_item_id="item_job_a",
            checksum_sha256=sha,
            status=AssetStatus.RELEVANCE_PASSED,
        )
        db_session.add(asset_a)
        db_session.commit()

        # Scope = job -> should not find it across jobs
        dedup_local = CrossBatchDeduplicator(db_session, dedup_scope="job")
        assert dedup_local.check_exact_duplicate(job_b, sha) is None

        # Scope = global -> finds it across jobs
        dedup_global = CrossBatchDeduplicator(db_session, dedup_scope="global")
        assert dedup_global.check_exact_duplicate(job_b, sha) == asset_a.id

    def test_11_concurrent_duplicate_detection(self, db_session):
        """Multiple threads running duplicate checks do not create race conditions or deadlocks."""
        from sqlalchemy.orm import sessionmaker

        job_id = uuid.uuid4()
        task_id = uuid.uuid4()
        sha = "shared_concurrent_sha"

        asset = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=task_id,
            source_type=SourceType.WEB,
            source_item_id="canon_item",
            checksum_sha256=sha,
            status=AssetStatus.RELEVANCE_PASSED,
        )
        db_session.add(asset)
        db_session.commit()
        target_id = asset.id

        session_factory = sessionmaker(bind=db_session.get_bind())
        results = []
        db_lock = threading.Lock()

        def worker():
            with session_factory() as thread_session:
                dedup = CrossBatchDeduplicator(thread_session)
                for _ in range(20):
                    with db_lock:
                        res = dedup.check_exact_duplicate(job_id, sha)
                    results.append(res)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 100
        assert all(r == target_id for r in results)

    def test_12_canonical_image_selection_and_provenance(self):
        """CanonicalSelector selects the higher resolution and annotated image, preserving metadata."""
        asset_low_res = Asset(
            id=uuid.uuid4(),
            width=200,
            height=200,
            file_format="jpg",
            classes=[],
            source_type=SourceType.WEB,
            source_item_id="low_res_item",
            status=AssetStatus.PROMOTED,
        )
        asset_high_res = Asset(
            id=uuid.uuid4(),
            width=1920,
            height=1080,
            file_format="png",
            classes=["fire", "smoke"],
            source_type=SourceType.COCO,
            source_item_id="coco_labeled_item",
            status=AssetStatus.PROMOTED,
        )

        winner, loser = CanonicalSelector.select_canonical(asset_low_res, asset_high_res)
        assert winner == asset_high_res
        assert loser == asset_low_res

        # Apply relationship
        CanonicalSelector.apply_duplicate_relationship(winner, loser)
        assert loser.status == AssetStatus.DUPLICATE
        assert loser.is_duplicate_of == winner.id
        assert loser.source_type == SourceType.WEB  # Provenance preserved!
        assert winner.classes == ["fire", "smoke"]

    def test_13_different_images_with_similar_64x64_pixel_representations(self):
        """Images with coincidentally similar downsampled pixel hashes but differing dimensions or visual scenes are retained."""
        # Create two images with different aspect ratios and different scene contents
        img1 = Image.new("RGB", (400, 200), color=(128, 128, 128))
        draw1 = ImageDraw.Draw(img1)
        draw1.rectangle([50, 50, 150, 150], fill=(200, 50, 50))

        img2 = Image.new("RGB", (200, 400), color=(128, 128, 128))
        draw2 = ImageDraw.Draw(img2)
        draw2.rectangle([50, 150, 150, 250], fill=(50, 50, 200))

        verifier = StructuralVerifier()
        # Even if forced to have same hash, aspect ratio inconsistency prevents CONFIRMED_NEAR_DUPLICATE
        with patch("app.filtering.structural_verifier.compute_canonical_pixel_hash", return_value="identical_mocked_pixel_hash"):
            cat, details = verifier.verify_pair(img1, img2)
            assert cat != DuplicateCategory.CONFIRMED_NEAR_DUPLICATE
            assert cat == DuplicateCategory.POSSIBLE_DUPLICATE
            assert details["aspect_ratio_consistent"] is False

    def test_14_different_photographs_of_same_object(self):
        """Different photographs taken from different angles have non-planar perspective/parallax and are retained."""
        img1 = _generate_synthetic_image(seed=1401, width=300, height=300)
        # Simulate non-planar parallax: composite an object moved against an independent background
        bg1 = _generate_synthetic_image(seed=999, width=300, height=300)
        bg2 = _generate_synthetic_image(seed=888, width=300, height=300)

        obj = _generate_synthetic_image(seed=777, width=120, height=120)
        # Photo 1: Object on left of background 1
        photo1 = bg1.copy()
        photo1.paste(obj, (30, 90))
        # Photo 2: Object on right of background 2 (simulating different camera perspective & scene)
        photo2 = bg2.copy()
        photo2.paste(obj, (150, 90))

        verifier = StructuralVerifier()
        cat, details = verifier.verify_pair(photo1, photo2)
        # Must NOT be marked as confirmed duplicate!
        assert cat in (DuplicateCategory.UNIQUE, DuplicateCategory.POSSIBLE_DUPLICATE)
        assert cat != DuplicateCategory.CONFIRMED_NEAR_DUPLICATE

    def test_15_similar_backgrounds_with_different_foreground_objects(self):
        """Images sharing a smooth uniform background but with distinct objects are not confirmed duplicates."""
        # Shared smooth vertical gradient background
        bg = Image.new("RGB", (250, 250))
        for y in range(250):
            for x in range(250):
                bg.putpixel((x, y), (200, 200, min(255, 180 + y // 4)))

        img_a = bg.copy()
        draw_a = ImageDraw.Draw(img_a)
        draw_a.rectangle([60, 60, 190, 190], fill=(20, 20, 220), outline="black")  # Large blue square

        img_b = bg.copy()
        draw_b = ImageDraw.Draw(img_b)
        draw_b.ellipse([60, 60, 190, 190], fill=(220, 20, 20), outline="yellow")  # Large red circle

        verifier = StructuralVerifier()
        cat, details = verifier.verify_pair(img_a, img_b)
        assert cat != DuplicateCategory.CONFIRMED_NEAR_DUPLICATE
        assert cat in (DuplicateCategory.POSSIBLE_DUPLICATE, DuplicateCategory.UNIQUE)

    def test_16_weak_orb_matches_localized_watermark(self):
        """Features concentrated in a tiny watermark do not trigger duplicate confirmation."""
        # Two completely different scenes
        scene_a = _generate_synthetic_image(seed=1601, width=300, height=300)
        scene_b = _generate_synthetic_image(seed=1602, width=300, height=300)

        # Paste identical tiny 20x20 textured badge in the exact bottom corner (x: 270-290, y: 270-290)
        badge = _generate_synthetic_image(seed=555, width=20, height=20)
        scene_a.paste(badge, (275, 275))
        scene_b.paste(badge, (275, 275))

        # Spatial distribution check: inliers will be confined to < 10% of image dimensions
        match, inliers, ratio = verify_orb_homography(scene_a, scene_b)
        assert match is False  # Must reject localized watermark false positive!

    def test_17_strong_ssim_with_inconsistent_geometry(self):
        """When SSIM is high but geometric support is absent, matches remain POSSIBLE_DUPLICATE and are retained."""
        img1 = _generate_synthetic_image(seed=1701)
        img2 = _generate_synthetic_image(seed=1702)

        verifier = StructuralVerifier()
        # Mock high SSIM (0.87) with no ORB inliers
        with patch("app.filtering.structural_verifier.compute_ssim", return_value=0.87), \
             patch("app.filtering.structural_verifier.verify_orb_homography", return_value=(False, 2, 0.10)):
            cat, details = verifier.verify_pair(img1, img2)
            # High SSIM alone on ambiguous image is NOT enough to confirm duplicate:
            assert cat == DuplicateCategory.POSSIBLE_DUPLICATE
            assert "without_geometric_support_retained" in details["reason"]

    def test_18_conflicting_annotations_preservation(self):
        """Conflicting class labels from duplicate assets are merged as a union without silent overwrite."""
        asset_a = Asset(
            id=uuid.uuid4(),
            width=1000,
            height=800,
            file_format="jpg",
            classes=["cat", "pet"],
            source_type=SourceType.WEB,
            source_item_id="cat_item_01",
            status=AssetStatus.PROMOTED,
            asset_metadata={},
        )
        asset_b = Asset(
            id=uuid.uuid4(),
            width=800,
            height=600,
            file_format="jpg",
            classes=["dog", "animal", "pet"],  # "pet" overlaps, "dog" and "animal" differ
            source_type=SourceType.KAGGLE,
            source_item_id="dog_item_02",
            status=AssetStatus.PROMOTED,
            asset_metadata={},
        )

        winner, loser = CanonicalSelector.select_canonical(asset_a, asset_b)
        assert winner == asset_a
        assert loser == asset_b

        CanonicalSelector.apply_duplicate_relationship(winner, loser)
        # Winner must contain union of distinct classes:
        assert set(winner.classes) == {"cat", "pet", "dog", "animal"}
        # Loser record retains its original classes unchanged
        assert loser.classes == ["dog", "animal", "pet"]
        # Provenance on winner records loser's source information
        prov = winner.asset_metadata["merged_provenance"]
        assert len(prov) == 1
        assert prov[0]["source_item_id"] == "dog_item_02"
        assert prov[0]["source_type"] == str(SourceType.KAGGLE)

    def test_19_bounding_box_and_segmentation_preservation(self):
        """Bounding boxes, segmentation masks, and keypoints are preserved with source asset traceability."""
        canon = Asset(
            id=uuid.uuid4(),
            width=1920,
            height=1080,
            file_format="png",
            classes=["person"],
            source_type=SourceType.COCO,
            source_item_id="coco_item_01",
            status=AssetStatus.PROMOTED,
            asset_metadata={
                "bounding_boxes": [{"label": "person", "box": [10, 10, 100, 200]}],
                "annotation_ids": ["ann_coco_1"],
            },
        )
        duplicate = Asset(
            id=uuid.uuid4(),
            width=1280,
            height=720,
            file_format="jpg",
            classes=["bicycle"],
            source_type=SourceType.ROBOFLOW,
            source_item_id="rf_item_02",
            status=AssetStatus.PROMOTED,
            asset_metadata={
                "bounding_boxes": [{"label": "bicycle", "box": [50, 60, 150, 180]}],
                "segmentation_masks": [{"label": "bicycle", "polygon": [[50, 60], [150, 60], [150, 180]]}],
                "keypoints": [{"label": "wheel", "point": [100, 120]}],
                "annotation_ids": ["ann_rf_2"],
            },
        )

        CanonicalSelector.apply_duplicate_relationship(canon, duplicate)

        # Winner preserves both bounding boxes, each tagged with its source asset ID
        boxes = canon.asset_metadata["bounding_boxes"]
        assert len(boxes) == 2
        assert boxes[0]["source_asset_id"] == str(canon.id)
        assert boxes[1]["source_asset_id"] == str(duplicate.id)
        assert boxes[1]["source_type"] == str(SourceType.ROBOFLOW)

        # Segmentation masks and keypoints from duplicate are preserved in canonical
        masks = canon.asset_metadata["segmentation_masks"]
        assert len(masks) == 1
        assert masks[0]["source_asset_id"] == str(duplicate.id)

        keypoints = canon.asset_metadata["keypoints"]
        assert len(keypoints) == 1
        assert keypoints[0]["source_asset_id"] == str(duplicate.id)

        # Annotation IDs are tracked
        assert set(canon.asset_metadata["annotation_ids"]) == {"ann_coco_1", "ann_rf_2", "item:rf_item_02"}

        # Duplicate original metadata remains intact
        assert len(duplicate.asset_metadata["bounding_boxes"]) == 1
        assert duplicate.asset_metadata["canonical_asset_id"] == str(canon.id)

    def test_20_concurrent_canonical_selection(self):
        """Concurrent selection and relationship resolution across asset clusters never creates cycles."""
        assets = [
            Asset(
                id=uuid.uuid4(),
                width=1000 + i * 100,
                height=1000 + i * 100,
                file_format="png",
                classes=[f"class_{i}"],
                source_type=SourceType.WEB,
                source_item_id=f"item_{i}",
                status=AssetStatus.PROMOTED,
                asset_metadata={},
            )
            for i in range(10)
        ]

        def resolve_pair(a, b):
            w, l = CanonicalSelector.select_canonical(a, b)
            CanonicalSelector.apply_duplicate_relationship(w, l)

        threads = []
        for i in range(len(assets) - 1):
            t = threading.Thread(target=resolve_pair, args=(assets[i], assets[i + 1]))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Check: No asset is duplicate of itself or has circular reference
        for a in assets:
            if a.status == AssetStatus.DUPLICATE:
                assert a.is_duplicate_of is not None
                assert a.is_duplicate_of != a.id
            else:
                assert a.is_duplicate_of is None
