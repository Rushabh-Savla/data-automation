"""canonical_selector — Superior representative image selection for duplicates.

When two duplicate images are detected, evaluates:
  - Resolution (width * height)
  - Image format / compression quality (lossless PNG/TIFF > JPG > WEBP)
  - Annotation richness (bounding boxes, classes, descriptive metadata)

Selects the higher-quality asset as the canonical representation, marks the other
as duplicate, and preserves complete source provenance and annotation relationships.
"""

from __future__ import annotations

import math
import threading
from typing import Any

from app.db.models.asset import Asset
from app.domain.enums import AnnotationFormat, AssetStatus


class CanonicalSelector:
    """Evaluates and selects the superior representative asset when duplicates are identified."""

    _lock = threading.RLock()

    @staticmethod
    def evaluate_quality(asset: Asset | dict[str, Any]) -> float:
        """Computes a composite quality and completeness score for an asset."""
        width = getattr(asset, "width", None) if hasattr(asset, "width") else asset.get("width")
        height = getattr(asset, "height", None) if hasattr(asset, "height") else asset.get("height")
        file_format = (
            getattr(asset, "file_format", "") if hasattr(asset, "file_format") else asset.get("file_format", "")
        ) or ""
        classes = (
            getattr(asset, "classes", []) if hasattr(asset, "classes") else asset.get("classes", [])
        ) or []
        ann_format = (
            getattr(asset, "annotation_format", AnnotationFormat.NONE)
            if hasattr(asset, "annotation_format")
            else asset.get("annotation_format", AnnotationFormat.NONE)
        )
        metadata = (
            getattr(asset, "asset_metadata", {})
            if hasattr(asset, "asset_metadata")
            else asset.get("asset_metadata", {})
        ) or {}

        score = 0.0

        # 1. Resolution Score: log10(pixels) * 10
        w = width or 100
        h = height or 100
        pixels = max(1, w * h)
        score += math.log10(pixels) * 10.0

        # 2. File Format / Compression Quality
        fmt = str(file_format).lower().lstrip(".")
        if fmt in ("png", "tiff", "tif"):
            score += 15.0  # Lossless preferred
        elif fmt in ("jpg", "jpeg"):
            score += 10.0  # Standard photographic
        elif fmt in ("webp",):
            score += 6.0
        else:
            score += 2.0

        # 3. Annotation and Metadata Completeness
        if classes and len(classes) > 0:
            score += 25.0  # Contains labeled classes
        if ann_format and str(ann_format) != AnnotationFormat.NONE.value:
            score += 20.0  # Has linked annotation file
        if metadata and len(metadata) > 0:
            if metadata.get("segmentation_masks"):
                score += 40.0  # Segmentation masks offer highest utility
            if metadata.get("bounding_boxes"):
                score += 30.0  # Object detection bounding boxes
            if metadata.get("keypoints"):
                score += 30.0  # Pose / keypoint annotations
            score += min(10.0, len(metadata) * 2.0)

        return score

    @classmethod
    def select_canonical(cls, asset_a: Asset, asset_b: Asset) -> tuple[Asset, Asset]:
        """Compares two assets and returns (winner_canonical, loser_duplicate)."""
        score_a = cls.evaluate_quality(asset_a)
        score_b = cls.evaluate_quality(asset_b)

        if score_a >= score_b:
            return asset_a, asset_b
        return asset_b, asset_a

    @classmethod
    def apply_duplicate_relationship(cls, canonical: Asset, duplicate: Asset) -> None:
        """Applies duplicate relationship while preventing cycles and preserving rich annotations.

        Preserves:
          - Bounding boxes (tagged with source_asset_id to avoid collisions)
          - Segmentation masks (tagged with source_asset_id)
          - Keypoints (tagged with source_asset_id)
          - Class labels (union of distinct labels)
          - Annotation IDs and original annotation file paths
          - Full source provenance and traceability
        """
        if canonical.id == duplicate.id:
            return

        from datetime import timezone
        from datetime import datetime

        with cls._lock:
            # 1. Prevent circular references and resolve target root canonical
            target_canonical_id = canonical.id

            if getattr(canonical, "is_duplicate_of", None) == duplicate.id:
                canonical.is_duplicate_of = None
                if canonical.status == AssetStatus.DUPLICATE:
                    canonical.status = AssetStatus.PROMOTED
            elif getattr(canonical, "is_duplicate_of", None) is not None:
                # Canonical is already a duplicate of a higher root; point duplicate directly to root
                target_canonical_id = canonical.is_duplicate_of
            else:
                canonical.is_duplicate_of = None

            # 2. Update duplicate record without destroying its original data
            duplicate.status = AssetStatus.DUPLICATE
            duplicate.is_duplicate_of = target_canonical_id
            dup_meta = dict(getattr(duplicate, "asset_metadata", None) or {})
            dup_meta["canonical_asset_id"] = str(target_canonical_id)
            dup_meta["merged_at"] = datetime.now(timezone.utc).isoformat()
            duplicate.asset_metadata = dup_meta

            # 3. Canonical metadata and provenance preservation
            can_meta = dict(getattr(canonical, "asset_metadata", None) or {})
            merged_provenance = list(can_meta.get("merged_provenance", []))

            # 4. Class labels: union of distinct labels without destructive overwrite
            can_classes = list(getattr(canonical, "classes", None) or [])
            dup_classes = list(getattr(duplicate, "classes", None) or [])
            canonical.classes = list(dict.fromkeys(can_classes + dup_classes))

            # 5. Annotation file inheritance if canonical has none
            can_has_file = (
                getattr(canonical, "annotation_nas_path", None)
                and getattr(canonical, "annotation_format", AnnotationFormat.NONE) != AnnotationFormat.NONE
            )
            dup_has_file = (
                getattr(duplicate, "annotation_nas_path", None)
                and getattr(duplicate, "annotation_format", AnnotationFormat.NONE) != AnnotationFormat.NONE
            )
            if not can_has_file and dup_has_file:
                canonical.annotation_nas_path = duplicate.annotation_nas_path
                canonical.annotation_format = duplicate.annotation_format

            # 6. Rich Annotation Preservation: Bounding boxes, Segmentation masks, Keypoints
            # Bounding boxes
            can_boxes = list(can_meta.get("bounding_boxes", []))
            dup_boxes = list(dup_meta.get("bounding_boxes", []))
            for b in can_boxes:
                if isinstance(b, dict):
                    b.setdefault("source_asset_id", str(canonical.id))
                    b.setdefault("source_type", str(getattr(canonical, "source_type", "")))
            merged_boxes = list(can_boxes)
            for b in dup_boxes:
                b_copy = dict(b) if isinstance(b, dict) else {"data": b}
                b_copy["source_asset_id"] = str(duplicate.id)
                b_copy["source_type"] = str(getattr(duplicate, "source_type", ""))
                merged_boxes.append(b_copy)
            if merged_boxes:
                can_meta["bounding_boxes"] = merged_boxes

            # Segmentation masks
            can_masks = list(can_meta.get("segmentation_masks", []))
            dup_masks = list(dup_meta.get("segmentation_masks", []))
            for m in can_masks:
                if isinstance(m, dict):
                    m.setdefault("source_asset_id", str(canonical.id))
                    m.setdefault("source_type", str(getattr(canonical, "source_type", "")))
            merged_masks = list(can_masks)
            for m in dup_masks:
                m_copy = dict(m) if isinstance(m, dict) else {"data": m}
                m_copy["source_asset_id"] = str(duplicate.id)
                m_copy["source_type"] = str(getattr(duplicate, "source_type", ""))
                merged_masks.append(m_copy)
            if merged_masks:
                can_meta["segmentation_masks"] = merged_masks

            # Keypoints
            can_kp = list(can_meta.get("keypoints", []))
            dup_kp = list(dup_meta.get("keypoints", []))
            for k in can_kp:
                if isinstance(k, dict):
                    k.setdefault("source_asset_id", str(canonical.id))
                    k.setdefault("source_type", str(getattr(canonical, "source_type", "")))
            merged_kp = list(can_kp)
            for k in dup_kp:
                k_copy = dict(k) if isinstance(k, dict) else {"data": k}
                k_copy["source_asset_id"] = str(duplicate.id)
                k_copy["source_type"] = str(getattr(duplicate, "source_type", ""))
                merged_kp.append(k_copy)
            if merged_kp:
                can_meta["keypoints"] = merged_kp

            # Annotation IDs
            ann_ids = list(can_meta.get("annotation_ids", []))
            dup_ann_ids = list(dup_meta.get("annotation_ids", []))
            if getattr(duplicate, "source_item_id", None):
                dup_ann_ids.append(f"item:{duplicate.source_item_id}")
            can_meta["annotation_ids"] = list(dict.fromkeys(ann_ids + dup_ann_ids))

            # 7. Record complete source provenance
            merged_provenance.append({
                "duplicate_asset_id": str(duplicate.id),
                "source_type": str(getattr(duplicate, "source_type", "")),
                "source_item_id": getattr(duplicate, "source_item_id", None),
                "source_url": getattr(duplicate, "source_url", None),
                "license": getattr(duplicate, "license", None),
                "dataset_identifier": getattr(duplicate, "dataset_identifier", None),
                "annotation_format": str(getattr(duplicate, "annotation_format", "")),
                "annotation_nas_path": getattr(duplicate, "annotation_nas_path", None),
                "classes": dup_classes,
                "box_count": len(dup_boxes),
                "mask_count": len(dup_masks),
                "keypoint_count": len(dup_kp),
            })
            can_meta["merged_provenance"] = merged_provenance
            canonical.asset_metadata = can_meta

