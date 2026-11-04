"""Cross-batch and cross-job deduplication index and verification engine.

Implements the 5-Level Multi-Tier Deduplication Pipeline:
  - Level 1: Exact byte-identical match via indexed SHA256 database lookup.
  - Level 2: Canonical pixel hash match (normalized EXIF orientation + 64x64 raw RGB).
  - Level 3: Perceptual pHash Hamming distance filtering (near-duplicate candidates).
  - Level 4: CLIP embedding cosine similarity matrix comparison (semantic/visual candidates).
  - Level 5: Structural visual verification on ambiguous matches using SSIM and ORB with RANSAC.

Enforces conservative decision strategy:
  - Automatically marks/rejects ONLY confirmed duplicates (EXACT_DUPLICATE, CONFIRMED_NEAR_DUPLICATE).
  - Retains ambiguous matches as POSSIBLE_DUPLICATE or UNIQUE; never discards a distinct photograph.
  - Applies CanonicalSelector to retain the highest-resolution, highest-quality, and most-annotated asset.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.db.models.asset import Asset
from app.domain.enums import AssetStatus
from app.filtering.canonical_selector import CanonicalSelector
from app.filtering.dedup import HybridDeduplicator
from app.filtering.structural_verifier import (
    DuplicateCategory,
    StructuralVerifier,
    compute_canonical_pixel_hash,
)
from app.storage.asset_files import local_path_for_read

logger = logging.getLogger(__name__)


@dataclass
class DuplicateCheckResult:
    is_duplicate: bool
    duplicate_of_id: uuid.UUID | None = None
    reason: str = ""
    category: DuplicateCategory = DuplicateCategory.UNIQUE
    details: dict = field(default_factory=dict)


class CrossBatchDeduplicator:
    """Checks candidate assets against previously accepted assets across batches and jobs."""

    def __init__(
        self,
        db: Session,
        hamming_threshold: int | None = None,
        cosine_threshold: float | None = None,
        ssim_threshold: float | None = None,
        orb_inlier_threshold: float | None = None,
        orb_min_inliers: int | None = None,
        dedup_scope: str | None = None,
    ):
        self.db = db
        self.settings: Settings = get_settings()

        self.hamming_threshold = (
            hamming_threshold
            if hamming_threshold is not None
            else self.settings.dedup_phash_hamming_threshold
        )
        self.cosine_threshold = (
            cosine_threshold
            if cosine_threshold is not None
            else self.settings.dedup_clip_cosine_threshold
        )
        self.ssim_threshold = (
            ssim_threshold
            if ssim_threshold is not None
            else self.settings.dedup_ssim_threshold
        )
        self.orb_inlier_threshold = (
            orb_inlier_threshold
            if orb_inlier_threshold is not None
            else self.settings.dedup_orb_inlier_threshold
        )
        self.orb_min_inliers = (
            orb_min_inliers
            if orb_min_inliers is not None
            else self.settings.dedup_orb_min_inliers
        )
        self.dedup_scope = (
            dedup_scope if dedup_scope is not None else self.settings.dedup_scope
        )

        self.deduplicator = HybridDeduplicator(
            hamming_threshold=self.hamming_threshold,
            cosine_threshold=self.cosine_threshold,
        )
        self.structural_verifier = StructuralVerifier(
            ssim_threshold=self.ssim_threshold,
            orb_inlier_threshold=self.orb_inlier_threshold,
            orb_min_inliers=self.orb_min_inliers,
        )

    def check_exact_duplicate(
        self,
        job_id: uuid.UUID,
        checksum_sha256: str | None,
        cross_job: bool | None = None,
    ) -> uuid.UUID | None:
        """Level 1: Fast database index lookup for exact SHA256 byte-identical match."""
        if not checksum_sha256:
            return None

        use_cross_job = cross_job if cross_job is not None else (self.dedup_scope == "global")

        stmt = select(Asset.id).where(
            Asset.checksum_sha256 == checksum_sha256,
            Asset.status.in_([AssetStatus.RELEVANCE_PASSED, AssetStatus.PROMOTED]),
        )
        if not use_cross_job:
            stmt = stmt.where(Asset.job_id == job_id)

        stmt = stmt.limit(1)
        return self.db.execute(stmt).scalar_one_or_none()

    def check_canonical_pixel_duplicate(
        self,
        job_id: uuid.UUID,
        candidate_img_or_hash: str | Path | bytes,
        candidate_dims: tuple[int, int] | None = None,
        cross_job: bool | None = None,
    ) -> uuid.UUID | None:
        """Level 2: Canonical Pixel Duplicate Screening (candidate fingerprinting).

        Treated as a screening filter, NOT definitive proof of duplicate.
        Verifies aspect ratio and visual similarity before confirming a match.
        """
        cand_img_obj = None
        if isinstance(candidate_img_or_hash, str) and len(candidate_img_or_hash) == 64:
            cand_pixel_hash = candidate_img_or_hash
        else:
            try:
                cand_img_obj = candidate_img_or_hash
                cand_pixel_hash = compute_canonical_pixel_hash(candidate_img_or_hash)
            except Exception:
                return None

        use_cross_job = cross_job if cross_job is not None else (self.dedup_scope == "global")

        # Check existing assets with matching pixel_hash stored in asset_metadata
        stmt = select(Asset).where(
            Asset.status.in_([AssetStatus.RELEVANCE_PASSED, AssetStatus.PROMOTED])
        )
        if not use_cross_job:
            stmt = stmt.where(Asset.job_id == job_id)

        rows = self.db.execute(stmt).scalars().all()
        for ref_asset in rows:
            ref_meta = ref_asset.asset_metadata or {}
            ref_pixel_hash = ref_meta.get("pixel_hash")
            if not ref_pixel_hash or ref_pixel_hash != cand_pixel_hash:
                continue

            # Candidate match found! Now verify dimensions and visual similarity
            # 1. Dimension / Aspect ratio check
            if candidate_dims and ref_asset.width and ref_asset.height:
                cw, ch = candidate_dims
                rw, rh = ref_asset.width, ref_asset.height
                c_ar = float(cw) / float(max(1, ch))
                r_ar = float(rw) / float(max(1, rh))
                ar_ratio = max(c_ar, r_ar) / max(1e-5, min(c_ar, r_ar))
                ar_ratio_rot = max(c_ar, 1.0 / r_ar) / max(1e-5, min(c_ar, 1.0 / r_ar))
                if ar_ratio > 1.25 and ar_ratio_rot > 1.25:
                    logger.info(f"Canonical pixel hash matched but aspect ratios diverged ({c_ar:.2f} vs {r_ar:.2f}); retaining as separate")
                    continue

            # 2. Visual verification if images are available
            if cand_img_obj is not None and ref_asset.nas_path:
                try:
                    ref_local = local_path_for_read(str(ref_asset.nas_path))
                    cat, details = self.structural_verifier.verify_pair(cand_img_obj, Path(ref_local))
                    if cat != DuplicateCategory.CONFIRMED_NEAR_DUPLICATE:
                        logger.info(f"Canonical pixel hash matched but structural verification categorized as {cat}; retaining as separate")
                        continue
                except Exception as e:
                    logger.warning(f"Could not visually verify canonical pixel hash match: {e}; retaining conservatively")
                    continue

            return ref_asset.id

        return None

    def check_perceptual_and_semantic(
        self,
        job_id: uuid.UUID,
        candidate_id: uuid.UUID,
        candidate_phash: str | None,
        candidate_embedding: list[float] | None,
        exclude_batch_id: uuid.UUID | None = None,
        candidate_file_or_buffer: Path | bytes | None = None,
        cross_job: bool | None = None,
    ) -> uuid.UUID | None:
        """Level 3 & 4 with Level 5 Structural Verification fallback for ambiguous matches."""
        result = self.deduplicate_candidate(
            job_id=job_id,
            candidate_id=candidate_id,
            candidate_phash=candidate_phash,
            candidate_embedding=candidate_embedding,
            exclude_batch_id=exclude_batch_id,
            candidate_file_or_buffer=candidate_file_or_buffer,
            cross_job=cross_job,
        )
        if result.is_duplicate:
            return result.duplicate_of_id
        return None

    def deduplicate_candidate(
        self,
        job_id: uuid.UUID,
        candidate_id: uuid.UUID,
        candidate_phash: str | None,
        candidate_embedding: list[float] | None,
        exclude_batch_id: uuid.UUID | None = None,
        candidate_file_or_buffer: Path | bytes | None = None,
        cross_job: bool | None = None,
    ) -> DuplicateCheckResult:
        """Performs full 5-level deduplication on a candidate asset.

        Only flags is_duplicate=True for EXACT_DUPLICATE or CONFIRMED_NEAR_DUPLICATE.
        Ambiguous matches are categorized as POSSIBLE_DUPLICATE and retained.
        """
        if not candidate_phash and not candidate_embedding:
            return DuplicateCheckResult(
                is_duplicate=False,
                category=DuplicateCategory.UNIQUE,
                reason="no_fingerprints",
            )

        use_cross_job = cross_job if cross_job is not None else (self.dedup_scope == "global")

        stmt = select(Asset).where(
            Asset.id != candidate_id,
            Asset.status == AssetStatus.RELEVANCE_PASSED,
        )
        if not use_cross_job:
            stmt = stmt.where(Asset.job_id == job_id)

        if exclude_batch_id is not None:
            stmt = stmt.where((Asset.batch_id != exclude_batch_id) | (Asset.batch_id.is_(None)))

        rows = self.db.execute(stmt).scalars().all()
        if not rows:
            return DuplicateCheckResult(
                is_duplicate=False,
                category=DuplicateCategory.UNIQUE,
                reason="no_reference_assets",
            )

        # 1. Format candidate and references for HybridDeduplicator (Level 3 & 4 coarse filter)
        items = [(str(candidate_id), candidate_phash, candidate_embedding)]
        items.extend([(str(a.id), a.phash, a.embedding) for a in rows])

        groups = self.deduplicator.find_duplicate_groups(items)
        matched_ref_id_str: str | None = None

        for group in groups:
            if str(candidate_id) in group.duplicate_asset_ids:
                matched_ref_id_str = group.keep_asset_id
                break
            if group.keep_asset_id == str(candidate_id) and group.duplicate_asset_ids:
                matched_ref_id_str = group.duplicate_asset_ids[0]
                break

        if not matched_ref_id_str:
            return DuplicateCheckResult(
                is_duplicate=False,
                category=DuplicateCategory.UNIQUE,
                reason="no_perceptual_or_clip_match",
            )

        # Coarse match found! Candidate is potentially a near-duplicate.
        matched_ref_uuid = uuid.UUID(matched_ref_id_str)
        matched_asset = next((a for a in rows if a.id == matched_ref_uuid), None)

        if matched_asset is None:
            return DuplicateCheckResult(
                is_duplicate=False,
                category=DuplicateCategory.UNIQUE,
                reason="reference_asset_missing",
            )

        # 2. Level 5: Structural Visual Verification
        # If image bytes are available, run SSIM and ORB RANSAC to avoid false positives
        if candidate_file_or_buffer is not None and matched_asset.nas_path:
            try:
                ref_local_path = local_path_for_read(str(matched_asset.nas_path))
                category, details = self.structural_verifier.verify_pair(
                    candidate_file_or_buffer, Path(ref_local_path)
                )

                if category == DuplicateCategory.CONFIRMED_NEAR_DUPLICATE:
                    return DuplicateCheckResult(
                        is_duplicate=True,
                        duplicate_of_id=matched_asset.id,
                        category=category,
                        reason=details.get("reason", "structural_verification_confirmed"),
                        details=details,
                    )
                elif category == DuplicateCategory.POSSIBLE_DUPLICATE:
                    # Ambiguous (e.g. same object photographed from different angle): retain!
                    return DuplicateCheckResult(
                        is_duplicate=False,
                        duplicate_of_id=matched_asset.id,
                        category=category,
                        reason=details.get("reason", "ambiguous_retained"),
                        details=details,
                    )
                else:
                    return DuplicateCheckResult(
                        is_duplicate=False,
                        category=DuplicateCategory.UNIQUE,
                        reason=details.get("reason", "structurally_distinct"),
                        details=details,
                    )
            except Exception as e:
                logger.warning(f"Structural visual verification failed: {e}, conservatively retaining asset")
                return DuplicateCheckResult(
                    is_duplicate=False,
                    category=DuplicateCategory.POSSIBLE_DUPLICATE,
                    duplicate_of_id=matched_asset.id,
                    reason=f"verification_error_retained: {e}",
                )

        # When local image binaries are not readily accessible for deep pixel compare,
        # fallback conservatively to coarse match if pHash is extremely tight (Hamming <= 2)
        if candidate_phash and matched_asset.phash:
            import imagehash

            h_cand = imagehash.hex_to_hash(candidate_phash)
            h_ref = imagehash.hex_to_hash(matched_asset.phash)
            dist = h_cand - h_ref
            if dist <= 2:
                return DuplicateCheckResult(
                    is_duplicate=True,
                    duplicate_of_id=matched_asset.id,
                    category=DuplicateCategory.CONFIRMED_NEAR_DUPLICATE,
                    reason=f"tight_phash_hamming_{dist}",
                )

        # High CLIP without visual confirmation is only POSSIBLE_DUPLICATE: retain!
        return DuplicateCheckResult(
            is_duplicate=False,
            duplicate_of_id=matched_asset.id,
            category=DuplicateCategory.POSSIBLE_DUPLICATE,
            reason="clip_similarity_high_but_unconfirmed_visually_retained",
        )
