from app.filtering.canonical_selector import CanonicalSelector
from app.filtering.cross_batch_dedup import CrossBatchDeduplicator, DuplicateCheckResult
from app.filtering.dedup import DuplicateGroup, HybridDeduplicator
from app.filtering.structural_verifier import (
    DuplicateCategory,
    StructuralVerifier,
    compute_canonical_pixel_hash,
    compute_ssim,
    verify_orb_homography,
)

__all__ = [
    "HybridDeduplicator",
    "DuplicateGroup",
    "CrossBatchDeduplicator",
    "DuplicateCheckResult",
    "CanonicalSelector",
    "DuplicateCategory",
    "StructuralVerifier",
    "compute_canonical_pixel_hash",
    "compute_ssim",
    "verify_orb_homography",
]

