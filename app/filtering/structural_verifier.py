"""structural_verifier — Level 2 Canonical Pixel Hashing and Level 5 Structural Visual Verification.

Implements:
  - Level 2: Canonical Pixel Duplicate Detection (EXIF orientation normalization + 64x64 RGB pixel hash).
  - Level 5: Structural Similarity Index (SSIM) and ORB feature matching with RANSAC geometric homography.

Used selectively for ambiguous near-duplicate candidates identified by perceptual hashing (Level 3)
or CLIP embedding similarity (Level 4) to eliminate false positives.
"""

from __future__ import annotations

import hashlib
import logging
from enum import Enum
from pathlib import Path
from typing import BinaryIO

import cv2
import numpy as np
from PIL import Image, ImageOps

logger = logging.getLogger(__name__)


class DuplicateCategory(str, Enum):
    EXACT_DUPLICATE = "EXACT_DUPLICATE"
    CONFIRMED_NEAR_DUPLICATE = "CONFIRMED_NEAR_DUPLICATE"
    POSSIBLE_DUPLICATE = "POSSIBLE_DUPLICATE"
    UNIQUE = "UNIQUE"


def compute_canonical_pixel_hash(image_source: Path | BinaryIO | bytes | Image.Image | np.ndarray) -> str:
    """Level 2: Computes a canonical pixel hash normalized across EXIF orientation and color space.

    Normalizes:
      1. EXIF orientation via ImageOps.exif_transpose.
      2. Color space: converted to standard 8-bit RGB.
      3. Downsamples to fixed 64x64 raw RGB buffer.
      4. SHA256 of raw uncompressed pixel bytes.

    Detects images with identical visual content that have different EXIF headers,
    metadata timestamps, or compression wrappers.
    """
    if isinstance(image_source, np.ndarray):
        if len(image_source.shape) == 2:
            img = Image.fromarray(image_source, "L").convert("RGB")
        elif image_source.shape[2] == 3:
            rgb = cv2.cvtColor(image_source, cv2.COLOR_BGR2RGB)
            img = Image.fromarray(rgb, "RGB")
        else:
            img = Image.fromarray(image_source).convert("RGB")
        close_img = True
    elif isinstance(image_source, Image.Image):
        img = image_source
        close_img = False
    elif isinstance(image_source, (bytes, bytearray)):
        import io
        img = Image.open(io.BytesIO(image_source))
        close_img = True
    elif hasattr(image_source, "seek") and hasattr(image_source, "read"):
        image_source.seek(0)
        img = Image.open(image_source)
        close_img = True
    else:
        img = Image.open(Path(image_source))
        close_img = True

    try:
        # 1. Normalize EXIF orientation
        img = ImageOps.exif_transpose(img) or img
        # 2. Standardize to RGB
        if img.mode != "RGB":
            img = img.convert("RGB")
        # 3. Downsample to canonical 64x64 grid
        thumb = img.resize((64, 64), Image.Resampling.LANCZOS)
        # 4. Hash uncompressed raw pixel bytes
        return hashlib.sha256(thumb.tobytes()).hexdigest()
    finally:
        if close_img:
            img.close()


def _to_cv2_image(image_source: Path | BinaryIO | bytes | Image.Image | np.ndarray) -> np.ndarray:
    """Loads an image into a standard BGR numpy array for OpenCV processing."""
    if isinstance(image_source, np.ndarray):
        return image_source

    if isinstance(image_source, Image.Image):
        rgb = np.array(image_source.convert("RGB"))
        return cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)

    if isinstance(image_source, (bytes, bytearray)):
        nparr = np.frombuffer(image_source, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError("Failed to decode image from bytes")
        return img

    if hasattr(image_source, "seek") and hasattr(image_source, "read"):
        image_source.seek(0)
        content = image_source.read()
        nparr = np.frombuffer(content, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError("Failed to decode image from stream")
        return img

    p = Path(image_source)
    if not p.exists():
        raise FileNotFoundError(f"Image path not found: {p}")
    img = cv2.imread(str(p), cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError(f"OpenCV could not read image at {p}")
    return img


def compute_ssim(
    img1: Path | BinaryIO | bytes | Image.Image | np.ndarray,
    img2: Path | BinaryIO | bytes | Image.Image | np.ndarray,
) -> float:
    """Computes Structural Similarity Index (SSIM) between two images.

    Handles resolution differences by resizing img2 to img1's dimensions.
    Returns float in range [-1.0, 1.0], where 1.0 indicates perfect structural identity.
    """
    im1 = _to_cv2_image(img1)
    im2 = _to_cv2_image(img2)

    # Resize im2 to match im1 geometry if necessary
    if im1.shape[:2] != im2.shape[:2]:
        im2 = cv2.resize(im2, (im1.shape[1], im1.shape[0]), interpolation=cv2.INTER_AREA)

    # Convert to grayscale float32
    g1 = cv2.cvtColor(im1, cv2.COLOR_BGR2GRAY).astype(np.float32)
    g2 = cv2.cvtColor(im2, cv2.COLOR_BGR2GRAY).astype(np.float32)

    C1 = (0.01 * 255) ** 2
    C2 = (0.03 * 255) ** 2

    # Gaussian kernel 11x11, sigma=1.5
    mu1 = cv2.GaussianBlur(g1, (11, 11), 1.5)
    mu2 = cv2.GaussianBlur(g2, (11, 11), 1.5)

    mu1_sq = mu1 * mu1
    mu2_sq = mu2 * mu2
    mu1_mu2 = mu1 * mu2

    sigma1_sq = cv2.GaussianBlur(g1 * g1, (11, 11), 1.5) - mu1_sq
    sigma2_sq = cv2.GaussianBlur(g2 * g2, (11, 11), 1.5) - mu2_sq
    sigma12 = cv2.GaussianBlur(g1 * g2, (11, 11), 1.5) - mu1_mu2

    num = (2 * mu1_mu2 + C1) * (2 * sigma12 + C2)
    den = (mu1_sq + mu2_sq + C1) * (sigma1_sq + sigma2_sq + C2)

    ssim_map = num / (den + 1e-7)
    return float(np.mean(ssim_map))


def _is_valid_homography(H: np.ndarray | None, width: int, height: int) -> bool:
    """Validates that a homography matrix represents a physically plausible 2D transformation.

    Ensures:
      1. H is non-null and contains no NaN / Inf values.
      2. Determinant of affine/linear part is strictly positive and within realistic scale bounds.
      3. Projected quadrilateral from image corners is strictly convex.
      4. Projected area is positive and within a plausible scale ratio (0.05 to 20.0).
    """
    if H is None or not isinstance(H, np.ndarray) or H.shape != (3, 3):
        return False
    if np.isnan(H).any() or np.isinf(H).any():
        return False

    det = float(np.linalg.det(H[:2, :2]))
    if det <= 0.05 or det >= 20.0:
        return False

    w, h = float(max(1, width)), float(max(1, height))
    corners = np.array([
        [0.0, 0.0],
        [w, 0.0],
        [w, h],
        [0.0, h],
    ], dtype=np.float32).reshape(-1, 1, 2)

    try:
        projected = cv2.perspectiveTransform(corners, H)
    except Exception:
        return False

    if projected is None or np.isnan(projected).any() or np.isinf(projected).any():
        return False

    # Check convexity of the projected 4-corner polygon
    if not cv2.isContourConvex(np.int32(projected)):
        return False

    orig_area = w * h
    proj_area = float(cv2.contourArea(projected))
    if orig_area <= 0 or proj_area <= 0:
        return False

    area_ratio = proj_area / orig_area
    if area_ratio < 0.05 or area_ratio > 20.0:
        return False

    return True


def _check_inlier_spatial_distribution(
    pts: np.ndarray,
    width: int,
    height: int,
    min_span_ratio: float = 0.20,
) -> bool:
    """Ensures inlier keypoints are distributed across the image, preventing concentrated false-positive clusters.

    Rejects matches where all inliers are clustered inside a tiny sub-region (e.g. a shared logo
    or corner watermark) while the main visual scene is completely different.
    """
    if len(pts) < 4:
        return False
    xs = pts[:, 0, 0]
    ys = pts[:, 0, 1]
    dx = float(np.max(xs) - np.min(xs))
    dy = float(np.max(ys) - np.min(ys))
    w_span = dx / float(max(1, width))
    h_span = dy / float(max(1, height))
    return (w_span >= min_span_ratio) and (h_span >= min_span_ratio)


def verify_orb_homography(
    img1: Path | BinaryIO | bytes | Image.Image | np.ndarray,
    img2: Path | BinaryIO | bytes | Image.Image | np.ndarray,
    min_inliers: int = 15,
    inlier_ratio_threshold: float = 0.60,
) -> tuple[bool, int, float]:
    """Level 5: ORB feature matching with RANSAC geometric homography verification.

    Detects whether two images share the exact same underlying scene/subject through
    crops, perspective shifts, rotations, or compression.

    Enforces:
      - Minimum inliers count (e.g. >= 15)
      - Inlier ratio threshold (e.g. >= 0.60)
      - Non-degenerate homography matrix (positive determinant, convex corner projection)
      - Spatial inlier distribution (inliers must span >= 20% of image dimensions)

    Returns:
      (is_match, inliers_count, inlier_ratio)
    """
    im1 = _to_cv2_image(img1)
    im2 = _to_cv2_image(img2)

    h1, w1 = im1.shape[:2]
    g1 = cv2.cvtColor(im1, cv2.COLOR_BGR2GRAY)
    g2 = cv2.cvtColor(im2, cv2.COLOR_BGR2GRAY)

    orb = cv2.ORB_create(nfeatures=500)
    kp1, des1 = orb.detectAndCompute(g1, None)
    kp2, des2 = orb.detectAndCompute(g2, None)

    if des1 is None or des2 is None or len(kp1) < 4 or len(kp2) < 4:
        return False, 0, 0.0

    bf = cv2.BFMatcher(cv2.NORM_HAMMING)
    matches = bf.knnMatch(des1, des2, k=2)

    # Lowe's ratio test (threshold 0.75)
    good = []
    for pair in matches:
        if len(pair) == 2:
            m, n = pair
            if m.distance < 0.75 * n.distance:
                good.append(m)

    if len(good) < 4:
        return False, len(good), 0.0

    src_pts = np.float32([kp1[m.queryIdx].pt for m in good]).reshape(-1, 1, 2)
    dst_pts = np.float32([kp2[m.trainIdx].pt for m in good]).reshape(-1, 1, 2)

    # Geometric alignment via RANSAC
    H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
    if mask is None or H is None:
        return False, len(good), 0.0

    inliers = int(np.sum(mask))
    ratio = inliers / max(1, len(good))

    # Validate homography geometry (determinant, corner convexity, area preservation)
    geom_valid = _is_valid_homography(H, w1, h1)

    # Validate inlier spatial distribution across the image plane
    inlier_src_pts = src_pts[mask.ravel() == 1]
    spatial_valid = _check_inlier_spatial_distribution(inlier_src_pts, w1, h1, min_span_ratio=0.20)

    is_match = (
        geom_valid
        and spatial_valid
        and inliers >= min_inliers
        and ratio >= inlier_ratio_threshold
    )

    return is_match, inliers, ratio


class StructuralVerifier:
    """Performs deep structural verification on ambiguous candidate pairs."""

    def __init__(
        self,
        ssim_threshold: float = 0.85,
        orb_inlier_threshold: float = 0.60,
        orb_min_inliers: int = 15,
    ):
        self.ssim_threshold = ssim_threshold
        self.orb_inlier_threshold = orb_inlier_threshold
        self.orb_min_inliers = orb_min_inliers

    def verify_pair(
        self,
        candidate_img: Path | BinaryIO | bytes | Image.Image,
        reference_img: Path | BinaryIO | bytes | Image.Image,
    ) -> tuple[DuplicateCategory, dict]:
        """Evaluates an ambiguous candidate against a reference image using conservative multi-signal verification.

        Conservative Decision Hierarchy:
          1. Level 2 Canonical Pixel Hash: Checked as candidate fingerprint, but verified against aspect ratio
             and structural similarity (SSIM >= 0.88). Never confirms duplicates on downsampled hash alone.
          2. Level 5 Structural Similarity (SSIM):
             - SSIM >= 0.95 (near-identical) with consistent aspect ratio -> CONFIRMED_NEAR_DUPLICATE.
             - SSIM >= 0.85 requires corroborating geometric features (inliers >= 10 or valid homography).
             - SSIM >= 0.85 without geometric corroboration (e.g. similar background but different object)
               remains POSSIBLE_DUPLICATE (retained).
          3. Level 5 Geometric Feature Matching (ORB with RANSAC):
             - Valid convex homography + inliers >= min_inliers + ratio >= threshold + spatial distribution >= 20%
               + SSIM >= 0.45 -> CONFIRMED_NEAR_DUPLICATE.
          4. Ambiguous matches (SSIM >= 0.70 or inliers >= 10) -> POSSIBLE_DUPLICATE (retained).
          5. Below thresholds -> UNIQUE.

        Returns:
          (DuplicateCategory, details_dict)
        """
        details: dict = {}
        try:
            im1 = _to_cv2_image(candidate_img)
            im2 = _to_cv2_image(reference_img)

            h1, w1 = im1.shape[:2]
            h2, w2 = im2.shape[:2]
            ar1 = float(w1) / float(max(1, h1))
            ar2 = float(w2) / float(max(1, h2))
            ar_ratio = max(ar1, ar2) / max(1e-5, min(ar1, ar2))
            aspect_ratio_consistent = (ar_ratio <= 1.20)
            details["aspect_ratio_consistent"] = aspect_ratio_consistent
            details["candidate_dims"] = (w1, h1)
            details["reference_dims"] = (w2, h2)

            # 1. Level 2 Canonical Pixel Hash Check (Candidate fingerprint only)
            hash_cand = compute_canonical_pixel_hash(im1)
            hash_ref = compute_canonical_pixel_hash(im2)
            pixel_hash_match = (hash_cand == hash_ref)
            details["pixel_hash_match"] = pixel_hash_match

            # 2. Level 5 SSIM
            ssim_score = compute_ssim(im1, im2)
            details["ssim_score"] = ssim_score

            # 3. Level 5 Geometric Feature Matching (ORB with RANSAC)
            orb_match, inliers, ratio = verify_orb_homography(
                im1,
                im2,
                min_inliers=self.orb_min_inliers,
                inlier_ratio_threshold=self.orb_inlier_threshold,
            )
            details["orb_inliers"] = inliers
            details["orb_ratio"] = ratio
            details["orb_match"] = orb_match

            # If pixel hash matches, confirm only if aspect ratio matches AND visual similarity is corroborated
            if pixel_hash_match:
                if aspect_ratio_consistent and (ssim_score >= 0.96 or (ssim_score >= 0.88 and inliers >= 8)):
                    return DuplicateCategory.CONFIRMED_NEAR_DUPLICATE, {
                        **details,
                        "reason": f"canonical_pixel_hash_and_ssim_confirmed (ssim={ssim_score:.3f}, inliers={inliers})",
                    }
                else:
                    return DuplicateCategory.POSSIBLE_DUPLICATE, {
                        **details,
                        "reason": f"canonical_pixel_hash_match_unverified (ar_consistent={aspect_ratio_consistent}, ssim={ssim_score:.3f}, inliers={inliers})",
                    }

            # Rule A: Near-identical SSIM (>= 0.95) with consistent aspect ratio
            if ssim_score >= 0.95 and aspect_ratio_consistent:
                return DuplicateCategory.CONFIRMED_NEAR_DUPLICATE, {
                    **details,
                    "reason": f"ssim_near_identical ({ssim_score:.3f} >= 0.95)",
                }

            # Rule B: Strong geometric alignment via RANSAC homography
            if orb_match and ssim_score >= 0.35:
                return DuplicateCategory.CONFIRMED_NEAR_DUPLICATE, {
                    **details,
                    "reason": f"orb_homography_confirmed ({inliers} inliers, ratio {ratio:.2f}, ssim {ssim_score:.3f})",
                }

            # Rule C: Moderate-to-high SSIM (>= threshold) MUST be supported by geometric features
            if ssim_score >= self.ssim_threshold and aspect_ratio_consistent:
                if inliers >= 10:
                    return DuplicateCategory.CONFIRMED_NEAR_DUPLICATE, {
                        **details,
                        "reason": f"ssim_high_with_geometric_support ({ssim_score:.3f} >= {self.ssim_threshold}, inliers={inliers})",
                    }
                else:
                    # Without geometric support, high SSIM on different objects/backgrounds is ambiguous!
                    return DuplicateCategory.POSSIBLE_DUPLICATE, {
                        **details,
                        "reason": f"ssim_high_without_geometric_support_retained ({ssim_score:.3f}, inliers={inliers})",
                    }

            # Rule D: Ambiguous moderate similarity (retain as POSSIBLE_DUPLICATE)
            if ssim_score >= 0.70 or inliers >= 10:
                return DuplicateCategory.POSSIBLE_DUPLICATE, {
                    **details,
                    "reason": f"moderate_similarity_retained (ssim={ssim_score:.3f}, inliers={inliers})",
                }

            return DuplicateCategory.UNIQUE, {**details, "reason": "visually_distinct"}
        except Exception as e:
            logger.warning(f"Error during structural verification: {e}")
            # Fail conservatively: never reject on verification error!
            return DuplicateCategory.POSSIBLE_DUPLICATE, {
                **details,
                "error": str(e),
                "reason": "verification_error_retained",
            }

