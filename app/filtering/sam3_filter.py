"""Meta SAM 3 / SAM 3.1 image filtering and relevance verification.

This module replaces SmolVLM with Meta's official SAM 3 (SAM 3.1 Object Multiplex)
foundation model for open-vocabulary concept segmentation and confidence-based
relevance filtering.

Architecture:
  - Official Meta SAM 3 model builder and processor (build_sam3_image_model, Sam3Processor)
  - Pretrained SAM 3.1 multiplex checkpoint
  - Process-level singleton model caching (loaded once per worker process)
  - Concise concept-prompt extraction and multi-class query interpretation (AND / OR)
  - Genuine instance-level confidence score extraction and evaluation:
      ACCEPT if at least one matching instance has score >= threshold
      REJECT if no matching instance reaches threshold
  - Full Redis score caching by SHA256 fingerprint and concept
  - GPU memory management and CUDA OOM recovery
"""

from __future__ import annotations

import glob
import hashlib
import logging
import math
import os
import re
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

import numpy as np
from PIL import Image

from app.filtering import query_expansion

logger = logging.getLogger(__name__)

MODEL_NAME = "Meta-SAM-3.1"
MODEL_REVISION = "sam3.1_multiplex"
DEFAULT_THRESHOLD = 0.40

# Outcomes for asset status mapping
OUTCOME_ACCEPT = "accept"
OUTCOME_REJECT = "reject"
OUTCOME_REVIEW = "review"
OUTCOME_ERROR = "error"


class VlmVerdict(str, Enum):
    YES = "YES"
    NO = "NO"
    UNCERTAIN = "UNCERTAIN"
    ERROR = "ERROR"


def _select_device() -> str:
    """Selects CUDA if available, otherwise CPU."""
    try:
        import torch

        return "cuda" if torch.cuda.is_available() else "cpu"
    except Exception:
        return "cpu"


def find_sam3_checkpoint() -> str | None:
    """Locates the SAM 3.1 checkpoint from environment or local HuggingFace cache."""
    # 1. Explicit environment variable
    env_path = os.environ.get("SAM3_CHECKPOINT_PATH")
    if env_path and os.path.exists(env_path):
        return env_path

    # 2. Check local HuggingFace cache locations
    search_patterns = [
        "/root/.cache/huggingface/hub/models--*sam3*/snapshots/*/*.pt",
        os.path.expanduser("~/.cache/huggingface/hub/models--*sam3*/snapshots/*/*.pt"),
        "/srv/models/*.pt",
        "/nas/models/*.pt",
    ]
    for pattern in search_patterns:
        matches = glob.glob(pattern)
        # Prioritize sam3.1_multiplex.pt
        for m in matches:
            if "sam3.1_multiplex.pt" in m and os.path.exists(m):
                return m
        # Fall back to any sam3 .pt checkpoint
        for m in matches:
            if os.path.exists(m):
                return m

    # 3. Auto-download from HuggingFace via sam3 model_builder
    try:
        from sam3.model_builder import download_ckpt_from_hf

        return download_ckpt_from_hf(version="sam3.1")
    except Exception as e:
        logger.warning(f"[SAM 3.1] Could not resolve checkpoint via HuggingFace download: {e}")
        return None


@lru_cache(maxsize=1)
def _load_sam3():
    """Loads Meta SAM 3.1 model once per process."""
    import torch
    from sam3.model_builder import build_sam3_image_model
    from sam3.model.sam3_image_processor import Sam3Processor

    device = _select_device()
    ckpt_path = find_sam3_checkpoint()
    logger.info(f"[SAM 3.1] Initializing model on device '{device}', checkpoint: {ckpt_path}")

    try:
        model = build_sam3_image_model(
            checkpoint_path=ckpt_path,
            load_from_HF=(ckpt_path is None),
            device=device,
            eval_mode=True,
        )
    except (torch.cuda.OutOfMemoryError, RuntimeError) as e:
        if "out of memory" in str(e).lower() and device == "cuda":
            logger.warning(f"[SAM 3.1] CUDA OOM during model load: {e}. Clearing cache and retrying...")
            torch.cuda.empty_cache()
            import time
            time.sleep(1.0)
            model = build_sam3_image_model(
                checkpoint_path=ckpt_path,
                load_from_HF=(ckpt_path is None),
                device=device,
                eval_mode=True,
            )
        else:
            raise

    # Set confidence_threshold to 0.0 so the processor does not pre-filter
    # candidate detections; our evaluator compares each score >= configured threshold.
    processor = Sam3Processor(model, device=device, confidence_threshold=0.0)
    logger.info(f"[SAM 3.1] Successfully loaded model on device '{device}'")
    return processor, model, device


def clean_concept_prompt(text: str) -> str:
    """Extracts a concise, clean concept noun phrase for SAM 3 text prompt.

    SAM 3 performs best with concise noun phrases rather than long conversational prompts.
    """
    cleaned = query_expansion.normalize_query(text).strip()
    # Remove leading articles: 'a ', 'an ', 'the '
    cleaned = re.sub(r"^(a|an|the)\s+", "", cleaned, flags=re.IGNORECASE)
    # Remove context words like dataset, image, photo that are not detectable physical objects
    cleaned = re.sub(r"\b(dataset|datasets|photos?|images?|pictures?)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(".,!?\"' ")
    return cleaned if cleaned else text.strip()


def compute_sam3_cache_key(
    fingerprint: str,
    concept: str,
    model_name: str = MODEL_NAME,
    revision: str = MODEL_REVISION,
) -> str:
    """Builds a deterministic Redis cache key incorporating image hash and concept."""
    norm_concept = " ".join(concept.strip().lower().split())
    raw = f"{fingerprint}|{norm_concept}|{model_name}|{revision}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"sam3_score:{digest}"


class Sam3Verifier:
    """Meta SAM 3.1 image filtering and relevance verification."""

    def __init__(self, checkpoint_path: str | None = None, default_threshold: float = DEFAULT_THRESHOLD):
        self.default_threshold = default_threshold
        if checkpoint_path:
            os.environ["SAM3_CHECKPOINT_PATH"] = checkpoint_path
        self._processor, self._model, self._device = _load_sam3()
        self.model_id = MODEL_NAME
        self.model_revision = MODEL_REVISION

    def _get_cached_score(self, cache_key: str) -> float | None:
        """Retrieves a cached numeric confidence score from Redis if available."""
        try:
            from app.core.redis_client import get_redis

            r = get_redis()
            val = r.get(cache_key)
            if val is not None:
                if isinstance(val, bytes):
                    val = val.decode("utf-8")
                return float(val)
        except Exception:
            pass
        return None

    def _set_cached_score(self, cache_key: str, score: float, ttl_seconds: int = 86400 * 7) -> None:
        """Caches numeric confidence score in Redis with TTL."""
        try:
            from app.core.redis_client import get_redis

            r = get_redis()
            r.set(cache_key, str(score), ex=ttl_seconds)
        except Exception:
            pass

    def _load_pil(self, raw_item: Any, max_dimension: int = 1280) -> Image.Image:
        """Loads, normalizes, and bounds an image item into a PIL RGB Image."""
        if hasattr(raw_item, "to_pil_image"):
            img = raw_item.to_pil_image().convert("RGB")
        elif hasattr(raw_item, "read"):
            raw_item.seek(0)
            img = Image.open(raw_item).convert("RGB")
        elif isinstance(raw_item, Image.Image):
            img = raw_item.convert("RGB")
        else:
            img = Image.open(raw_item).convert("RGB")

        # Constrain max dimension to 1280 to prevent peak activation spikes during vision encoding
        w, h = img.size
        if max(w, h) > max_dimension:
            scale = float(max_dimension) / float(max(w, h))
            new_w = max(1, int(round(w * scale)))
            new_h = max(1, int(round(h * scale)))
            img = img.resize((new_w, new_h), Image.Resampling.BILINEAR)

        return img

    def infer_concept(self, image_or_state: Any, concept: str) -> dict[str, Any]:
        """Runs SAM 3 inference for a single concept prompt.

        Returns dict containing:
          - scores: 1D numpy array of float confidence scores
          - boxes: bounding boxes in [x0, y0, x1, y1] format
          - masks: boolean segmentation masks
          - max_score: float maximum score or 0.0
        """
        import torch

        with torch.inference_mode():
            # Prepare inference state
            if isinstance(image_or_state, dict) and "backbone_out" in image_or_state:
                state = image_or_state
            else:
                pil_img = self._load_pil(image_or_state)
                state = self._processor.set_image(pil_img)

            clean_prompt = clean_concept_prompt(concept)
            output = self._processor.set_text_prompt(prompt=clean_prompt, state=state)

            scores_t = output.get("scores")
            boxes_t = output.get("boxes")
            masks_t = output.get("masks")

            if scores_t is not None and len(scores_t) > 0:
                scores_np = scores_t.detach().float().cpu().numpy()
                # Filter non-finite scores (NaN, inf)
                valid_mask = np.isfinite(scores_np)
                scores_np = scores_np[valid_mask]
                max_score = float(np.max(scores_np)) if len(scores_np) > 0 else 0.0
            else:
                scores_np = np.array([], dtype=np.float32)
                max_score = 0.0

            return {
                "scores": scores_np,
                "boxes": boxes_t,
                "masks": masks_t,
                "max_score": max_score,
                "state": state,
            }

    def evaluate_asset(
        self,
        image_item: Any,
        query: str,
        threshold: float = DEFAULT_THRESHOLD,
        fingerprint: str | None = None,
    ) -> tuple[str, float | None]:
        """Evaluates a single asset against the configured confidence threshold.

        Rule:
          - ACCEPT if at least one matching instance has score >= threshold
          - REJECT if no matching instance reaches threshold
          - Returns (outcome, score) where outcome is 'accept' | 'reject' | 'review' | 'error'
        """
        import torch

        # Normalize threshold
        try:
            threshold = float(threshold)
            if math.isnan(threshold) or threshold < 0.0 or threshold > 1.0:
                threshold = self.default_threshold
        except (TypeError, ValueError):
            threshold = self.default_threshold

        # 1. Check Redis cache first if fingerprint is available
        if fingerprint:
            cache_key = compute_sam3_cache_key(fingerprint, query)
            cached_score = self._get_cached_score(cache_key)
            if cached_score is not None:
                outcome = OUTCOME_ACCEPT if cached_score >= threshold else OUTCOME_REJECT
                return outcome, cached_score

        try:
            pil_img = self._load_pil(image_item)
        except Exception as e:
            logger.warning(f"[SAM 3.1] Could not open image: {e}")
            return OUTCOME_ERROR, None

        try:
            with torch.inference_mode():
                # Reusable image state across multi-class concepts
                state = self._processor.set_image(pil_img)

                # Complex query evaluation: AND / OR
                if " AND " in query:
                    sub_queries = [q.strip() for q in query.split(" AND ") if q.strip()]
                    concept_scores: list[float] = []
                    concept_passes: list[bool] = []
                    for sub in sub_queries:
                        res = self.infer_concept(state, sub)
                        scores = res["scores"]
                        top = res["max_score"]
                        concept_scores.append(top)
                        concept_passes.append(bool(np.any(scores >= threshold)))

                    all_passed = all(concept_passes)
                    winning_score = float(min(concept_scores)) if concept_scores else 0.0
                    outcome = OUTCOME_ACCEPT if all_passed else OUTCOME_REJECT

                elif " OR " in query:
                    sub_queries = [q.strip() for q in query.split(" OR ") if q.strip()]
                    concept_scores: list[float] = []
                    concept_passes: list[bool] = []
                    for sub in sub_queries:
                        res = self.infer_concept(state, sub)
                        scores = res["scores"]
                        top = res["max_score"]
                        concept_scores.append(top)
                        concept_passes.append(bool(np.any(scores >= threshold)))

                    any_passed = any(concept_passes)
                    winning_score = float(max(concept_scores)) if concept_scores else 0.0
                    outcome = OUTCOME_ACCEPT if any_passed else OUTCOME_REJECT

                else:
                    # Single concept prompt or expanded concepts
                    subjects = query_expansion.derive_classes(query)
                    if not subjects:
                        subjects = query_expansion.query_subjects(query)
                    if len(subjects) > 1:
                        # Multi-subject expansion (treated as OR)
                        concept_scores = []
                        concept_passes = []
                        for sub in subjects:
                            res = self.infer_concept(state, sub)
                            scores = res["scores"]
                            top = res["max_score"]
                            concept_scores.append(top)
                            concept_passes.append(bool(np.any(scores >= threshold)))
                        any_passed = any(concept_passes)
                        winning_score = float(max(concept_scores)) if concept_scores else 0.0
                        outcome = OUTCOME_ACCEPT if any_passed else OUTCOME_REJECT
                    else:
                        target_concept = subjects[0] if subjects else query
                        res = self.infer_concept(state, target_concept)
                        scores = res["scores"]
                        winning_score = res["max_score"]
                        has_match = bool(np.any(scores >= threshold))
                        outcome = OUTCOME_ACCEPT if has_match else OUTCOME_REJECT

                # Cache successful score in Redis
                if fingerprint:
                    cache_key = compute_sam3_cache_key(fingerprint, query)
                    self._set_cached_score(cache_key, winning_score)

                return outcome, winning_score

        except (torch.cuda.OutOfMemoryError, RuntimeError) as oom_err:
            if "out of memory" in str(oom_err).lower():
                logger.warning(f"[SAM 3.1] CUDA OOM encountered during inference; releasing memory: {oom_err}")
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                return OUTCOME_ERROR, None
            logger.error(f"[SAM 3.1] Runtime error during inference: {oom_err}")
            return OUTCOME_ERROR, None
        except Exception as e:
            logger.error(f"[SAM 3.1] Unexpected error during inference: {e}")
            return OUTCOME_ERROR, None
        finally:
            state = None

    # -- Backward Compatibility Interfaces -------------------------------------

    def verify(
        self,
        image_path: Any,
        query: str,
        box: tuple[float, float, float, float] | None = None,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> bool:
        """Returns True if image passes threshold for query, False otherwise."""
        outcome, _ = self.evaluate_asset(image_path, query, threshold=threshold)
        return outcome == OUTCOME_ACCEPT

    def verify_batch(
        self,
        image_paths: list[Any],
        query: str,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> list[bool | None]:
        """Backward-compatible batch interface returning True / False / None."""
        results: list[bool | None] = []
        for path in image_paths:
            outcome, _ = self.evaluate_asset(path, query, threshold=threshold)
            if outcome == OUTCOME_ACCEPT:
                results.append(True)
            elif outcome == OUTCOME_REJECT:
                results.append(False)
            else:
                results.append(None)
        return results

    def verify_batch_any(
        self,
        image_paths: list[Any],
        subjects: list[str],
        threshold: float = DEFAULT_THRESHOLD,
    ) -> list[bool | None]:
        """Evaluates each image against multiple subjects (OR condition)."""
        or_query = " OR ".join(subjects)
        return self.verify_batch(image_paths, or_query, threshold=threshold)

    def verify_query_complex(
        self,
        image_paths: list[Any],
        query: str,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> list[VlmVerdict]:
        """Evaluates complex queries returning VlmVerdict enum list."""
        verdicts: list[VlmVerdict] = []
        for path in image_paths:
            outcome, _ = self.evaluate_asset(path, query, threshold=threshold)
            if outcome == OUTCOME_ACCEPT:
                verdicts.append(VlmVerdict.YES)
            elif outcome == OUTCOME_REJECT:
                verdicts.append(VlmVerdict.NO)
            elif outcome == OUTCOME_REVIEW:
                verdicts.append(VlmVerdict.UNCERTAIN)
            else:
                verdicts.append(VlmVerdict.ERROR)
        return verdicts

    def verify_batch_verdicts(
        self,
        image_paths: list[Any],
        query: str,
        fingerprints: list[str] | None = None,
        batch_size: int = 8,
        logical_operator: str = "EXACT",
        threshold: float = DEFAULT_THRESHOLD,
    ) -> list[VlmVerdict]:
        """Evaluates batch of images returning VlmVerdict list."""
        verdicts: list[VlmVerdict] = []
        for idx, item in enumerate(image_paths):
            fp = fingerprints[idx] if fingerprints and idx < len(fingerprints) else None
            outcome, _ = self.evaluate_asset(item, query, threshold=threshold, fingerprint=fp)
            if outcome == OUTCOME_ACCEPT:
                verdicts.append(VlmVerdict.YES)
            elif outcome == OUTCOME_REJECT:
                verdicts.append(VlmVerdict.NO)
            elif outcome == OUTCOME_REVIEW:
                verdicts.append(VlmVerdict.UNCERTAIN)
            else:
                verdicts.append(VlmVerdict.ERROR)
        return verdicts

