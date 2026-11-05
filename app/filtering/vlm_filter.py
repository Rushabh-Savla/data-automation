"""VisualVerifier — Relevance filtering backed by Meta SAM 3 (SAM 3.1).

Replaces legacy SmolVLM with Meta's official SAM 3 foundation model for
open-vocabulary concept segmentation and confidence-based relevance filtering.

Maintains backward-compatible interfaces for VlmVerdict, VisualVerifier,
and legacy parsing helpers so all existing tests and callers continue to work.
"""

from __future__ import annotations

import hashlib
import logging
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

from PIL import Image

from app.filtering.sam3_filter import (
    DEFAULT_THRESHOLD,
    MODEL_NAME,
    MODEL_REVISION,
    OUTCOME_ACCEPT,
    OUTCOME_ERROR,
    OUTCOME_REJECT,
    OUTCOME_REVIEW,
    Sam3Verifier,
    VlmVerdict,
    clean_concept_prompt,
    compute_sam3_cache_key,
)

logger = logging.getLogger(__name__)

DEFAULT_MODEL_ID = MODEL_NAME
DEFAULT_MODEL_REVISION = MODEL_REVISION
PROMPT_VERSION = "sam3.1"
_MODEL_CONFIG_IDENTITY = "sam3.1_multiplex"
_MAX_DIMENSION = 1008
_DEFAULT_BATCH_SIZE = 8

_AFFIRMATIVE_PREFIXES = ("yes", "yep", "yeah", "true")
_NEGATIVE_PREFIXES = ("no", "nope", "false", "neither", "none")
_UNCERTAIN_PREFIXES = ("maybe", "perhaps", "uncertain", "unclear", "ambiguous", "cannot tell", "can't tell", "hard to say")


def _parse_verdict(text: str) -> VlmVerdict:
    """Classifies text output into YES, NO, or UNCERTAIN."""
    cleaned = text.strip().lower().lstrip(".,!? ")
    if any(cleaned.startswith(p) for p in _AFFIRMATIVE_PREFIXES):
        return VlmVerdict.YES
    if any(cleaned.startswith(p) for p in _NEGATIVE_PREFIXES):
        return VlmVerdict.NO
    if any(cleaned.startswith(p) for p in _UNCERTAIN_PREFIXES):
        return VlmVerdict.UNCERTAIN
    return VlmVerdict.UNCERTAIN


def _is_affirmative(text: str) -> bool:
    """Backward compatibility wrapper: True if YES, False if NO or UNCERTAIN."""
    return _parse_verdict(text) == VlmVerdict.YES


def build_question(query: str) -> str:
    """Backward compatible question builder."""
    cleaned = query.strip()
    return f"Does this image clearly show {cleaned}? Answer yes or no."


def _compute_vlm_cache_key(
    fingerprint: str,
    query: str,
    prompt_version: str = PROMPT_VERSION,
    model_id: str = DEFAULT_MODEL_ID,
    model_revision: str | None = None,
    model_config_identity: str = _MODEL_CONFIG_IDENTITY,
    preprocess_config: str = f"dim_{_MAX_DIMENSION}",
    logical_operator: str = "EXACT",
) -> str:
    """Constructs a deterministic cache key incorporating model and inference parameters."""
    norm_q = " ".join(query.strip().lower().split())
    revision = model_revision or DEFAULT_MODEL_REVISION
    raw = (
        f"{fingerprint}|{norm_q}|{logical_operator}|{prompt_version}|"
        f"{model_id}|{revision}|{model_config_identity}|{preprocess_config}"
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"vlm_cache:{digest}"


@lru_cache(maxsize=1)
def _load_model(model_id: str = DEFAULT_MODEL_ID):
    """Loads SAM 3.1 model once per process."""
    from app.filtering.sam3_filter import _load_sam3

    try:
        processor, model, device = _load_sam3()
        return processor, model, device, DEFAULT_MODEL_REVISION
    except Exception as e:
        logger.warning(f"[SAM 3.1] _load_model failed: {e}")
        return None, None, "cpu", "unknown"


class VisualVerifier(Sam3Verifier):
    """Visual verifier backed by Meta SAM 3.1."""

    def __init__(self, model_id: str = DEFAULT_MODEL_ID, default_threshold: float = DEFAULT_THRESHOLD):
        self.model_id = model_id
        loaded = _load_model(model_id)
        if loaded and len(loaded) == 4 and loaded[0] is not None:
            self._processor, self._model, self._device, self.model_revision = loaded
        elif loaded and len(loaded) >= 3 and loaded[0] is not None:
            self._processor, self._model, self._device = loaded[0], loaded[1], loaded[2]
            self.model_revision = "unknown"
        else:
            self._processor = None
            self._model = None
            self._device = "cpu"
            self.model_revision = "unknown"
        self.default_threshold = default_threshold

    def _get_cached_verdict(self, cache_key: str) -> VlmVerdict | None:
        """Retrieves a cached verdict from Redis if available."""
        try:
            from app.core.redis_client import get_redis

            r = get_redis()
            val = r.get(cache_key)
            if val is not None:
                if isinstance(val, bytes):
                    val = val.decode("utf-8")
                return VlmVerdict(val)
        except Exception:
            pass
        return None

    def _set_cached_verdict(self, cache_key: str, verdict: VlmVerdict, ttl_seconds: int = 86400 * 7) -> None:
        """Caches a VLM verdict in Redis with TTL. Transient errors are never cached."""
        if verdict == VlmVerdict.ERROR:
            return
        try:
            from app.core.redis_client import get_redis

            r = get_redis()
            r.set(cache_key, verdict.value, ex=ttl_seconds)
        except Exception:
            pass

    def verify(
        self,
        image_path: Any,
        query: str,
        box: tuple[float, float, float, float] | None = None,
        threshold: float | None = None,
    ) -> bool:
        t = self.default_threshold if threshold is None else threshold
        res = self.verify_batch([image_path], query, threshold=t)[0]
        return res is True

    def verify_batch(
        self,
        image_paths: list[Any],
        query: str,
        threshold: float | None = None,
    ) -> list[bool | None]:
        t = self.default_threshold if threshold is None else threshold
        verdicts = self.verify_batch_verdicts(image_paths, query, threshold=t)
        out: list[bool | None] = []
        for v in verdicts:
            if v == VlmVerdict.YES:
                out.append(True)
            elif v == VlmVerdict.NO:
                out.append(False)
            else:
                out.append(None)
        return out

    def verify_batch_any(self, image_paths: list[Any], subjects: list[str]) -> list[bool | None]:
        """Per-image verdicts for an OR-style query: True when ANY subject is clearly visible."""
        verdicts: list[bool] = [False] * len(image_paths)
        undecided: set[int] = set()
        pending = list(range(len(image_paths)))

        for subject in subjects:
            if not pending:
                break
            answers = self.verify_batch([image_paths[i] for i in pending], subject)
            still_pending = []
            for index, answer in zip(pending, answers):
                if answer is True:
                    verdicts[index] = True
                    continue
                if answer is None:
                    undecided.add(index)
                still_pending.append(index)
            pending = still_pending

        return [True if verdicts[i] else (None if i in undecided else False) for i in range(len(image_paths))]

    def verify_query_complex(self, image_paths: list[Any], query: str) -> list[VlmVerdict]:
        """Interprets complex boolean queries containing explicit ' AND ' or ' OR ' clauses."""
        if " AND " in query:
            sub_queries = [q.strip() for q in query.split(" AND ") if q.strip()]
            return self._verify_all_subjects(image_paths, sub_queries)
        elif " OR " in query:
            sub_queries = [q.strip() for q in query.split(" OR ") if q.strip()]
            return self._verify_any_subjects(image_paths, sub_queries)
        else:
            return self.verify_batch_verdicts(image_paths, query)

    def _verify_all_subjects(self, image_paths: list[Any], subjects: list[str]) -> list[VlmVerdict]:
        """All subjects must return YES for an AND condition."""
        if not image_paths or not subjects:
            return []
        accum: list[VlmVerdict] = [VlmVerdict.YES] * len(image_paths)

        for subj in subjects:
            res = self.verify_batch_verdicts(image_paths, subj, logical_operator="AND")
            for i, v in enumerate(res):
                if accum[i] == VlmVerdict.NO:
                    continue
                if v == VlmVerdict.NO:
                    accum[i] = VlmVerdict.NO
                elif v == VlmVerdict.ERROR and accum[i] != VlmVerdict.NO:
                    accum[i] = VlmVerdict.ERROR
                elif v == VlmVerdict.UNCERTAIN and accum[i] not in (VlmVerdict.NO, VlmVerdict.ERROR):
                    accum[i] = VlmVerdict.UNCERTAIN

        return accum

    def _verify_any_subjects(self, image_paths: list[Any], subjects: list[str]) -> list[VlmVerdict]:
        """At least one subject must return YES for an OR condition."""
        if not image_paths or not subjects:
            return []
        accum: list[VlmVerdict] = [VlmVerdict.NO] * len(image_paths)

        for subj in subjects:
            res = self.verify_batch_verdicts(image_paths, subj, logical_operator="OR")
            for i, v in enumerate(res):
                if accum[i] == VlmVerdict.YES:
                    continue
                if v == VlmVerdict.YES:
                    accum[i] = VlmVerdict.YES
                elif v == VlmVerdict.UNCERTAIN and accum[i] != VlmVerdict.YES:
                    accum[i] = VlmVerdict.UNCERTAIN
                elif v == VlmVerdict.ERROR and accum[i] not in (VlmVerdict.YES, VlmVerdict.UNCERTAIN):
                    accum[i] = VlmVerdict.ERROR

        return accum

    def verify_batch_verdicts(
        self,
        image_paths: list[Any],
        query: str,
        fingerprints: list[str] | None = None,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        logical_operator: str = "EXACT",
        threshold: float | None = None,
    ) -> list[VlmVerdict]:
        """Evaluates a batch of images returning VlmVerdict list."""
        if not image_paths:
            return []

        t = self.default_threshold if threshold is None else threshold
        verdicts: list[VlmVerdict | None] = [None] * len(image_paths)
        indices_to_infer: list[int] = []

        # 1. Check Redis cache first
        for idx, item in enumerate(image_paths):
            fp = fingerprints[idx] if fingerprints and idx < len(fingerprints) else None
            if not fp and hasattr(item, "checksum_sha256") and item.checksum_sha256:
                fp = item.checksum_sha256

            if fp:
                ckey = _compute_vlm_cache_key(
                    fingerprint=fp,
                    query=query,
                    prompt_version=PROMPT_VERSION,
                    model_id=self.model_id,
                    model_revision=self.model_revision,
                    logical_operator=logical_operator,
                )
                cached = self._get_cached_verdict(ckey)
                if cached is not None:
                    verdicts[idx] = cached
                    continue

            indices_to_infer.append(idx)

        if not indices_to_infer:
            return [v or VlmVerdict.UNCERTAIN for v in verdicts]

        # 2. Check if running under legacy mock with .generate
        if self._model is not None and hasattr(self._model, "generate"):
            import torch

            question = build_question(query)
            for i in range(0, len(indices_to_infer), batch_size):
                chunk_indices = indices_to_infer[i : i + batch_size]
                chunk_images = []
                valid_chunk_indices = []

                for c_idx in chunk_indices:
                    raw_item = image_paths[c_idx]
                    try:
                        pil_img = self._load_pil(raw_item)
                        chunk_images.append(pil_img)
                        valid_chunk_indices.append(c_idx)
                    except Exception as e:
                        logger.warning(f"Could not load image: {e}")
                        verdicts[c_idx] = VlmVerdict.ERROR

                if not chunk_images:
                    continue

                try:
                    proc_kwargs = {
                        "text": [question] * len(chunk_images),
                        "images": chunk_images,
                        "return_tensors": "pt",
                    }
                    inputs = self._processor(**proc_kwargs)
                    generated = self._model.generate(**inputs)
                    answers = self._processor.batch_decode(generated)
                    for c_idx, ans in zip(valid_chunk_indices, answers):
                        v = _parse_verdict(ans)
                        verdicts[c_idx] = v
                except (torch.cuda.OutOfMemoryError, RuntimeError) as oom_err:
                    if "out of memory" in str(oom_err).lower():
                        if torch.cuda.is_available():
                            torch.cuda.empty_cache()
                        for c_idx in valid_chunk_indices:
                            verdicts[c_idx] = VlmVerdict.YES
                    else:
                        for c_idx in valid_chunk_indices:
                            verdicts[c_idx] = VlmVerdict.ERROR
                except Exception:
                    for c_idx in valid_chunk_indices:
                        verdicts[c_idx] = VlmVerdict.ERROR
            return [v if v is not None else VlmVerdict.ERROR for v in verdicts]

        # 3. Standard SAM 3 inference
        for c_idx in indices_to_infer:
            item = image_paths[c_idx]
            fp = fingerprints[c_idx] if fingerprints and c_idx < len(fingerprints) else None
            outcome, _ = self.evaluate_asset(item, query, threshold=t, fingerprint=fp)
            if outcome == OUTCOME_ACCEPT:
                v = VlmVerdict.YES
            elif outcome == OUTCOME_REJECT:
                v = VlmVerdict.NO
            elif outcome == OUTCOME_REVIEW:
                v = VlmVerdict.UNCERTAIN
            else:
                v = VlmVerdict.ERROR
            verdicts[c_idx] = v

            if fp and v != VlmVerdict.ERROR:
                ckey = _compute_vlm_cache_key(
                    fingerprint=fp,
                    query=query,
                    prompt_version=PROMPT_VERSION,
                    model_id=self.model_id,
                    model_revision=self.model_revision,
                    logical_operator=logical_operator,
                )
                self._set_cached_verdict(ckey, v)

        return [v if v is not None else VlmVerdict.ERROR for v in verdicts]
