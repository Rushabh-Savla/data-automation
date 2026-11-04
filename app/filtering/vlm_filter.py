"""VisualVerifier — TinyVLM-based relevance filtering and classification.

This module implements:
  - Exact TinyVLM model integration (HuggingFaceTB/SmolVLM-256M-Instruct)
  - Genuine batching and bounded sequential fallback
  - Explicit four-state outcomes: YES (accept), NO (reject), UNCERTAIN (review_pending), ERROR (retry_pending)
  - AND / OR complex query interpretation
  - GPU / CPU memory management with CUDA OOM handling
  - Result caching with SHA256 image fingerprint, query, prompt version, and model version
"""

from __future__ import annotations

import hashlib
import json
import logging
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

from PIL import Image

logger = logging.getLogger(__name__)

# TinyVLM specification as documented in project architecture
DEFAULT_MODEL_ID = "HuggingFaceTB/SmolVLM-256M-Instruct"
DEFAULT_MODEL_REVISION = "7e3e67edbbed1bf9888184d9df282b700a323964"
PROMPT_VERSION = "v1"

# TinyVLM runs natively on 384px without tiling, keeping inference bounded, fast, and deterministic.
_MAX_DIMENSION = 384
_MAX_NEW_TOKENS = 5
_DEFAULT_BATCH_SIZE = 4
_MODEL_CONFIG_IDENTITY = "fp16_tok5_greedy"

_AFFIRMATIVE_PREFIXES = ("yes", "yep", "yeah", "true")
_NEGATIVE_PREFIXES = ("no", "nope", "false", "neither", "none")
_UNCERTAIN_PREFIXES = ("maybe", "perhaps", "uncertain", "unclear", "ambiguous", "cannot tell", "can't tell", "hard to say")


class VlmVerdict(str, Enum):
    YES = "YES"
    NO = "NO"
    UNCERTAIN = "UNCERTAIN"
    ERROR = "ERROR"


def _parse_verdict(text: str) -> VlmVerdict:
    """Classifies model text output into YES, NO, or UNCERTAIN."""
    cleaned = text.strip().lower().lstrip(".,!? ")
    if any(cleaned.startswith(p) for p in _AFFIRMATIVE_PREFIXES):
        return VlmVerdict.YES
    if any(cleaned.startswith(p) for p in _NEGATIVE_PREFIXES):
        return VlmVerdict.NO
    if any(cleaned.startswith(p) for p in _UNCERTAIN_PREFIXES):
        return VlmVerdict.UNCERTAIN
    # If the model produces unparseable text or rambles, categorize as UNCERTAIN (review_pending)
    return VlmVerdict.UNCERTAIN


def _is_affirmative(text: str) -> bool:
    """Backward compatibility wrapper: True if YES, False if NO or UNCERTAIN."""
    return _parse_verdict(text) == VlmVerdict.YES


def _select_device() -> str:
    """CUDA when a GPU is available, else CPU."""
    try:
        import torch

        return "cuda" if torch.cuda.is_available() else "cpu"
    except Exception:
        return "cpu"


@lru_cache(maxsize=1)
def _load_model(model_id: str = DEFAULT_MODEL_ID):
    """Loads TinyVLM processor and weights with local-cache preference and FP16 on CUDA."""
    import torch
    from transformers import AutoModelForImageTextToText, AutoProcessor

    device = _select_device()
    logger.info(f"[TinyVLM] Initializing model '{model_id}' on device: {device}")

    try:
        processor = AutoProcessor.from_pretrained(model_id, local_files_only=True)
    except Exception:
        processor = AutoProcessor.from_pretrained(model_id)

    # Use FP16 on GPU (halves weight footprint to ~500MB VRAM); FP32 on CPU
    dtype = torch.float16 if device == "cuda" else torch.float32
    try:
        model = AutoModelForImageTextToText.from_pretrained(
            model_id, dtype=dtype, local_files_only=True
        )
    except Exception:
        model = AutoModelForImageTextToText.from_pretrained(
            model_id, dtype=dtype
        )

    model.to(device)
    model.eval()
    revision = getattr(model.config, "_commit_hash", None) or DEFAULT_MODEL_REVISION
    logger.info(f"[TinyVLM] Successfully loaded model '{model_id}' (rev: {revision[:8]}) on device: {device}")
    return processor, model, device, revision


def build_question(query: str) -> str:
    """Constructs the yes/no question put to TinyVLM."""
    return f"Is {query} clearly visible in this image? Answer only yes or no."


def _compute_vlm_cache_key(
    fingerprint: str,
    query: str,
    prompt_version: str = PROMPT_VERSION,
    model_id: str = DEFAULT_MODEL_ID,
    model_revision: str | None = None,
    model_config_identity: str = _MODEL_CONFIG_IDENTITY,
    preprocess_config: str = f"dim_{_MAX_DIMENSION}_notile",
    logical_operator: str = "EXACT",
) -> str:
    """Constructs a deterministic, complete cache key incorporating all model and inference parameters."""
    norm_q = " ".join(query.strip().lower().split())
    revision = model_revision or DEFAULT_MODEL_REVISION
    raw = (
        f"{fingerprint}|{norm_q}|{logical_operator}|{prompt_version}|"
        f"{model_id}|{revision}|{model_config_identity}|{preprocess_config}"
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"vlm_cache:{digest}"


class VisualVerifier:
    """TinyVLM visual verifier supporting batching, caching, and explicit outcomes."""

    def __init__(self, model_id: str = DEFAULT_MODEL_ID):
        self.model_id = model_id
        loaded = _load_model(model_id)
        if len(loaded) == 4:
            self._processor, self._model, self._device, self.model_revision = loaded
        else:
            self._processor, self._model, self._device = loaded[0], loaded[1], loaded[2]
            self.model_revision = "unknown"

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
            return  # Never cache transient errors as final decisions
        try:
            from app.core.redis_client import get_redis
            r = get_redis()
            r.set(cache_key, verdict.value, ex=ttl_seconds)
        except Exception:
            pass

    def verify(self, image_path: Path | Any, query: str, box: tuple[float, float, float, float] | None = None) -> bool:
        """True when the image clearly shows `query`."""
        res = self.verify_batch([image_path], query)[0]
        return res is True

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
                    continue  # Short-circuit failure
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
                    continue  # Short-circuit success
                if v == VlmVerdict.YES:
                    accum[i] = VlmVerdict.YES
                elif v == VlmVerdict.UNCERTAIN and accum[i] != VlmVerdict.YES:
                    accum[i] = VlmVerdict.UNCERTAIN
                elif v == VlmVerdict.ERROR and accum[i] not in (VlmVerdict.YES, VlmVerdict.UNCERTAIN):
                    accum[i] = VlmVerdict.ERROR

        return accum

    def verify_batch(self, image_paths: list[Any], query: str) -> list[bool | None]:
        """Backward-compatible batch interface returning True (YES), False (NO), None (UNCERTAIN/ERROR)."""
        verdicts = self.verify_batch_verdicts(image_paths, query)
        out: list[bool | None] = []
        for v in verdicts:
            if v == VlmVerdict.YES:
                out.append(True)
            elif v == VlmVerdict.NO:
                out.append(False)
            else:
                out.append(None)
        return out

    def verify_batch_verdicts(
        self,
        image_paths: list[Any],
        query: str,
        fingerprints: list[str] | None = None,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        logical_operator: str = "EXACT",
    ) -> list[VlmVerdict]:
        """Evaluates a batch of images using TinyVLM with genuine batching and caching.

        Returns:
          list of VlmVerdict (YES, NO, UNCERTAIN, ERROR)
        """
        if not image_paths:
            return []

        import torch

        question = build_question(query)
        verdicts: list[VlmVerdict | None] = [None] * len(image_paths)
        indices_to_infer: list[int] = []

        # 1. Check cache first
        for idx, item in enumerate(image_paths):
            fp = None
            if fingerprints and idx < len(fingerprints):
                fp = fingerprints[idx]
            elif hasattr(item, "checksum_sha256") and item.checksum_sha256:
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

        # 2. Process remaining images via genuine batching (chunks of batch_size)
        for i in range(0, len(indices_to_infer), batch_size):
            chunk_indices = indices_to_infer[i : i + batch_size]
            chunk_images: list[Image.Image] = []
            valid_chunk_indices: list[int] = []

            for c_idx in chunk_indices:
                raw_item = image_paths[c_idx]
                try:
                    if hasattr(raw_item, "to_pil_image"):
                        pil_img = raw_item.to_pil_image().convert("RGB")
                    elif hasattr(raw_item, "read"):
                        raw_item.seek(0)
                        pil_img = Image.open(raw_item).convert("RGB")
                    elif isinstance(raw_item, Image.Image):
                        pil_img = raw_item.convert("RGB")
                    else:
                        pil_img = Image.open(raw_item).convert("RGB")

                    pil_img.thumbnail((_MAX_DIMENSION, _MAX_DIMENSION))
                    chunk_images.append(pil_img)
                    valid_chunk_indices.append(c_idx)
                except Exception as e:
                    logger.warning(f"[TinyVLM] Could not load image for asset index {c_idx}: {e}")
                    verdicts[c_idx] = VlmVerdict.ERROR

            if not chunk_images:
                continue

            # Attempt genuine batch inference with OOM fallback
            try:
                prompts = []
                for _ in chunk_images:
                    msgs = [{"role": "user", "content": [{"type": "image"}, {"type": "text", "text": question}]}]
                    prompts.append(self._processor.apply_chat_template(msgs, add_generation_prompt=True))

                proc_kwargs = {
                    "text": prompts,
                    "images": chunk_images,
                    "return_tensors": "pt",
                    "padding": True,
                }
                if "smolvlm" in self.model_id.lower():
                    proc_kwargs["do_image_splitting"] = False

                inputs = self._processor(**proc_kwargs).to(self._device)
                with torch.inference_mode():
                    generated = self._model.generate(
                        **inputs, max_new_tokens=_MAX_NEW_TOKENS, do_sample=False
                    )

                answers = self._processor.batch_decode(
                    generated[:, inputs["input_ids"].shape[1] :], skip_special_tokens=True
                )

                for c_idx, ans in zip(valid_chunk_indices, answers):
                    v = _parse_verdict(ans)
                    verdicts[c_idx] = v
                    # Cache successful verdicts
                    fp = fingerprints[c_idx] if fingerprints and c_idx < len(fingerprints) else None
                    if fp:
                        ckey = _compute_vlm_cache_key(
                            fingerprint=fp,
                            query=query,
                            prompt_version=PROMPT_VERSION,
                            model_id=self.model_id,
                            model_revision=self.model_revision,
                            logical_operator=logical_operator,
                        )
                        self._set_cached_verdict(ckey, v)

            except (torch.cuda.OutOfMemoryError, RuntimeError) as oom_err:
                if "out of memory" in str(oom_err).lower():
                    logger.warning(f"[TinyVLM] CUDA OOM encountered during batch inference; recovering via bounded sequential processing: {oom_err}")
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()

                    # Fallback to sequential inference for this chunk
                    for c_idx, img in zip(valid_chunk_indices, chunk_images):
                        v = self._infer_single_safe(img, question)
                        verdicts[c_idx] = v
                        fp = fingerprints[c_idx] if fingerprints and c_idx < len(fingerprints) else None
                        if fp:
                            ckey = _compute_vlm_cache_key(
                                fingerprint=fp,
                                query=query,
                                prompt_version=PROMPT_VERSION,
                                model_id=self.model_id,
                                model_revision=self.model_revision,
                                logical_operator=logical_operator,
                            )
                            self._set_cached_verdict(ckey, v)
                else:
                    logger.error(f"[TinyVLM] Runtime error during batch inference: {oom_err}")
                    for c_idx in valid_chunk_indices:
                        verdicts[c_idx] = VlmVerdict.ERROR
            except Exception as e:
                logger.error(f"[TinyVLM] Unexpected exception during batch inference: {e}")
                for c_idx in valid_chunk_indices:
                    verdicts[c_idx] = VlmVerdict.ERROR

        return [v if v is not None else VlmVerdict.ERROR for v in verdicts]

    def _infer_single_safe(self, image: Image.Image, question: str) -> VlmVerdict:
        """Sequential single-image fallback with individual error handling."""
        import torch

        try:
            msgs = [{"role": "user", "content": [{"type": "image"}, {"type": "text", "text": question}]}]
            text = self._processor.apply_chat_template(msgs, add_generation_prompt=True)
            proc_kwargs = {"text": [text], "images": [image], "return_tensors": "pt"}
            if "smolvlm" in self.model_id.lower():
                proc_kwargs["do_image_splitting"] = False

            inputs = self._processor(**proc_kwargs).to(self._device)
            with torch.inference_mode():
                generated = self._model.generate(**inputs, max_new_tokens=_MAX_NEW_TOKENS, do_sample=False)
            ans = self._processor.batch_decode(
                generated[:, inputs["input_ids"].shape[1] :], skip_special_tokens=True
            )[0]
            return _parse_verdict(ans)
        except Exception as e:
            logger.warning(f"[TinyVLM] Sequential inference failed for image: {e}")
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            return VlmVerdict.ERROR
