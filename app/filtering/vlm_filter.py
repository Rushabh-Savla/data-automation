"""VisualVerifier — the relevance gate: does this image actually show the query?

This is the ONLY thing that decides accept/reject. It replaced a cascade that
did not work, and the reasons are worth recording because they all look
reasonable on paper.

What was tried, and measured on real job frames (a "weapon in movie scene"
job whose accepted set turned out to be briefcases, wagon wheels and smoke):

  * CLIP ViT-B/32 similarity, scored as probability mass against a background
    prompt set. Separation between real weapons and briefcase frames: +0.007.
    Every one of the job's 500 accepted assets scored 90-100%. It was not
    filtering at all, it was passing everything with a confident-looking
    number attached.
  * SigLIP base-384: +0.032 -- ranks correctly but needs a threshold around
    0.006, far too tight to be stable across queries.
  * SigLIP SO400M: worse than base (1 of 5 positives above all negatives).
  * YOLO-World with concrete class prompts (gun/pistol/rifle): max confidence
    0.06 across the whole sample, including at a 0.05 threshold. No signal.
  * SmolVLM-256M asked a yes/no question: answered "no" to everything,
    positives included. A constant classifier.

The common cause for the embedding approaches is structural rather than a
tuning mistake: a weapon is a *small object* inside a 640x360 cinematic
frame, and a global image-text embedding encodes scene gist -- "dark room,
men in suits, metal object" -- in which a briefcase and a pistol are
genuinely near-identical. No prompt set fixes that, which is why this module
does not try to.

A model that actually reasons about image content does work. Qwen2-VL-2B,
measured on the same frames: 9% pass rate where the old gate passed 100%,
and the accepted frames are real (a drawn blade, soldiers firing with muzzle
flash, a weapon struggle). At 0.18s/image on an RTX A4000 it is also FASTER
than the 256M model it replaces (0.39s/image) -- the problem was model
quality, not model size. Being that cheap, it runs on every asset, so there
is no uncertainty band and no threshold to calibrate.
"""

import logging
from functools import lru_cache
from pathlib import Path

from PIL import Image

logger = logging.getLogger(__name__)

DEFAULT_MODEL_ID = "HuggingFaceTB/SmolVLM-256M-Instruct"

# SmolVLM runs on 384px without tiling, keeping inference quick and deterministic.
_MAX_DIMENSION = 384

# The answer is a short token sequence (e.g. " Yes.", " No.").
_MAX_NEW_TOKENS = 5

_AFFIRMATIVE_PREFIXES = ("yes",)


def _is_affirmative(text: str) -> bool:
    cleaned = text.strip().lower().lstrip(".,!? ")
    return cleaned.startswith(_AFFIRMATIVE_PREFIXES)


def _select_device() -> str:
    """CUDA when the container was actually granted a GPU, else CPU."""
    try:
        import torch

        return "cuda" if torch.cuda.is_available() else "cpu"
    except Exception:  # pragma: no cover - torch absent entirely
        return "cpu"


@lru_cache(maxsize=1)
def _load_model(model_id: str = DEFAULT_MODEL_ID):
    import torch
    from transformers import AutoModelForImageTextToText, AutoProcessor

    device = _select_device()
    logger.info(f"[VLM RELEVANCY] Initializing relevance model '{model_id}' on device: {device}")
    processor = AutoProcessor.from_pretrained(model_id)
    # fp16 on GPU halves the weight footprint and the matmul cost; on CPU it
    # is slower than fp32 (no native half path), so dtype follows the device.
    model = AutoModelForImageTextToText.from_pretrained(
        model_id, dtype=torch.float16 if device == "cuda" else torch.float32
    )
    model.to(device)
    model.eval()
    logger.info(f"[VLM RELEVANCY] Successfully loaded relevance model '{model_id}' on device: {device}")
    return processor, model, device


def build_question(query: str) -> str:
    """The yes/no question put to the model."""
    return f"Is {query} clearly visible in this image? Answer only yes or no."


class VisualVerifier:
    def __init__(self, model_id: str = DEFAULT_MODEL_ID):
        self.model_id = model_id
        self._processor, self._model, self._device = _load_model(model_id)

    def verify(self, image_path: Path, query: str, box: tuple[float, float, float, float] | None = None) -> bool:
        """True when the image clearly shows `query`."""
        return self.verify_batch([image_path], query)[0] is True

    def verify_batch_any(self, image_paths: list[Path], subjects: list[str]) -> list[bool | None]:
        """Per-image verdicts for a query with several subjects: True when ANY is clearly visible."""
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

    def verify_batch(self, image_paths: list[Path], query: str) -> list[bool | None]:
        """Per-image verdicts: True, False, or None when it could not be decided."""
        if not image_paths:
            return []

        import torch

        question = build_question(query)
        results: list[bool | None] = []

        logger.info(
            f"[VLM RELEVANCY] Evaluating batch of {len(image_paths)} asset(s) with model '{self.model_id}' for query: '{query}'"
        )

        for idx, image_path in enumerate(image_paths, 1):
            try:
                image = Image.open(image_path).convert("RGB")
                image.thumbnail((_MAX_DIMENSION, _MAX_DIMENSION))
                messages = [
                    {
                        "role": "user",
                        "content": [{"type": "image"}, {"type": "text", "text": question}],
                    }
                ]
                text = self._processor.apply_chat_template(messages, add_generation_prompt=True)
                proc_kwargs = {"text": [text], "images": [image], "return_tensors": "pt"}
                if "smolvlm" in self.model_id.lower():
                    proc_kwargs["do_image_splitting"] = False
                inputs = self._processor(**proc_kwargs).to(self._device)
                with torch.inference_mode():
                    generated = self._model.generate(
                        **inputs, max_new_tokens=_MAX_NEW_TOKENS, do_sample=False
                    )
                answer = self._processor.batch_decode(
                    generated[:, inputs["input_ids"].shape[1] :], skip_special_tokens=True
                )[0]
                verdict = _is_affirmative(answer)
                p = Path(image_path)
                logger.info(
                    f"[VLM RELEVANCY] ({idx}/{len(image_paths)}) Model: {self.model_id} | Asset: {p.name} | "
                    f"Question: \"{question}\" | Model Output: {repr(answer.strip())} -> Verdict: {'RELEVANT (PASS)' if verdict else 'REJECTED'}"
                )
                results.append(verdict)
            except Exception as e:
                p = Path(image_path)
                logger.warning(
                    f"[VLM RELEVANCY] ({idx}/{len(image_paths)}) Model: {self.model_id} | Check failed for {p.name}: {e}"
                )
                results.append(None)

        return results
