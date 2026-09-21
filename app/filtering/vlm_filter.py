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

DEFAULT_MODEL_ID = "Qwen/Qwen2-VL-2B-Instruct"

# Large enough that a small object survives the downscale -- the failure mode
# of the previous 384px gate -- and still well inside the model's budget.
_MAX_DIMENSION = 512

# The answer is one token. Anything longer is the model ignoring the
# instruction, which _is_affirmative treats as "not a yes".
_MAX_NEW_TOKENS = 4

_AFFIRMATIVE_PREFIXES = ("yes",)


def _is_affirmative(text: str) -> bool:
    cleaned = text.strip().lower().lstrip(".,!? ")
    return cleaned.startswith(_AFFIRMATIVE_PREFIXES)


def _select_device() -> str:
    """CUDA when the container was actually granted a GPU, else CPU.

    Guarded rather than assumed: the same image runs on the CPU-only IO
    worker, and torch.cuda.is_available() is also False when the installed
    wheel's CUDA build outruns the host driver. Falling back keeps the filter
    working rather than crashing the worker at import.
    """
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
    processor = AutoProcessor.from_pretrained(model_id)
    # fp16 on GPU halves the weight footprint and the matmul cost; on CPU it
    # is slower than fp32 (no native half path), so dtype follows the device.
    model = AutoModelForImageTextToText.from_pretrained(
        model_id, dtype=torch.float16 if device == "cuda" else torch.float32
    )
    model.to(device)
    model.eval()
    logger.info(f"Loaded relevance VLM {model_id} on {device}")
    return processor, model, device


def build_question(query: str) -> str:
    """The yes/no question put to the model.

    Two things matter here. "clearly visible" is deliberate: without it the
    model answers about what the scene is *about* rather than what is in the
    frame, and a conversation scene from an action film counts as a weapon
    scene. And the query arrives already normalised by
    query_expansion.normalize_query, so a raw job string like
    "weapon on movies scence" is asked as "a weapon" -- the previous gate
    passed the typo through verbatim.
    """
    return f"Is {query} clearly visible in this image? Answer only yes or no."


class VisualVerifier:
    def __init__(self, model_id: str = DEFAULT_MODEL_ID):
        self._processor, self._model, self._device = _load_model(model_id)

    def verify(self, image_path: Path, query: str, box: tuple[float, float, float, float] | None = None) -> bool:
        """True when the image clearly shows `query`.

        `box` is accepted for call-site compatibility and ignored: it came
        from YOLO-World, whose detections measured 0.0 on every asset of a
        real job, so cropping to it was cropping to noise. The model is given
        the whole frame.
        """
        return self.verify_batch([image_path], query)[0] is True

    def verify_batch_any(self, image_paths: list[Path], subjects: list[str]) -> list[bool | None]:
        """Per-image verdicts for a query with several subjects: True when ANY is clearly visible.

        A query like "car and person" asks for either class, so each subject is
        put to the model as its own question -- the single-subject question is
        what was measured to work. An image stops being asked about once one
        subject is a yes, so the extra cost falls only on images that fail the
        first.

        The None/False distinction is preserved: an image is None (undecided,
        file kept) unless some subject said yes, or every subject was decided as
        no, because a rejection deletes the file.
        """
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
        """Per-image verdicts: True, False, or None when it could not be decided.

        None is a distinct third outcome, never collapsed into False, because
        the caller deletes the file on a rejection. An unreadable image or a
        model failure is an absence of evidence, not evidence of
        irrelevance -- the old gate conflated the two and destroyed images
        whenever a model merely failed to load.
        """
        if not image_paths:
            return []

        import torch

        question = build_question(query)
        results: list[bool | None] = []

        for image_path in image_paths:
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
                inputs = self._processor(text=[text], images=[image], return_tensors="pt").to(self._device)
                with torch.inference_mode():
                    generated = self._model.generate(
                        **inputs, max_new_tokens=_MAX_NEW_TOKENS, do_sample=False
                    )
                answer = self._processor.batch_decode(
                    generated[:, inputs["input_ids"].shape[1] :], skip_special_tokens=True
                )[0]
                results.append(_is_affirmative(answer))
            except Exception as e:
                logger.warning(f"Relevance check failed for {image_path}: {e}")
                results.append(None)

        return results
