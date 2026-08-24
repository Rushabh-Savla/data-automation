"""VisualVerifier — hybrid two-stage relevance check: YOLO-World
(app/filtering/yolo_world_filter.py) proposes *where* in the image to look,
SmolVLM-256M judges the full natural-language query against that region —
used from app/tasks/filtering_tasks.py::run_relevance_filter as the sole
accept/reject decision for every source.

YOLO-World is an open-vocabulary *object detector*: it can only answer
"does some region match one of these class prompts," which structurally
can't represent a compound query like "red cane" (no color understanding)
or "people wearing helmet" (no compositional "wearing" relationship) — its
score alone previously let mismatched images through (e.g. any cane at all
satisfying a bare "cane" fallback class, regardless of color). There's no
fixed keyword list that can enumerate every way a query can be compound
(color, negation, counting, spatial relations...), so instead of trying to
classify query types, YOLO-World's detection is used only to crop toward
the relevant region, and SmolVLM — a real language-grounded VQA model —
answers the full original query against that crop. This also incidentally
helps SmolVLM's own known weakness (missing a small/background object in a
busy full-image downscale), since the crop presents a zoomed, more
prominent view of the candidate region instead of a shrunk full scene.
"""

import logging
from functools import lru_cache
from pathlib import Path

from PIL import Image

logger = logging.getLogger(__name__)

_AFFIRMATIVE_PREFIXES = ("yes",)
# SmolVLM's default image processing splits an image into multiple
# sub-tiles (do_image_splitting=True, up to a 2048px longest edge) for
# fine-grained detail — measured at 20+ minutes of CPU time per image on a
# real ~640px photo. This check only needs a coarse whole-image judgment
# ("global classification", not fine detail), so splitting is disabled and
# the image is downscaled first — measured at ~6 seconds per image with no
# accuracy change on clear single-subject test images.
_MAX_DIMENSION = 384
_CROP_MARGIN_RATIO = 0.25


def _is_affirmative(text: str) -> bool:
    """True if a short VQA response reads as "yes" (case/punctuation-insensitive,
    tolerant of a leading article like "Yes." or "yes, it does")."""
    cleaned = text.strip().lower().lstrip(".,!? ")
    return cleaned.startswith(_AFFIRMATIVE_PREFIXES)


def _crop_with_margin(
    image: Image.Image, box: tuple[float, float, float, float] | None, margin_ratio: float = _CROP_MARGIN_RATIO
) -> Image.Image:
    """Crops `image` to `box` (xyxy) expanded by `margin_ratio` on each side
    and clamped to the image bounds. The margin keeps surrounding context a
    relational query needs (e.g. "wearing" depends on something *near* the
    detected region, not just the region itself). Returns `image` unchanged
    if `box` is None — the caller had no detection to crop toward."""
    if box is None:
        return image

    width, height = image.size
    x1, y1, x2, y2 = box
    box_w, box_h = x2 - x1, y2 - y1
    pad_x, pad_y = box_w * margin_ratio, box_h * margin_ratio

    left = max(0, x1 - pad_x)
    top = max(0, y1 - pad_y)
    right = min(width, x2 + pad_x)
    bottom = min(height, y2 + pad_y)

    if right <= left or bottom <= top:
        return image
    return image.crop((left, top, right, bottom))


@lru_cache(maxsize=1)
def _load_model(model_id: str = "HuggingFaceTB/SmolVLM-256M-Instruct"):
    from transformers import AutoModelForImageTextToText, AutoProcessor

    processor = AutoProcessor.from_pretrained(model_id)
    model = AutoModelForImageTextToText.from_pretrained(model_id)
    return processor, model


class VisualVerifier:
    def __init__(self, model_id: str = "HuggingFaceTB/SmolVLM-256M-Instruct"):
        self._processor, self._model = _load_model(model_id)

    def verify(self, image_path: Path, query: str, box: tuple[float, float, float, float] | None = None) -> bool:
        """Yes/no check: does this image (cropped to `box`, if given) show
        `query`? `query` is the full, unmodified natural-language query —
        no keyword extraction — so any phrasing (color, "wearing X",
        negation, counting...) is judged as the user actually wrote it."""
        try:
            image = Image.open(image_path).convert("RGB")
            image = _crop_with_margin(image, box)
            image.thumbnail((_MAX_DIMENSION, _MAX_DIMENSION))
            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "image"},
                        {"type": "text", "text": f"Does this image show {query}? Answer with only yes or no."},
                    ],
                }
            ]
            prompt = self._processor.apply_chat_template(messages, add_generation_prompt=True)
            inputs = self._processor(text=prompt, images=[image], do_image_splitting=False, return_tensors="pt")
            generated = self._model.generate(**inputs, max_new_tokens=5)
            response = self._processor.batch_decode(
                generated[:, inputs["input_ids"].shape[1] :], skip_special_tokens=True
            )[0]
            return _is_affirmative(response)
        except Exception as e:
            logger.warning(f"SmolVLM verification failed for {image_path}: {e}")
            return False
