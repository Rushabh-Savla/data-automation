"""VisualVerifier — whole-image yes/no relevance check using SmolVLM-256M,
used alongside YOLO-World (app/filtering/yolo_world_filter.py) as a second
opinion in run_relevance_filter.

YOLO-World is an open-vocabulary *object detector*: it looks for a
region of the image matching a class prompt. That's a good fit for "is
there a red bag somewhere in this image" but a weaker fit for a query
that's really a whole-image classification question ("is this a dog
photo, or a cat photo") — which is exactly the failure mode that let
mismatched images (e.g. cats from a Kaggle "dog vs cat classification"
dataset) through undetected. SmolVLM-256M is a small vision-language
model that can answer a direct yes/no question about the whole image,
complementing YOLO-World's region-based signal.
"""

import logging
from functools import lru_cache
from pathlib import Path

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


def _is_affirmative(text: str) -> bool:
    """True if a short VQA response reads as "yes" (case/punctuation-insensitive,
    tolerant of a leading article like "Yes." or "yes, it does")."""
    cleaned = text.strip().lower().lstrip(".,!? ")
    return cleaned.startswith(_AFFIRMATIVE_PREFIXES)


@lru_cache(maxsize=1)
def _load_model(model_id: str = "HuggingFaceTB/SmolVLM-256M-Instruct"):
    from transformers import AutoModelForImageTextToText, AutoProcessor

    processor = AutoProcessor.from_pretrained(model_id)
    model = AutoModelForImageTextToText.from_pretrained(model_id)
    return processor, model


class VisualVerifier:
    def __init__(self, model_id: str = "HuggingFaceTB/SmolVLM-256M-Instruct"):
        self._processor, self._model = _load_model(model_id)

    def verify(self, image_path: Path, query: str) -> bool:
        """Yes/no whole-image check: does this image actually show `query`?"""
        try:
            from PIL import Image

            image = Image.open(image_path).convert("RGB")
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
