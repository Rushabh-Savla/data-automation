"""RelevanceFilter — open-vocabulary relevance scoring using YOLO-World,
prompted with the job's own free-text query.

Per the architecture doc, this replaces per-connector ad-hoc verification
(the legacy ddg_downloader.py's plain YOLOv8 + fixed 80 COCO classes +
K-means dominant-color matching) with a single shared, open-vocabulary
filter that isn't limited to a fixed class list — "red bag", "pedestrian",
or any other free-text query can be used directly as a detection prompt,
which a closed-vocabulary detector cannot do.
"""

import logging
import re
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

_STOPWORDS = {"a", "an", "the", "with", "and", "or", "of", "in", "on", "at", "wearing"}


def derive_prompt_classes(query: str) -> list[str]:
    """Turns a free-text job query into a handful of YOLO-World class prompts.

    Splits on common connective words/punctuation so a query like
    "people with red bag" yields candidate phrases ["people with red bag",
    "people", "red bag"] — the full query first (most specific), then its
    parts, giving YOLO-World several chances to match without needing a
    hardcoded class vocabulary.
    """
    query = query.strip().lower()
    if not query:
        return []

    parts = re.split(r"\bwith\b|\band\b|,", query)
    parts = [p.strip() for p in parts if p.strip()]

    classes = [query] + [p for p in parts if p != query]

    # Also add individual non-stopword tokens from the full query as a last
    # resort broad match (e.g. "bag" alone if "red bag" doesn't match).
    tokens = [t for t in re.findall(r"[a-z]+", query) if t not in _STOPWORDS and len(t) > 2]
    for t in tokens:
        if t not in classes:
            classes.append(t)

    return classes[:10]  # YOLO-World performance degrades with too many classes


@lru_cache(maxsize=1)
def _load_model(weights: str = "yolov8s-world.pt"):
    from ultralytics import YOLOWorld

    return YOLOWorld(weights)


class RelevanceFilter:
    def __init__(self, weights: str = "yolov8s-world.pt", conf_threshold: float = 0.15):
        self.conf_threshold = conf_threshold
        self._model = _load_model(weights)
        self._current_classes: list[str] | None = None

    def score(self, image_path: Path, prompt_classes: list[str]) -> float:
        """Returns the highest detection confidence across any prompt class,
        or 0.0 if nothing was detected."""
        if not prompt_classes:
            return 0.0
        if prompt_classes != self._current_classes:
            self._model.set_classes(prompt_classes)
            self._current_classes = prompt_classes

        try:
            results = self._model.predict(source=str(image_path), conf=self.conf_threshold, verbose=False)
        except Exception as e:
            logger.warning(f"YOLO-World inference failed for {image_path}: {e}")
            return 0.0

        if not results:
            return 0.0
        boxes = results[0].boxes
        if boxes is None or len(boxes) == 0:
            return 0.0
        return float(boxes.conf.max())
