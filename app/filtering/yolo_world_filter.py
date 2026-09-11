"""RelevanceFilter — open-vocabulary relevance scoring using YOLO-World,
prompted with the job's own free-text query.

Per the architecture doc, this replaces per-connector ad-hoc verification
(the legacy ddg_downloader.py's plain YOLOv8 + fixed 80 COCO classes +
K-means dominant-color matching) with a single shared, open-vocabulary
filter that isn't limited to a fixed class list — "red bag", "pedestrian",
or any other free-text query can be used directly as a detection prompt,
which a closed-vocabulary detector cannot do.

Changes (2024-09):
- conf_threshold default raised from 0.15 → 0.25. At 0.15 YOLO-World
  passes too many weakly-matched detections through to SmolVLM, adding
  unnecessary inference cost without improving recall (SmolVLM is the
  sole accept/reject gatekeeper; YOLO-World's role is region proposal,
  not final scoring). 0.25 sits in the empirically validated 0.2–0.3
  sweet-spot: enough signal to localise a real candidate region, tight
  enough to suppress background false-positives. The default is also
  the fallback value in filtering_tasks.py's job.config lookup —
  both must agree; see that module.
"""

import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DetectionResult:
    score: float
    box: tuple[float, float, float, float] | None  # xyxy, in the source image's own pixel coordinates

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
    # Sentinel used in filtering_tasks.py's job.config fallback — keep in
    # sync. Defined here so there is exactly one source of truth.
    DEFAULT_CONF_THRESHOLD: float = 0.25

    def __init__(self, weights: str = "yolov8s-world.pt", conf_threshold: float = DEFAULT_CONF_THRESHOLD):
        self.conf_threshold = conf_threshold
        self._model = _load_model(weights)
        self._current_classes: list[str] | None = None

    def detect(self, image_path: Path, prompt_classes: list[str]) -> DetectionResult:
        """Runs one inference pass and returns both the highest detection
        confidence across any prompt class AND that detection's own box —
        the box is a region-proposal hint for a downstream semantic check
        (see app/filtering/smolvlm_filter.py), not a standalone pass/fail
        signal: YOLO-World's flat "does any prompt class match anywhere"
        design can't represent compound queries (color, "wearing X", etc.),
        so the actual accept/reject decision belongs to whatever looks at
        this box next, not to this score alone."""
        if not prompt_classes:
            return DetectionResult(score=0.0, box=None)
        if prompt_classes != self._current_classes:
            self._model.set_classes(prompt_classes)
            self._current_classes = prompt_classes

        try:
            results = self._model.predict(source=str(image_path), conf=self.conf_threshold, verbose=False)
        except Exception as e:
            logger.warning(f"YOLO-World inference failed for {image_path}: {e}")
            return DetectionResult(score=0.0, box=None)

        if not results:
            return DetectionResult(score=0.0, box=None)
        boxes = results[0].boxes
        if boxes is None or len(boxes) == 0:
            return DetectionResult(score=0.0, box=None)

        best_idx = int(boxes.conf.argmax())
        score = float(boxes.conf[best_idx])
        box = tuple(float(v) for v in boxes.xyxy[best_idx].tolist())
        return DetectionResult(score=score, box=box)

    def score(self, image_path: Path, prompt_classes: list[str]) -> float:
        """Returns the highest detection confidence across any prompt class,
        or 0.0 if nothing was detected."""
        return self.detect(image_path, prompt_classes).score