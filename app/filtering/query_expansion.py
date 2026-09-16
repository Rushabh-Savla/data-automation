"""Turning a free-text job query into something a vision model can actually score.

The previous approach (derive_prompt_classes, since removed) split
the query on connectives and handed the fragments to the detector as class
prompts. On a real job -- query "weapon on movies scence", target 500 -- that
produced:

    ['weapon on movies scence', 'weapon', 'movies', 'scence']

Three of those four are not objects at all ("movies", "scence", and the whole
sentence), and the one that is, "weapon", is a superordinate category rather
than a thing with a consistent appearance. Measured on that job's own frames,
YOLO-World returned a confidence of 0.0 on all 754 assets even with the
threshold dropped to 0.05 -- the detector never fired once, so the region
proposal was always None and the downstream VLM always judged a whole
downscaled frame. Swapping in concrete nouns ("gun", "pistol", "rifle") on the
same frames produced real detections.

Two fixes follow from that, and this module implements both:

1. Expand superordinate categories to the concrete nouns that actually ground
   in CLIP/YOLO-World's text encoder. "weapon" cannot be detected; "pistol"
   can.
2. Normalise the phrasing before it reaches a language-grounded model. The
   VLM was being asked, verbatim, "Does this image show weapon on movies
   scence?" -- a typo'd, ungrammatical question that a 256M-parameter model
   has little chance of answering consistently.

Everything here is deterministic and offline: a static map plus string
rewriting, with per-job overrides through job.config. No model, no network,
no LLM call -- so it is fast enough to run per batch, unit-testable without
fixtures, and produces the same prompts today and next month. A job that
needs vocabulary this map does not cover sets its own terms in config rather
than waiting on a code change.
"""

import re

# Superordinate -> concrete, visually-groundable nouns. Keys are intentionally
# the abstract words users actually type; values are what a detector or CLIP
# text encoder can match. This is not meant to be exhaustive -- it covers the
# categories this platform is used for, and anything missing can be supplied
# per job via config["relevance"]["classes"].
_CATEGORY_EXPANSIONS: dict[str, list[str]] = {
    "weapon": ["gun", "pistol", "rifle", "handgun", "firearm", "knife", "sword"],
    "firearm": ["gun", "pistol", "rifle", "handgun", "shotgun"],
    "vehicle": ["car", "truck", "bus", "van", "motorcycle"],
    "animal": ["dog", "cat", "horse", "bird", "cow"],
    "person": ["person", "man", "woman", "pedestrian"],
    "people": ["person", "man", "woman", "crowd of people"],
    "furniture": ["chair", "table", "sofa", "bed", "desk"],
    "tool": ["hammer", "screwdriver", "wrench", "drill"],
    "fruit": ["apple", "banana", "orange", "grapes"],
    "bag": ["backpack", "handbag", "suitcase", "shopping bag"],
    "ppe": ["helmet", "safety vest", "hard hat", "safety goggles"],
}

# Dropped when reducing a query to its subject. "scene"/"movie" and friends
# describe the *medium* the subject appears in, not the subject -- keeping
# them as detector classes is what produced the useless "movies"/"scence"
# prompts. Misspellings are included deliberately: users type them, and a
# typo should not become a detection class.
_CONTEXT_WORDS = {
    "scene", "scenes", "scence", "scences", "sence",
    "movie", "movies", "film", "films", "cinema",
    "video", "videos", "clip", "clips", "footage",
    "image", "images", "photo", "photos", "picture", "pictures",
    "shot", "shots", "frame", "frames",
}

# Words carrying no subject information on their own. Note what is NOT here:
# "with", "wearing", "holding", "without" and similar relational words are
# deliberately preserved, because they are the whole meaning of a compound
# query -- "people with red bag" and "people" are different requests, and
# flattening the first into the second is exactly the over-matching the
# filtering pipeline is supposed to prevent.
_STOPWORDS = {
    "a", "an", "the", "of", "at", "from", "for", "to", "by",
    "is", "are", "was", "were", "be", "being", "been",
    "that", "this", "these", "those", "some", "any",
}

# Relational words kept inside a phrase but meaningless dangling at either
# end -- "weapon on <movies scene>" loses its object once the context words
# go, leaving a trailing "on" that would otherwise reach the model.
_CONNECTIVES = {"with", "wearing", "holding", "without", "on", "in", "near", "next", "and", "or"}

_PLURAL_EXCEPTIONS = {"people", "men", "women", "children", "police"}

# Prompt ensemble. CLIP's raw cosine similarities sit in a narrow band
# (~0.20-0.26 on real data), so a single template's absolute score is not a
# usable threshold. Averaging several phrasings is the standard way to get a
# more stable direction for a concept.
_POSITIVE_TEMPLATES = (
    "a photo of {}",
    "a photo containing {}",
    "a close-up photo of {}",
    "a movie still showing {}",
)

# Background/contrast prompts. These are what the positive prompts are scored
# *against*: relevance is the probability mass on the positives once these are
# in the same softmax, not a bare similarity. They deliberately cover the
# things a video-frame pipeline actually dredges up -- title cards, talking
# heads, empty establishing shots -- because those were the bulk of the noise
# on the job that motivated this module.
_NEGATIVE_PROMPTS = (
    "a photo of an empty room",
    "a photo of scenery",
    "a close-up photo of a person's face",
    "a person talking to the camera",
    "a screenshot of text",
    "a title card with text",
    "a dark image with nothing visible",
    "a blurry photo of nothing in particular",
    "a random unrelated object",
    "a computer user interface",
)


def _tokens(query: str) -> list[str]:
    return re.findall(r"[a-z]+", query.lower())


def subject_phrase(query: str) -> str:
    """The query reduced to what should actually be visible, without an article.

    Drops the words describing the medium ("movies", "scence") and bare
    stopwords, then trims connectives left dangling at either end. Internal
    structure is preserved:

        "weapon on movies scence"   -> "weapon"
        "people with red bag"       -> "people with red bag"
        "pedestrian wearing helmet" -> "pedestrian wearing helmet"

    Falls back to the cleaned original if stripping would leave nothing, so an
    all-context query still describes something rather than becoming empty.
    """
    words = _tokens(query)
    if not words:
        return query.strip()

    subject = [w for w in words if w not in _CONTEXT_WORDS and w not in _STOPWORDS]
    if not subject:
        subject = [w for w in words if w not in _STOPWORDS] or words

    while subject and subject[0] in _CONNECTIVES:
        subject.pop(0)
    while subject and subject[-1] in _CONNECTIVES:
        subject.pop()

    return " ".join(subject) if subject else " ".join(words)


def _needs_article(phrase: str) -> bool:
    """Whether "a" reads correctly in front of this phrase.

    "a weapon" is right; "a people with red bag" is not. Plurals and the
    irregular plurals in _PLURAL_EXCEPTIONS take no article.
    """
    if not phrase:
        return False
    head = phrase.split()[0]
    if head in _PLURAL_EXCEPTIONS:
        return False
    # Regular plural, but not words like "glass"/"dress" that merely end in s.
    return not (head.endswith("s") and not head.endswith("ss"))


def normalize_query(query: str) -> str:
    """The query as a natural noun phrase, for use in a VQA question.

    The VLM is asked "Does this image show {this}?", so the phrase has to
    read as English. Previously the raw query went in verbatim, producing
    "Does this image show weapon on movies scence?" -- typo, no article, and
    a trailing fragment. That is a question a 256M-parameter model cannot
    answer reliably, and its answer was the sole accept/reject decision.
    """
    phrase = subject_phrase(query)
    if phrase.startswith(("a ", "an ", "the ")):
        return phrase
    return f"a {phrase}" if _needs_article(phrase) else phrase


def derive_classes(query: str, overrides: list[str] | None = None) -> list[str]:
    """Concrete, detectable nouns for this query.

    `overrides` (job.config["relevance"]["classes"]) wins outright -- an
    operator who knows their domain should not have to fight the static map.
    """
    if overrides:
        return [c.strip() for c in overrides if c and c.strip()]

    # Connectives are excluded here even though subject_phrase keeps them:
    # "wearing" belongs in the phrase handed to a language model, but as a
    # standalone detector class it is meaningless and only dilutes the list.
    words = [
        w
        for w in _tokens(query)
        if w not in _CONTEXT_WORDS and w not in _STOPWORDS and w not in _CONNECTIVES and len(w) > 2
    ]

    classes: list[str] = []
    for word in words:
        for expansion in _CATEGORY_EXPANSIONS.get(word, [word]):
            if expansion not in classes:
                classes.append(expansion)

    # Multi-word subjects ("red bag") carry information that the individual
    # tokens lose, so keep the whole subject phrase as a class too.
    subject = subject_phrase(query)
    if len(subject.split()) > 1 and subject not in classes:
        classes.insert(0, subject)

    # Detector accuracy degrades as the class list grows; keep the strongest few.
    return classes[:10]


def positive_prompts(query: str, overrides: list[str] | None = None) -> list[str]:
    """The prompt ensemble representing "this image is relevant"."""
    if overrides:
        return [p.strip() for p in overrides if p and p.strip()]

    subject = subject_phrase(query)
    terms = [subject] + [c for c in derive_classes(query) if c != subject]

    prompts: list[str] = []
    for term in terms[:6]:
        for template in _POSITIVE_TEMPLATES:
            prompt = template.format(term)
            if prompt not in prompts:
                prompts.append(prompt)
    return prompts


def negative_prompts(overrides: list[str] | None = None) -> list[str]:
    """The contrast set that turns a bare similarity into a calibrated score."""
    if overrides:
        return [p.strip() for p in overrides if p and p.strip()]
    return list(_NEGATIVE_PROMPTS)
