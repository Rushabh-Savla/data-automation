"""Splitting a job query into the individual terms a connector matches on.

Connectors with a fixed vocabulary (nuImages categories, DAVIS sequence names)
used to test the *whole* query string against each label. That only works when
the query happens to be a single word: "car and person" is a substring of no
label, so both sources returned zero results for a perfectly reasonable job.
Matching per term ("car", "person") fixes that.

The stopword/connective handling is shared with the filtering stage
(app.filtering.query_expansion.content_terms) rather than copied, so "and",
"with" and the medium words ("scene", "video") are treated the same way in both
places.
"""

import re
from collections.abc import Iterable

from app.filtering.query_expansion import content_terms, singularize

__all__ = ["expand_terms", "match_labels", "query_terms", "singularize"]

# Everyday words a dataset's label list does not contain, mapped to the labels it
# does ("puppy" is not a COCO class, "dog" is). Small and deliberately literal: it
# only has to make the common cases hit their label. Anything it does not cover is
# left to the open-vocabulary scan, where the relevance filter decides.
_SYNONYMS: dict[str, tuple[str, ...]] = {
    "puppy": ("dog",), "pup": ("dog",), "hound": ("dog",), "retriever": ("dog",), "poodle": ("dog",),
    "kitten": ("cat",), "kitty": ("cat",),
    "automobile": ("car",), "auto": ("car",), "sedan": ("car",), "suv": ("car",), "taxi": ("car",),
    "cab": ("car",), "hatchback": ("car",),
    "lorry": ("truck",), "pickup": ("truck",),
    "bike": ("bicycle",), "cycle": ("bicycle",), "cyclist": ("bicycle", "person"),
    "motorbike": ("motorcycle",), "scooter": ("motorcycle",),
    "human": ("person",), "people": ("person",), "man": ("person",), "woman": ("person",),
    "men": ("person",), "women": ("person",), "boy": ("person",), "girl": ("person",),
    "kid": ("person",), "child": ("person",), "baby": ("person",), "crowd": ("person",),
    "pedestrian": ("person",), "walker": ("person",), "rider": ("person",),
    "plane": ("aeroplane", "airplane"), "jet": ("aeroplane", "airplane"), "aircraft": ("aeroplane", "airplane"),
    "ship": ("boat",), "yacht": ("boat",), "canoe": ("boat",),
    "tv": ("tvmonitor", "tv", "television"), "television": ("tvmonitor", "tv"),
    "monitor": ("tvmonitor", "tv"), "screen": ("tvmonitor", "tv"),
    "couch": ("sofa",), "settee": ("sofa",), "sofa": ("couch",),
    "cellphone": ("cell phone", "phone"), "smartphone": ("cell phone", "phone"), "mobile": ("cell phone", "phone"),
    "laptop": ("laptop", "computer"), "notebook": ("laptop",),
    "cup": ("cup", "mug"), "mug": ("cup", "mug"),
    "hen": ("bird",), "chicken": ("bird",), "duck": ("bird",), "parrot": ("bird",), "seagull": ("bird",),
    "pony": ("horse",), "stallion": ("horse",), "cattle": ("cow",), "bull": ("cow",), "ox": ("cow",),
    "lamb": ("sheep",), "table": ("dining table", "table", "diningtable"),
    "desk": ("dining table", "table", "diningtable"),
    "vehicle": ("car", "truck", "bus", "motorcycle", "bicycle", "van"),
    "person": ("pedestrian",),
    "plant": ("pottedplant", "potted plant"), "motorcycle": ("motorbike",), "airplane": ("aeroplane",),
}


def query_terms(query: str) -> list[str]:
    """Ordered, de-duplicated, singular subject terms: "cars and people" -> ["car", "person"]."""
    terms: list[str] = []
    for word in content_terms(query):
        term = singularize(word)
        if term not in terms:
            terms.append(term)
    return terms


def expand_terms(query: str) -> list[str]:
    """query_terms plus the labels their common synonyms stand for, de-duplicated."""
    expanded: list[str] = []
    for term in query_terms(query):
        for candidate in (term, *_SYNONYMS.get(term, ())):
            if candidate not in expanded:
                expanded.append(candidate)
    return expanded


def match_labels(query: str, labels: Iterable[str]) -> set[str]:
    """The dataset labels a query is asking for, for any query.

    A label matches when it, or one of its words, equals a query term or a synonym
    of one ("puppy" -> dog, "bike" -> bicycle, "cars" -> car, "tv monitor" <- tv),
    or when it appears whole inside the query ("a red car parked" -> car). The
    previous whole-query substring test only worked for a query that happened to be
    a label, or contain one, so most queries matched nothing.

    Empty is a normal answer, not an error: the caller then offers unlabelled
    images and lets the relevance filter decide.
    """
    labels = list(labels)
    query_lower = query.strip().lower()
    terms = set(expand_terms(query))
    matched: set[str] = set()
    for label in labels:
        label_lower = label.lower()
        words = {singularize(w) for w in re.split(r"[\s._/-]+", label_lower) if w}
        if (
            label_lower in terms
            or (words & terms)
            # Whole words only: "phone" is not in "xylophone", "car" is not in "scarf".
            or (len(label_lower) > 1 and re.search(rf"\b{re.escape(label_lower)}\b", query_lower))
        ):
            matched.add(label)
    return matched
