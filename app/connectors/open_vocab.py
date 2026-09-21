"""Offering a dataset's other images when its labels cannot cover a query.

The dataset connectors (COCO, KITTI, Pascal VOC, nuImages, ...) match a query
against a fixed list of labels and used to give up -- "No COCO categories matched
'guitar'" -- for anything outside it: "puppy", "kitchen", "golden retriever",
"hospital". Those queries have images in these datasets (people play guitars in
COCO); the labels just do not name them.

So when labels cannot fill the requested limit, the rest is filled from the whole
dataset, and the relevance filter -- which looks at the image, not its labels --
decides what is kept. That is slower than a label hit (only a few percent of a
random scan passes) and gets no help from the dataset's own boxes, so it is a
fallback: labelled matches always come first, and the images it adds are tagged
matched_by="vlm".
"""

import hashlib
from collections.abc import Collection, Iterable


def scan_order(ids: Iterable, exclude: Collection = ()) -> list:
    """`ids` minus `exclude`, in a fixed pseudo-random order.

    Hashed rather than left in dataset order because datasets are grouped (by
    scene, by sequence, by drive): the first N in file order would be N images of
    one place. Fixed, so a larger limit in a later top-up round is a superset of
    this one and the images already fetched are skipped as duplicates rather than
    replaced.
    """
    skip = set(exclude)
    return sorted((i for i in ids if i not in skip), key=lambda i: hashlib.md5(str(i).encode()).hexdigest())


def scan_candidates(ids: Iterable, exclude: Collection, count: int) -> list:
    """The first `count` unlabelled candidates, or none if `count` is not positive."""
    return scan_order(ids, exclude)[:count] if count > 0 else []
