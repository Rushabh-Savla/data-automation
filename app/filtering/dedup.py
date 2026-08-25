"""HybridDeduplicator — near-duplicate detection combining two signals:
perceptual hashing (cheap, catches near-identical resizes/recompressions)
and CLIP image-embedding cosine similarity ("vector matching" — catches the
same real-world photo re-appearing across sources with a crop, watermark,
filter, or different encoding, which pHash alone often misses). A pair is
flagged duplicate if *either* signal fires.

Added after a real false-negative report: pHash alone was missing genuine
cross-source duplicates. pHash is a DCT-based hash tolerant to resize/
recompression/minor color shifts, but NOT to content-level edits like
cropping or watermarking — exactly the kind of variation that shows up when
the same underlying photo is published by different dataset sources (e.g.
COCO vs Open Images vs Visual Genome may all carry the same Flickr-sourced
image, cropped or re-annotated differently). CLIP embeddings are trained to
recognize the same visual content despite that kind of edit, which is why
they're added as a second, independent signal rather than replacing pHash.

No cross-source/perceptual deduplication existed anywhere in the legacy
scripts (each source only dedupes against itself: kagglehub's cache,
yt-dlp's download_archive, deterministic filenames). This is net-new,
operating across all of a job's assets regardless of source.

Matching is computed via blocked matrix multiplication (see
_find_duplicate_pairs), not a pairwise Python loop — a naive O(n^2) loop
over 50k+ assets takes hours to days (measured; see the scale-fix plan
this was written against) and runs inside a single Celery task with no
per-item checkpointing, so a job that size would never finish. The
underlying comparison is still exactly O(n^2) pairs — that hasn't
changed — but each block of pairs is one numpy/BLAS matrix multiply
instead of millions of individual Python-level comparisons, which is
faster by orders of magnitude for the same exact result (see
_brute_force_reference_for_tests, kept for equivalence testing).
"""

from dataclasses import dataclass


@dataclass
class DuplicateGroup:
    keep_asset_id: str
    duplicate_asset_ids: list[str]


class HybridDeduplicator:
    def __init__(self, hamming_threshold: int = 5, cosine_threshold: float = 0.97, block_size: int = 2000):
        self.hamming_threshold = hamming_threshold
        # CLIP ViT-B/32 cosine similarity bands roughly as: >=0.95-0.99 for
        # the same photo (recompressed/cropped/watermarked), ~0.80-0.90 for
        # different shots of the same scene, ~0.60-0.80 same category but a
        # different subject. A false positive here silently deletes a real,
        # distinct image from the dataset — deliberately conservative
        # default; don't lower below ~0.93 without validating against real
        # job data first.
        self.cosine_threshold = cosine_threshold
        # Bounds memory: a full n x n similarity matrix at 50k assets would
        # be ~10GB (infeasible); a block_size x block_size float32 block is
        # a few MB regardless of n.
        self.block_size = block_size

    def find_duplicate_groups(
        self, assets: list[tuple[str, str | None, list[float] | None]]
    ) -> list[DuplicateGroup]:
        """assets: list of (asset_id, phash_hex | None, embedding | None).
        Returns groups of near-duplicates, each keeping the first-seen asset
        and marking the rest as duplicates of it. A pair is a duplicate if
        EITHER hamming(phash_a, phash_b) <= hamming_threshold OR
        cosine(embed_a, embed_b) >= cosine_threshold; a pair missing both
        signals on either side is never flagged."""
        n = len(assets)
        if n == 0:
            return []

        asset_ids = [a[0] for a in assets]
        pairs = _find_duplicate_pairs(assets, self.hamming_threshold, self.cosine_threshold, self.block_size)

        adjacency: dict[int, list[int]] = {}
        for i, j in pairs:
            adjacency.setdefault(i, []).append(j)

        assigned: set[int] = set()
        groups: list[DuplicateGroup] = []
        for i in range(n):
            if i in assigned:
                continue
            duplicates = [j for j in adjacency.get(i, []) if j not in assigned]
            assigned.update(duplicates)
            if duplicates:
                groups.append(
                    DuplicateGroup(keep_asset_id=asset_ids[i], duplicate_asset_ids=[asset_ids[j] for j in duplicates])
                )
            assigned.add(i)

        return groups


def _find_duplicate_pairs(
    assets: list[tuple[str, str | None, list[float] | None]],
    hamming_threshold: int,
    cosine_threshold: float,
    block_size: int,
) -> list[tuple[int, int]]:
    """Returns every (i, j) with i < j where hamming(phash_i, phash_j) <=
    hamming_threshold OR cosine(embed_i, embed_j) >= cosine_threshold —
    same semantics as a brute-force O(n^2) pairwise scan (see
    _brute_force_reference_for_tests), computed via blocked matrix
    multiplication instead. A pair missing a signal on either side never
    matches on that signal, exactly as in the brute-force version."""
    import imagehash
    import numpy as np

    n = len(assets)

    has_phash = np.zeros(n, dtype=bool)
    phash_bits = np.zeros((n, 64), dtype=np.float32)
    for i, (_, phash_hex, _) in enumerate(assets):
        if phash_hex:
            bits = imagehash.hex_to_hash(phash_hex).hash.flatten().astype(np.float32)
            phash_bits[i] = bits * 2 - 1  # bipolar +-1: hamming = (64 - dot(a, b)) / 2
            has_phash[i] = True

    embed_dim = next((len(emb) for _, _, emb in assets if emb is not None), None)
    has_embedding = np.zeros(n, dtype=bool)
    embed_matrix = None
    if embed_dim is not None:
        embed_matrix = np.zeros((n, embed_dim), dtype=np.float32)
        for i, (_, _, emb) in enumerate(assets):
            if emb is not None:
                embed_matrix[i] = np.asarray(emb, dtype=np.float32)
                has_embedding[i] = True

    pairs: list[tuple[int, int]] = []
    for bi in range(0, n, block_size):
        i_end = min(bi + block_size, n)
        for bj in range(bi, n, block_size):
            j_end = min(bj + block_size, n)

            dots = phash_bits[bi:i_end] @ phash_bits[bj:j_end].T
            hamming = (64.0 - dots) / 2.0
            valid_hash = has_phash[bi:i_end, None] & has_phash[None, bj:j_end]
            hit_block = valid_hash & (hamming <= hamming_threshold)

            if embed_matrix is not None:
                cosine = embed_matrix[bi:i_end] @ embed_matrix[bj:j_end].T
                valid_vec = has_embedding[bi:i_end, None] & has_embedding[None, bj:j_end]
                hit_block = hit_block | (valid_vec & (cosine >= cosine_threshold))

            local_i, local_j = np.nonzero(hit_block)
            for li, lj in zip(local_i.tolist(), local_j.tolist()):
                gi, gj = bi + li, bj + lj
                if gi < gj:
                    pairs.append((gi, gj))

    return pairs


def _brute_force_reference_for_tests(
    assets: list[tuple[str, str | None, list[float] | None]], hamming_threshold: int, cosine_threshold: float
) -> list[tuple[int, int]]:
    """The original O(n^2) pairwise-loop implementation, kept only to prove
    _find_duplicate_pairs' blocked-matmul result is identical, not for any
    production use."""
    import imagehash
    import numpy as np

    parsed = [
        (
            imagehash.hex_to_hash(phash) if phash else None,
            np.asarray(embedding, dtype=np.float32) if embedding else None,
        )
        for _, phash, embedding in assets
    ]
    pairs = []
    for i, (phash, vec) in enumerate(parsed):
        for j in range(i + 1, len(parsed)):
            other_phash, other_vec = parsed[j]
            hash_hit = phash is not None and other_phash is not None and (phash - other_phash) <= hamming_threshold
            vec_hit = vec is not None and other_vec is not None and float(np.dot(vec, other_vec)) >= cosine_threshold
            if hash_hit or vec_hit:
                pairs.append((i, j))
    return pairs
