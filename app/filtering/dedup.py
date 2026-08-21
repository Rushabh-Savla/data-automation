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
"""

from dataclasses import dataclass


@dataclass
class DuplicateGroup:
    keep_asset_id: str
    duplicate_asset_ids: list[str]


class HybridDeduplicator:
    def __init__(self, hamming_threshold: int = 5, cosine_threshold: float = 0.97):
        self.hamming_threshold = hamming_threshold
        # CLIP ViT-B/32 cosine similarity bands roughly as: >=0.95-0.99 for
        # the same photo (recompressed/cropped/watermarked), ~0.80-0.90 for
        # different shots of the same scene, ~0.60-0.80 same category but a
        # different subject. A false positive here silently deletes a real,
        # distinct image from the dataset — deliberately conservative
        # default; don't lower below ~0.93 without validating against real
        # job data first.
        self.cosine_threshold = cosine_threshold

    def find_duplicate_groups(
        self, assets: list[tuple[str, str | None, list[float] | None]]
    ) -> list[DuplicateGroup]:
        """assets: list of (asset_id, phash_hex | None, embedding | None).
        Returns groups of near-duplicates, each keeping the first-seen asset
        and marking the rest as duplicates of it. A pair is a duplicate if
        EITHER hamming(phash_a, phash_b) <= hamming_threshold OR
        cosine(embed_a, embed_b) >= cosine_threshold; a pair missing both
        signals on either side is never flagged.

        O(n^2) comparison — fine for a single job's asset count (hundreds to
        low thousands); a BK-tree/ANN index would be the natural upgrade for
        whole-dataset-scale cross-job dedup later.
        """
        import imagehash
        import numpy as np

        parsed = [
            (
                asset_id,
                imagehash.hex_to_hash(phash) if phash else None,
                np.asarray(embedding, dtype=np.float32) if embedding else None,
            )
            for asset_id, phash, embedding in assets
        ]
        assigned: set[str] = set()
        groups: list[DuplicateGroup] = []

        for i, (asset_id, phash, vec) in enumerate(parsed):
            if asset_id in assigned:
                continue
            duplicates = []
            for other_id, other_phash, other_vec in parsed[i + 1 :]:
                if other_id in assigned:
                    continue
                hash_hit = phash is not None and other_phash is not None and (phash - other_phash) <= self.hamming_threshold
                vec_hit = (
                    vec is not None
                    and other_vec is not None
                    and float(np.dot(vec, other_vec)) >= self.cosine_threshold
                )
                if hash_hit or vec_hit:
                    duplicates.append(other_id)
                    assigned.add(other_id)
            if duplicates:
                groups.append(DuplicateGroup(keep_asset_id=asset_id, duplicate_asset_ids=duplicates))
            assigned.add(asset_id)

        return groups
