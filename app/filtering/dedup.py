"""PerceptualDeduplicator — near-duplicate detection via perceptual hashing.

No cross-source/perceptual deduplication existed anywhere in the legacy
scripts (each source only dedupes against itself: kagglehub's cache,
yt-dlp's download_archive, deterministic filenames). This is net-new,
operating across all of a job's assets regardless of source.
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DuplicateGroup:
    keep_asset_id: str
    duplicate_asset_ids: list[str]


class PerceptualDeduplicator:
    def __init__(self, hamming_threshold: int = 5):
        self.hamming_threshold = hamming_threshold

    @staticmethod
    def compute_phash(image_path: Path) -> str:
        import imagehash
        from PIL import Image

        with Image.open(image_path) as img:
            return str(imagehash.phash(img))

    def find_duplicate_groups(self, assets: list[tuple[str, str]]) -> list[DuplicateGroup]:
        """assets: list of (asset_id, phash_hex). Returns groups of
        near-duplicates (hamming distance <= threshold), each keeping the
        first-seen asset and marking the rest as duplicates of it.

        O(n^2) hamming comparison — fine for a single job's asset count
        (hundreds to low thousands); a BK-tree/LSH index would be the
        natural upgrade for whole-dataset-scale cross-job dedup later.
        """
        import imagehash

        parsed = [(asset_id, imagehash.hex_to_hash(h)) for asset_id, h in assets]
        assigned: set[str] = set()
        groups: list[DuplicateGroup] = []

        for i, (asset_id, phash) in enumerate(parsed):
            if asset_id in assigned:
                continue
            duplicates = []
            for other_id, other_phash in parsed[i + 1 :]:
                if other_id in assigned:
                    continue
                if phash - other_phash <= self.hamming_threshold:
                    duplicates.append(other_id)
                    assigned.add(other_id)
            if duplicates:
                groups.append(DuplicateGroup(keep_asset_id=asset_id, duplicate_asset_ids=duplicates))
            assigned.add(asset_id)

        return groups
