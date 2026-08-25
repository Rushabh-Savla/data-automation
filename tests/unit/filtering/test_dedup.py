import random

from app.filtering.dedup import HybridDeduplicator, _brute_force_reference_for_tests, _find_duplicate_pairs


class TestHybridDeduplicator:
    def test_identical_hashes_are_grouped_as_duplicates(self):
        deduper = HybridDeduplicator(hamming_threshold=5)
        # Same hex hash repeated — hamming distance 0, well within threshold.
        assets = [
            ("a", "ffff0000ffff0000", None),
            ("b", "ffff0000ffff0000", None),
            ("c", "ffff0000ffff0000", None),
        ]
        groups = deduper.find_duplicate_groups(assets)

        assert len(groups) == 1
        assert groups[0].keep_asset_id == "a"
        assert set(groups[0].duplicate_asset_ids) == {"b", "c"}

    def test_very_different_hashes_are_not_grouped(self):
        deduper = HybridDeduplicator(hamming_threshold=5)
        assets = [("a", "0000000000000000", None), ("b", "ffffffffffffffff", None)]
        groups = deduper.find_duplicate_groups(assets)
        assert groups == []

    def test_empty_input_returns_no_groups(self):
        deduper = HybridDeduplicator()
        assert deduper.find_duplicate_groups([]) == []

    def test_single_asset_is_never_a_duplicate_of_itself(self):
        deduper = HybridDeduplicator()
        groups = deduper.find_duplicate_groups([("a", "ffff0000ffff0000", None)])
        assert groups == []

    def test_identical_embeddings_are_grouped_even_with_different_hashes(self):
        deduper = HybridDeduplicator(hamming_threshold=5, cosine_threshold=0.97)
        vec = [1.0, 0.0, 0.0]
        # Hamming distance between these two phashes is large (well above
        # threshold) — only the vector signal should fire here.
        assets = [
            ("a", "0000000000000000", vec),
            ("b", "ffffffffffffffff", vec),
        ]
        groups = deduper.find_duplicate_groups(assets)
        assert len(groups) == 1
        assert groups[0].duplicate_asset_ids == ["b"]

    def test_dissimilar_embeddings_below_threshold_are_not_grouped(self):
        deduper = HybridDeduplicator(hamming_threshold=5, cosine_threshold=0.97)
        assets = [
            ("a", None, [1.0, 0.0, 0.0]),
            ("b", None, [0.0, 1.0, 0.0]),
        ]
        groups = deduper.find_duplicate_groups(assets)
        assert groups == []

    def test_missing_embedding_still_dedups_via_phash_alone(self):
        deduper = HybridDeduplicator(hamming_threshold=5)
        assets = [
            ("a", "ffff0000ffff0000", None),
            ("b", "ffff0000ffff0000", [1.0, 0.0, 0.0]),
        ]
        groups = deduper.find_duplicate_groups(assets)
        assert len(groups) == 1
        assert groups[0].duplicate_asset_ids == ["b"]


def _random_hex_hash(rng: random.Random) -> str:
    return f"{rng.getrandbits(64):016x}"


def _random_unit_vector(rng: random.Random, dim: int) -> list[float]:
    import math

    v = [rng.gauss(0, 1) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / norm for x in v]


class TestFindDuplicatePairsMatchesBruteForce:
    """Proves the blocked-matmul candidate generator (_find_duplicate_pairs,
    used in production) returns exactly the same pairs as the original
    O(n^2) pairwise loop (_brute_force_reference_for_tests, kept only for
    this comparison) — the optimization changed how the comparison is
    computed, not what it computes."""

    def _assert_matches_brute_force(self, assets, hamming_threshold=5, cosine_threshold=0.97, block_size=2000):
        fast = set(_find_duplicate_pairs(assets, hamming_threshold, cosine_threshold, block_size))
        reference = set(_brute_force_reference_for_tests(assets, hamming_threshold, cosine_threshold))
        assert fast == reference

    def test_mixed_random_dataset_small_blocks(self):
        # block_size smaller than n forces multiple block-pairs, including
        # the diagonal (self-comparison) case, to actually exercise the
        # blocking logic rather than degenerating to one block.
        rng = random.Random(42)
        assets = []
        for i in range(37):
            has_phash = rng.random() < 0.8
            has_embedding = rng.random() < 0.8
            phash = _random_hex_hash(rng) if has_phash else None
            embedding = _random_unit_vector(rng, 8) if has_embedding else None
            assets.append((f"asset-{i}", phash, embedding))
        # Force some genuine near-duplicates so both signals get real hits,
        # not just random-chance misses.
        assets[5] = ("asset-5", assets[2][1], assets[2][2])
        assets[10] = ("asset-10", None, assets[3][2])
        self._assert_matches_brute_force(assets, block_size=10)

    def test_all_identical_forces_dense_matches(self):
        rng = random.Random(7)
        phash = _random_hex_hash(rng)
        vec = _random_unit_vector(rng, 8)
        assets = [(f"asset-{i}", phash, vec) for i in range(15)]
        self._assert_matches_brute_force(assets, block_size=4)

    def test_no_signals_at_all_yields_no_pairs(self):
        assets = [(f"asset-{i}", None, None) for i in range(10)]
        self._assert_matches_brute_force(assets, block_size=3)

    def test_empty_and_single_asset(self):
        self._assert_matches_brute_force([])
        self._assert_matches_brute_force([("a", "ffff0000ffff0000", [1.0, 0.0])])
