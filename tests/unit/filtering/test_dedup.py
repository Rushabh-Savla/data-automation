from app.filtering.dedup import HybridDeduplicator


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
