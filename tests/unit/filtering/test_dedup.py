from app.filtering.dedup import PerceptualDeduplicator


class TestPerceptualDeduplicator:
    def test_identical_hashes_are_grouped_as_duplicates(self):
        deduper = PerceptualDeduplicator(hamming_threshold=5)
        # Same hex hash repeated — hamming distance 0, well within threshold.
        assets = [("a", "ffff0000ffff0000"), ("b", "ffff0000ffff0000"), ("c", "ffff0000ffff0000")]
        groups = deduper.find_duplicate_groups(assets)

        assert len(groups) == 1
        assert groups[0].keep_asset_id == "a"
        assert set(groups[0].duplicate_asset_ids) == {"b", "c"}

    def test_very_different_hashes_are_not_grouped(self):
        deduper = PerceptualDeduplicator(hamming_threshold=5)
        assets = [("a", "0000000000000000"), ("b", "ffffffffffffffff")]
        groups = deduper.find_duplicate_groups(assets)
        assert groups == []

    def test_empty_input_returns_no_groups(self):
        deduper = PerceptualDeduplicator()
        assert deduper.find_duplicate_groups([]) == []

    def test_single_asset_is_never_a_duplicate_of_itself(self):
        deduper = PerceptualDeduplicator()
        groups = deduper.find_duplicate_groups([("a", "ffff0000ffff0000")])
        assert groups == []
