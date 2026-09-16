"""Tests for query normalization and class expansion.

The behaviour pinned here is what the previous token-splitting approach got
wrong on a real job (query "weapon on movies scence"): it emitted the medium
words "movies"/"scence" as detection classes, kept the abstract noun "weapon"
that no detector can ground, and handed the raw typo'd string to a VLM as a
yes/no question.
"""

import pytest

from app.filtering.query_expansion import (
    derive_classes,
    negative_prompts,
    normalize_query,
    positive_prompts,
    subject_phrase,
)


class TestSubjectPhrase:
    def test_strips_medium_words(self):
        assert subject_phrase("weapon on movies scence") == "weapon"

    def test_strips_misspelled_medium_words(self):
        # Users type these; a typo must not survive as a detection class.
        assert subject_phrase("gun in movie scence") == "gun"
        assert subject_phrase("gun in movie sence") == "gun"

    def test_preserves_relational_structure(self):
        # "people" and "people with red bag" are different requests — the
        # whole point of the compound query must survive.
        assert subject_phrase("people with red bag") == "people with red bag"
        assert subject_phrase("pedestrian wearing helmet") == "pedestrian wearing helmet"

    def test_trims_dangling_connectives(self):
        # "weapon on <medium>" loses its object once the medium goes; the
        # orphaned "on" must not reach the model.
        assert subject_phrase("weapon on movies") == "weapon"
        assert subject_phrase("a car in a photo") == "car"

    def test_all_context_query_still_returns_something(self):
        assert subject_phrase("movie scene") != ""

    def test_empty_query_does_not_crash(self):
        assert subject_phrase("") == ""
        assert subject_phrase("   ") == ""


class TestNormalizeQuery:
    def test_adds_article_to_singular(self):
        assert normalize_query("weapon on movies scence") == "a weapon"
        assert normalize_query("dog") == "a dog"

    def test_no_article_for_plurals(self):
        # "a people with red bag" is not English.
        assert normalize_query("people with red bag") == "people with red bag"
        assert normalize_query("cars") == "cars"

    def test_words_ending_in_ss_are_not_plurals(self):
        assert normalize_query("glass") == "a glass"

    def test_does_not_double_an_existing_article(self):
        assert normalize_query("a red car") == "a red car"


class TestDeriveClasses:
    def test_expands_superordinates_to_concrete_nouns(self):
        # "weapon" is not visually groundable; "gun"/"pistol" are. This is the
        # fix for YOLO-World scoring 0.0 on all 754 assets of a real job.
        classes = derive_classes("weapon on movies scence")
        assert "gun" in classes
        assert "pistol" in classes
        assert "weapon" not in classes

    def test_drops_medium_words(self):
        classes = derive_classes("weapon on movies scence")
        assert "movies" not in classes
        assert "scence" not in classes

    def test_drops_connectives(self):
        # "wearing" belongs in the VLM question but is a useless detector class.
        assert "wearing" not in derive_classes("pedestrian wearing helmet")

    def test_keeps_multiword_subject_as_its_own_class(self):
        classes = derive_classes("people with red bag")
        assert classes[0] == "people with red bag"

    def test_unmapped_word_passes_through(self):
        assert "helicopter" in derive_classes("helicopter")

    def test_overrides_win_outright(self):
        # An operator who knows their domain should not have to fight the map.
        assert derive_classes("weapon", overrides=["katana"]) == ["katana"]

    def test_class_list_is_capped(self):
        # Detector accuracy degrades as the class list grows.
        assert len(derive_classes("weapon vehicle animal furniture tool fruit")) <= 10


class TestPrompts:
    def test_positive_prompts_are_an_ensemble(self):
        # A single template's absolute score is not a usable threshold, so
        # several phrasings are averaged.
        prompts = positive_prompts("dog")
        assert len(prompts) > 1
        assert all("dog" in p for p in prompts)

    def test_positive_prompts_have_no_duplicates(self):
        prompts = positive_prompts("weapon on movies scence")
        assert len(prompts) == len(set(prompts))

    def test_negative_prompts_cover_video_frame_noise(self):
        # Talking heads and title cards are the bulk of what frame extraction
        # dredges up; the contrast set has to represent them.
        joined = " ".join(negative_prompts())
        assert "text" in joined
        assert "face" in joined or "talking" in joined

    @pytest.mark.parametrize("overrides", [["a photo of a katana"], ["x", "y"]])
    def test_prompt_overrides_win(self, overrides):
        assert positive_prompts("weapon", overrides=overrides) == overrides
        assert negative_prompts(overrides=overrides) == overrides
