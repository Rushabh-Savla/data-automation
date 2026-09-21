"""Tests for query_terms — splitting "car and person" into the terms a
fixed-vocabulary connector (nuImages, DAVIS) matches on. Before this, both
sources tested the whole query as one substring and returned zero results for
any multi-word query."""

import pytest

from app.connectors.query_terms import query_terms, singularize


class TestQueryTerms:
    def test_splits_on_connectives(self):
        assert query_terms("car and person") == ["car", "person"]
        assert query_terms("car person") == ["car", "person"]
        assert query_terms("car, person") == ["car", "person"]

    def test_drops_stopwords_and_medium_words(self):
        assert query_terms("a photo of the car") == ["car"]

    def test_singularises_and_deduplicates(self):
        assert query_terms("cars and people") == ["car", "person"]
        assert query_terms("car cars") == ["car"]

    def test_empty_for_all_noise(self):
        assert query_terms("and the") == []


class TestSingularize:
    @pytest.mark.parametrize(
        ("word", "expected"),
        [
            ("cars", "car"),
            ("trucks", "truck"),
            ("buses", "bus"),
            ("horses", "horse"),
            ("houses", "house"),
            ("boxes", "box"),
            ("glasses", "glass"),
            ("people", "person"),
            ("women", "woman"),
        ],
    )
    def test_plurals(self, word, expected):
        assert singularize(word) == expected

    @pytest.mark.parametrize("word", ["bus", "glass", "dress", "debris", "car", "pedestrian"])
    def test_words_that_merely_end_in_s_are_left_alone(self, word):
        assert singularize(word) == word
