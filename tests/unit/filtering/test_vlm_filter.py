"""Relevance gate behaviour.

The rules pinned here are the ones whose violation caused real damage: a
verdict that cannot be parsed must not read as acceptance, and an error must
never be reported as a rejection, because a rejection deletes the file.
"""

from app.filtering.vlm_filter import VisualVerifier, _is_affirmative, build_question


class TestIsAffirmative:
    def test_accepts_plain_yes(self):
        assert _is_affirmative("yes") is True
        assert _is_affirmative("Yes") is True
        assert _is_affirmative("YES") is True

    def test_tolerates_punctuation_and_whitespace(self):
        assert _is_affirmative("  Yes. ") is True
        assert _is_affirmative("yes, it does") is True

    def test_rejects_no(self):
        assert _is_affirmative("no") is False
        assert _is_affirmative("No.") is False

    def test_unparseable_answer_is_not_acceptance(self):
        """A model that ignores the instruction must not be read as a yes.

        Defaulting the other way would silently accept whenever the model
        rambled, which is the same class of failure as the old gate scoring
        everything 95%.
        """
        assert _is_affirmative("") is False
        assert _is_affirmative("I cannot tell") is False
        assert _is_affirmative("maybe") is False
        # "not yes" must not match on a bare substring search.
        assert _is_affirmative("not yes") is False


class TestBuildQuestion:
    def test_asks_about_visibility_not_subject_matter(self):
        """'clearly visible' is load-bearing.

        Without it the model answers about what the scene is *about* rather
        than what is in the frame, so a conversation scene from an action film
        counts as a weapon scene.
        """
        q = build_question("a weapon")
        assert "clearly visible" in q

    def test_constrains_the_answer_format(self):
        assert "yes or no" in build_question("a weapon").lower()

    def test_uses_the_query_verbatim(self):
        # Normalisation happens upstream in query_expansion; this must not
        # re-mangle an already-normalised phrase.
        assert "people with red bag" in build_question("people with red bag")


class _StubVerifier(VisualVerifier):
    """A verifier whose per-subject verdicts are scripted, so the any-of logic
    can be tested without loading a model."""

    def __init__(self, script):
        self.script = script  # subject -> {image name: True/False/None}
        self.asked: list[tuple[str, list[str]]] = []

    def verify_batch(self, image_paths, query):
        names = [p.name for p in image_paths]
        self.asked.append((query, names))
        return [self.script[query][n] for n in names]


class TestVerifyBatchAny:
    def _paths(self, *names):
        from pathlib import Path

        return [Path(n) for n in names]

    def test_accepts_when_any_subject_is_visible(self):
        v = _StubVerifier(
            {
                "a car": {"1": True, "2": False, "3": False},
                "a person": {"2": True, "3": False},
            }
        )
        assert v.verify_batch_any(self._paths("1", "2", "3"), ["a car", "a person"]) == [True, True, False]

    def test_later_subjects_are_only_asked_about_unresolved_images(self):
        v = _StubVerifier({"a car": {"1": True, "2": False}, "a person": {"2": False}})
        v.verify_batch_any(self._paths("1", "2"), ["a car", "a person"])
        assert v.asked == [("a car", ["1", "2"]), ("a person", ["2"])]

    def test_undecided_is_never_collapsed_into_rejection(self):
        # A rejection deletes the file, so "could not tell" must stay None.
        v = _StubVerifier({"a car": {"1": None, "2": False}, "a person": {"1": False, "2": False}})
        assert v.verify_batch_any(self._paths("1", "2"), ["a car", "a person"]) == [None, False]

    def test_a_yes_beats_an_earlier_undecided(self):
        v = _StubVerifier({"a car": {"1": None}, "a person": {"1": True}})
        assert v.verify_batch_any(self._paths("1"), ["a car", "a person"]) == [True]

    def test_single_subject_matches_verify_batch(self):
        v = _StubVerifier({"a car": {"1": True, "2": False, "3": None}})
        assert v.verify_batch_any(self._paths("1", "2", "3"), ["a car"]) == [True, False, None]

    def test_empty_input(self):
        assert _StubVerifier({}).verify_batch_any([], ["a car"]) == []
