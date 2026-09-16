"""Relevance gate behaviour.

The rules pinned here are the ones whose violation caused real damage: a
verdict that cannot be parsed must not read as acceptance, and an error must
never be reported as a rejection, because a rejection deletes the file.
"""

from app.filtering.vlm_filter import _is_affirmative, build_question


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
