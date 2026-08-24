from app.filtering.smolvlm_filter import _is_affirmative


class TestIsAffirmative:
    def test_plain_yes(self):
        assert _is_affirmative("yes") is True

    def test_capitalized_with_punctuation(self):
        assert _is_affirmative("Yes.") is True

    def test_yes_with_trailing_explanation(self):
        assert _is_affirmative("yes, it does show a dog") is True

    def test_plain_no(self):
        assert _is_affirmative("no") is False

    def test_unrelated_response_is_not_affirmative(self):
        assert _is_affirmative("it's a cat") is False

    def test_empty_response_is_not_affirmative(self):
        assert _is_affirmative("") is False
