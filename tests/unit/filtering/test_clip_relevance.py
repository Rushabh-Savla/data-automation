"""Tests for the CLIP relevance band logic.

score_embeddings itself needs the CLIP weights, so these cover the pure
decision boundary — `classify` — plus score_embeddings' behaviour on inputs
that must not require a model at all.
"""

from app.filtering.clip_relevance import classify, score_embeddings


class TestClassify:
    def test_at_or_above_accept_is_accepted(self):
        assert classify(0.80, accept_at=0.75, reject_at=0.45) == "accept"
        assert classify(0.75, accept_at=0.75, reject_at=0.45) == "accept"

    def test_at_or_below_reject_is_rejected(self):
        assert classify(0.20, accept_at=0.75, reject_at=0.45) == "reject"
        assert classify(0.45, accept_at=0.75, reject_at=0.45) == "reject"

    def test_middle_band_is_uncertain(self):
        assert classify(0.60, accept_at=0.75, reject_at=0.45) == "uncertain"

    def test_missing_score_is_uncertain_never_reject(self):
        """A None score must escalate, not reject.

        Rejection deletes the file. "No score" is an absence of evidence —
        treating it as evidence of irrelevance is how good images get
        destroyed by an infrastructure failure.
        """
        assert classify(None, accept_at=0.75, reject_at=0.45) == "uncertain"

    def test_degenerate_thresholds_never_produce_a_silent_reject(self):
        # If the two bands are configured to overlap, accept wins — the
        # conservative direction, since the other branch deletes data.
        assert classify(0.5, accept_at=0.4, reject_at=0.6) == "accept"


class TestScoreEmbeddings:
    def test_no_positive_prompts_defers_everything(self):
        # Must not require loading CLIP to answer this.
        assert score_embeddings([[0.1, 0.2]], [], ["neg"]) == [None]

    def test_empty_input_returns_empty(self):
        assert score_embeddings([], ["a photo of a dog"], ["neg"]) == []

    def test_all_missing_embeddings_short_circuits(self):
        # No embeddings present means no model call is needed at all.
        assert score_embeddings([None, None], ["a photo of a dog"], ["neg"]) == [None, None]
