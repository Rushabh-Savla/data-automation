"""Comprehensive unit tests for Meta SAM 3.1 image filtering and confidence-based relevance filtering.

Tests include:
  1. Exact threshold table from requirements (0.15, 0.25, 0.39, 0.40, 0.41, 0.65, 0.89, 0.98).
  2. Inclusive comparison (score >= threshold).
  3. Multi-instance scoring: accept if ANY instance passes; reject if none pass.
  4. Zero detections: classified as REJECT with score 0.0.
  5. Boundary thresholds: 0.00 and 1.00.
  6. Non-finite score handling (NaN / inf).
  7. Concept prompt cleaning and multi-class query handling (AND / OR).
  8. Redis caching of numeric scores.
  9. Real model inference verification on CUDA with SAM 3.1 checkpoint.
"""

import math
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import torch
from PIL import Image, ImageDraw

from app.filtering.sam3_filter import (
    DEFAULT_THRESHOLD,
    OUTCOME_ACCEPT,
    OUTCOME_ERROR,
    OUTCOME_REJECT,
    Sam3Verifier,
    clean_concept_prompt,
    compute_sam3_cache_key,
    find_sam3_checkpoint,
)


class TestConceptPromptCleaning:
    def test_strips_leading_articles(self):
        assert clean_concept_prompt("a red sports car") == "red sports car"
        assert clean_concept_prompt("an apple on a table") == "apple on table"
        assert clean_concept_prompt("the golden retriever") == "golden retriever"

    def test_preserves_core_concept(self):
        assert clean_concept_prompt("person with red backpack") == "person with red backpack"
        assert clean_concept_prompt("dog") == "dog"

    def test_handles_punctuation_and_whitespace(self):
        assert clean_concept_prompt("  a white cat.  ") == "white cat"
        assert clean_concept_prompt('"excavator"') == "excavator"


class TestRelevanceThresholdLogic:
    """Verifies the exact threshold rules and table specified in project requirements."""

    @pytest.fixture
    def mock_verifier(self):
        """Creates a Sam3Verifier with mocked model/processor for deterministic unit testing."""
        with patch("app.filtering.sam3_filter._load_sam3") as mock_load:
            mock_proc = MagicMock()
            mock_model = MagicMock()
            mock_load.return_value = (mock_proc, mock_model, "cpu")
            verifier = Sam3Verifier(default_threshold=0.40)
            yield verifier, mock_proc

    @pytest.mark.parametrize(
        "score, expected_outcome",
        [
            (0.15, OUTCOME_REJECT),
            (0.25, OUTCOME_REJECT),
            (0.39, OUTCOME_REJECT),
            (0.40, OUTCOME_ACCEPT),
            (0.41, OUTCOME_ACCEPT),
            (0.65, OUTCOME_ACCEPT),
            (0.89, OUTCOME_ACCEPT),
            (0.98, OUTCOME_ACCEPT),
        ],
    )
    def test_exact_threshold_table(self, mock_verifier, score, expected_outcome):
        """Verifies:
        - Score 0.15: REJECT
        - Score 0.25: REJECT
        - Score 0.39: REJECT
        - Score 0.40: ACCEPT
        - Score 0.41: ACCEPT
        - Score 0.65: ACCEPT
        - Score 0.89: ACCEPT
        - Score 0.98: ACCEPT
        """
        verifier, mock_proc = mock_verifier
        img = Image.new("RGB", (64, 64))

        # Mock infer_concept to return the exact score
        with patch.object(
            verifier,
            "infer_concept",
            return_value={
                "scores": np.array([score], dtype=np.float32),
                "boxes": torch.tensor([[10, 10, 50, 50]]),
                "masks": torch.ones((1, 1, 64, 64), dtype=torch.bool),
                "max_score": score,
            },
        ):
            outcome, reported_score = verifier.evaluate_asset(img, "test_object", threshold=0.40)
            assert outcome == expected_outcome
            assert reported_score == pytest.approx(score, abs=1e-5)

    def test_multi_instance_one_passing_instance_accepts_image(self, mock_verifier):
        """If multiple instances exist and at least one passes threshold, ACCEPT."""
        verifier, _ = mock_verifier
        img = Image.new("RGB", (64, 64))

        # Detections with scores [0.12, 0.45, 0.28] -> 0.45 passes threshold 0.40
        scores = np.array([0.12, 0.45, 0.28], dtype=np.float32)
        with patch.object(
            verifier,
            "infer_concept",
            return_value={
                "scores": scores,
                "boxes": torch.zeros((3, 4)),
                "masks": torch.zeros((3, 1, 64, 64)),
                "max_score": 0.45,
            },
        ):
            outcome, winning_score = verifier.evaluate_asset(img, "dog", threshold=0.40)
            assert outcome == OUTCOME_ACCEPT
            assert winning_score == pytest.approx(0.45, abs=1e-5)

    def test_multi_instance_none_passing_rejects_image(self, mock_verifier):
        """If multiple instances exist but none reaches threshold, REJECT."""
        verifier, _ = mock_verifier
        img = Image.new("RGB", (64, 64))

        # Detections with scores [0.15, 0.38, 0.22] -> all below 0.40
        scores = np.array([0.15, 0.38, 0.22], dtype=np.float32)
        with patch.object(
            verifier,
            "infer_concept",
            return_value={
                "scores": scores,
                "boxes": torch.zeros((3, 4)),
                "masks": torch.zeros((3, 1, 64, 64)),
                "max_score": 0.38,
            },
        ):
            outcome, winning_score = verifier.evaluate_asset(img, "dog", threshold=0.40)
            assert outcome == OUTCOME_REJECT
            assert winning_score == pytest.approx(0.38, abs=1e-5)

    def test_zero_detections_rejects_with_zero_score(self, mock_verifier):
        """Image with 0 detections is REJECT with score 0.0 (not an error)."""
        verifier, _ = mock_verifier
        img = Image.new("RGB", (64, 64))

        with patch.object(
            verifier,
            "infer_concept",
            return_value={
                "scores": np.array([], dtype=np.float32),
                "boxes": None,
                "masks": None,
                "max_score": 0.0,
            },
        ):
            outcome, score = verifier.evaluate_asset(img, "truck", threshold=0.40)
            assert outcome == OUTCOME_REJECT
            assert score == 0.0

    def test_boundary_threshold_zero_accepts_any_detection(self, mock_verifier):
        """Threshold 0.00 accepts any detection >= 0.00."""
        verifier, _ = mock_verifier
        img = Image.new("RGB", (64, 64))

        with patch.object(
            verifier,
            "infer_concept",
            return_value={
                "scores": np.array([0.01], dtype=np.float32),
                "boxes": None,
                "masks": None,
                "max_score": 0.01,
            },
        ):
            outcome, score = verifier.evaluate_asset(img, "car", threshold=0.00)
            assert outcome == OUTCOME_ACCEPT
            assert score == pytest.approx(0.01)

    def test_boundary_threshold_one_strict(self, mock_verifier):
        """Threshold 1.00 rejects 0.99 and accepts 1.00."""
        verifier, _ = mock_verifier
        img = Image.new("RGB", (64, 64))

        with patch.object(
            verifier,
            "infer_concept",
            return_value={
                "scores": np.array([0.99], dtype=np.float32),
                "boxes": None,
                "masks": None,
                "max_score": 0.99,
            },
        ):
            outcome, _ = verifier.evaluate_asset(img, "car", threshold=1.00)
            assert outcome == OUTCOME_REJECT

        with patch.object(
            verifier,
            "infer_concept",
            return_value={
                "scores": np.array([1.00], dtype=np.float32),
                "boxes": None,
                "masks": None,
                "max_score": 1.00,
            },
        ):
            outcome, score = verifier.evaluate_asset(img, "car", threshold=1.00)
            assert outcome == OUTCOME_ACCEPT
            assert score == pytest.approx(1.00)

    def test_nan_and_inf_scores_filtered_safely(self, mock_verifier):
        """Non-finite scores (NaN, inf) must be ignored and not crash inference."""
        verifier, mock_proc = mock_verifier
        mock_proc.set_image.return_value = {"backbone_out": {}}
        mock_proc.set_text_prompt.return_value = {
            "scores": torch.tensor([float("nan"), 0.55, float("inf")]),
            "boxes": torch.zeros((3, 4)),
            "masks": torch.zeros((3, 1, 64, 64)),
        }
        res = verifier.infer_concept(Image.new("RGB", (64, 64)), "cat")
        assert len(res["scores"]) == 1
        assert res["scores"][0] == pytest.approx(0.55)
        assert res["max_score"] == pytest.approx(0.55)


class TestComplexQueriesAndMultiClass:
    @pytest.fixture
    def mock_verifier(self):
        with patch("app.filtering.sam3_filter._load_sam3") as mock_load:
            mock_proc = MagicMock()
            mock_model = MagicMock()
            mock_load.return_value = (mock_proc, mock_model, "cpu")
            verifier = Sam3Verifier(default_threshold=0.40)
            yield verifier

    def test_and_query_requires_both_concepts(self, mock_verifier):
        verifier = mock_verifier
        img = Image.new("RGB", (64, 64))

        # Case 1: "dog" passes (0.80), "cat" fails (0.20) -> REJECT
        def mock_infer(state, concept):
            if "dog" in concept:
                return {"scores": np.array([0.80]), "max_score": 0.80}
            return {"scores": np.array([0.20]), "max_score": 0.20}

        with patch.object(verifier, "infer_concept", side_effect=mock_infer):
            outcome, score = verifier.evaluate_asset(img, "dog AND cat", threshold=0.40)
            assert outcome == OUTCOME_REJECT
            assert score == pytest.approx(0.20)

        # Case 2: both pass (0.80, 0.60) -> ACCEPT with winning score min = 0.60
        def mock_infer_both_pass(state, concept):
            if "dog" in concept:
                return {"scores": np.array([0.80]), "max_score": 0.80}
            return {"scores": np.array([0.60]), "max_score": 0.60}

        with patch.object(verifier, "infer_concept", side_effect=mock_infer_both_pass):
            outcome, score = verifier.evaluate_asset(img, "dog AND cat", threshold=0.40)
            assert outcome == OUTCOME_ACCEPT
            assert score == pytest.approx(0.60)

    def test_or_query_accepts_if_any_concept_passes(self, mock_verifier):
        verifier = mock_verifier
        img = Image.new("RGB", (64, 64))

        def mock_infer(state, concept):
            if "car" in concept:
                return {"scores": np.array([0.75]), "max_score": 0.75}
            return {"scores": np.array([0.10]), "max_score": 0.10}

        with patch.object(verifier, "infer_concept", side_effect=mock_infer):
            outcome, score = verifier.evaluate_asset(img, "car OR motorcycle", threshold=0.40)
            assert outcome == OUTCOME_ACCEPT
            assert score == pytest.approx(0.75)


class TestRedisCaching:
    @pytest.fixture
    def mock_verifier(self):
        with patch("app.filtering.sam3_filter._load_sam3") as mock_load:
            mock_proc = MagicMock()
            mock_model = MagicMock()
            mock_load.return_value = (mock_proc, mock_model, "cpu")
            verifier = Sam3Verifier(default_threshold=0.40)
            yield verifier

    def test_retrieves_score_from_cache(self, mock_verifier):
        verifier = mock_verifier
        img = Image.new("RGB", (64, 64))

        # Mock Redis cache returning 0.65
        with patch.object(verifier, "_get_cached_score", return_value=0.65):
            outcome, score = verifier.evaluate_asset(
                img, "person", threshold=0.40, fingerprint="sha256_mock"
            )
            assert outcome == OUTCOME_ACCEPT
            assert score == 0.65

            # Same cached score with higher threshold (0.70) becomes REJECT without re-inference!
            outcome_strict, score_strict = verifier.evaluate_asset(
                img, "person", threshold=0.70, fingerprint="sha256_mock"
            )
            assert outcome_strict == OUTCOME_REJECT
            assert score_strict == 0.65


class TestRealSam3ModelInference:
    """Verifies genuine Meta SAM 3.1 model inference on CUDA using actual pretrained checkpoint."""

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="Requires CUDA GPU")
    def test_real_sam31_inference_on_shapes(self):
        ckpt = find_sam3_checkpoint()
        assert ckpt is not None, "SAM 3.1 checkpoint not found"

        verifier = Sam3Verifier()
        assert verifier._device == "cuda"

        # Create a synthetic image with a clear red circle
        img = Image.new("RGB", (320, 240), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        draw.ellipse([50, 50, 150, 150], fill=(255, 0, 0))

        # Query 1: "red circle" should be detected with high confidence
        res_circle = verifier.infer_concept(img, "red circle")
        assert len(res_circle["scores"]) > 0
        assert res_circle["max_score"] >= 0.40, f"Expected red circle score >= 0.40, got {res_circle['max_score']}"

        # Evaluation at default threshold 0.40 -> ACCEPT
        outcome, score = verifier.evaluate_asset(img, "red circle", threshold=0.40)
        assert outcome == OUTCOME_ACCEPT
        assert score >= 0.40

        # Query 2: "green truck" should NOT be detected in a red circle image
        outcome_truck, score_truck = verifier.evaluate_asset(img, "green truck", threshold=0.40)
        assert outcome_truck == OUTCOME_REJECT
        assert score_truck < 0.40

