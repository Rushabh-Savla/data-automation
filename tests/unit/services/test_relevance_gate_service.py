"""Unit tests for early visual relevance admission gate service."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from PIL import Image
import pytest

from app.services.relevance_gate_service import evaluate_transient_candidate


class TestRelevanceGateService:
    @pytest.fixture
    def sample_image(self, tmp_path):
        img_path = tmp_path / "test_candidate.jpg"
        img = Image.new("RGB", (320, 240), color=(255, 0, 0))
        img.save(img_path)
        return img_path

    def test_missing_image_returns_reject(self, tmp_path):
        missing_path = tmp_path / "non_existent.jpg"
        outcome, score = evaluate_transient_candidate(missing_path, query="car")
        assert outcome == "reject"
        assert score is None

    def test_corrupt_image_returns_reject(self, tmp_path):
        corrupt_path = tmp_path / "corrupt.jpg"
        corrupt_path.write_bytes(b"not a real image")
        outcome, score = evaluate_transient_candidate(corrupt_path, query="car")
        assert outcome == "reject"
        assert score is None

    def test_below_min_resolution_returns_reject(self, sample_image):
        # sample_image is 320x240, test min resolution 400x400
        outcome, score = evaluate_transient_candidate(
            sample_image, query="car", min_resolution=(400, 400)
        )
        assert outcome == "reject"
        assert score is None

    def test_cached_score_returns_immediately(self, sample_image):
        fake_redis = MagicMock()
        fake_redis.get.return_value = "0.85"

        with patch("app.services.relevance_gate_service.get_redis", return_value=fake_redis):
            outcome, score = evaluate_transient_candidate(
                sample_image, query="car", threshold=0.40, fingerprint="sha256_dummy"
            )
            assert outcome == "accept"
            assert score == 0.85

    def test_cached_score_below_threshold_rejects(self, sample_image):
        fake_redis = MagicMock()
        fake_redis.get.return_value = "0.20"

        with patch("app.services.relevance_gate_service.get_redis", return_value=fake_redis):
            outcome, score = evaluate_transient_candidate(
                sample_image, query="car", threshold=0.40, fingerprint="sha256_dummy"
            )
            assert outcome == "reject"
            assert score == 0.20

    def test_worker_gpu_dispatch_success(self, sample_image):
        mock_task = MagicMock()
        mock_async_res = MagicMock()
        mock_async_res.get.return_value = ("accept", 0.92)
        mock_task.apply_async.return_value = mock_async_res

        with patch("app.services.relevance_gate_service.get_redis", return_value=None), \
             patch("app.tasks.filtering_tasks.evaluate_transient_candidate_task", mock_task):
            outcome, score = evaluate_transient_candidate(
                sample_image, query="sports car", threshold=0.40
            )
            assert outcome == "accept"
            assert score == 0.92
            mock_task.apply_async.assert_called_once()

    def test_worker_gpu_dispatch_failure_returns_error(self, sample_image):
        mock_task = MagicMock()
        mock_task.apply_async.side_effect = TimeoutError("GPU worker timed out")

        with patch("app.services.relevance_gate_service.get_redis", return_value=None), \
             patch("app.tasks.filtering_tasks.evaluate_transient_candidate_task", mock_task):
            outcome, score = evaluate_transient_candidate(
                sample_image, query="sports car", threshold=0.40
            )
            assert outcome == "error"
            assert score is None

