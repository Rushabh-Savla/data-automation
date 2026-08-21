"""Tests for StandardizationService._compute_embedding — the CLIP "vector
matching" signal for hybrid dedup. Mocks the CLIP model itself (never
downloads real weights in tests) to exercise _compute_embedding's own
logic: preprocessing wiring, L2-normalization, and graceful failure."""

from pathlib import Path

import torch
from PIL import Image

from app.services import standardization_service as std_mod
from app.services.standardization_service import StandardizationService


class _FakeClipModel:
    def encode_image(self, image_input):
        batch = image_input.shape[0]
        return torch.arange(1, 513, dtype=torch.float32).unsqueeze(0).repeat(batch, 1)


def _fake_load_clip_model():
    return _FakeClipModel(), (lambda img: torch.zeros(3, 2, 2)), "cpu"


class TestComputeEmbedding:
    def test_returns_l2_normalized_512_dim_vector(self, monkeypatch, tmp_path: Path):
        monkeypatch.setattr(std_mod, "_load_clip_model", _fake_load_clip_model)

        img_path = tmp_path / "test.jpg"
        Image.new("RGB", (64, 64)).save(img_path)

        embedding = StandardizationService._compute_embedding(img_path)

        assert embedding is not None
        assert len(embedding) == 512
        norm = sum(v * v for v in embedding) ** 0.5
        assert abs(norm - 1.0) < 1e-4

    def test_swallows_clip_load_failure_to_none(self, monkeypatch, tmp_path: Path):
        def _raise():
            raise RuntimeError("CLIP not available")

        monkeypatch.setattr(std_mod, "_load_clip_model", _raise)

        img_path = tmp_path / "test.jpg"
        Image.new("RGB", (64, 64)).save(img_path)

        assert StandardizationService._compute_embedding(img_path) is None

    def test_swallows_corrupt_image_to_none(self, monkeypatch, tmp_path: Path):
        monkeypatch.setattr(std_mod, "_load_clip_model", _fake_load_clip_model)
        bad_path = tmp_path / "not_an_image.jpg"
        bad_path.write_bytes(b"not a real image")

        assert StandardizationService._compute_embedding(bad_path) is None
