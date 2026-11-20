"""Unit tests for NAS destination resolver and sanitization."""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from app.domain.nas_resolver import (
    DEFAULT_NAS_DESTINATION,
    resolve_job_nas_destination,
    sanitize_nas_path,
)


class TestNasResolver:
    def test_sanitize_nas_path_clean(self):
        assert sanitize_nas_path("datasets/custom_car") == "datasets/custom_car"
        assert sanitize_nas_path("/datasets/custom_car/") == "datasets/custom_car"
        assert sanitize_nas_path("  datasets/custom_car  ") == "datasets/custom_car"
        assert sanitize_nas_path(r"datasets\custom_car") == "datasets/custom_car"

    def test_sanitize_nas_path_traversal(self):
        # Traversal '..' should be stripped
        assert sanitize_nas_path("../../datasets/secret") == "datasets/secret"
        assert sanitize_nas_path("../../../") is None
        assert sanitize_nas_path("") is None
        assert sanitize_nas_path(None) is None

    def test_resolve_job_nas_destination_from_job_config(self):
        job = MagicMock()
        job.config = {"nas_storage_path": "user_datasets/project_a"}
        resolved = resolve_job_nas_destination(job)
        assert resolved == "user_datasets/project_a"

    def test_resolve_job_nas_destination_from_dict(self):
        config = {"nas_storage_path": "/user_datasets/project_b/"}
        resolved = resolve_job_nas_destination(config)
        assert resolved == "user_datasets/project_b"

    def test_resolve_job_nas_destination_fallback_to_default_storage(self):
        job = MagicMock()
        job.config = {}
        resolved = resolve_job_nas_destination(job, default_storage="custom_fallback")
        assert resolved == "custom_fallback"

    def test_resolve_job_nas_destination_none_when_unspecified(self):
        job = MagicMock()
        job.config = {}
        # When no nas_storage_path is specified, should return None (local storage only, no automatic fallback)
        resolved = resolve_job_nas_destination(job)
        assert resolved is None

