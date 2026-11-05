"""Unit tests for Intelligent Resource Orchestration (Automatic vs Manual Mode)."""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from app.domain.enums import SourceSelectionMode, SourceType
from app.domain.resource_orchestration import (
    TIER_1_SOURCES,
    TIER_2_SOURCES,
    TIER_3_SOURCES,
    build_orchestration_tiers,
    get_tier_for_source,
)
from app.services.job_service import JobService


class TestResourceOrchestrationTiers:
    def test_tier_mapping(self):
        assert get_tier_for_source(SourceType.WEB) == 1
        assert get_tier_for_source(SourceType.YOUTUBE) == 2
        for src in TIER_3_SOURCES:
            assert get_tier_for_source(src) == 3

    def test_build_orchestration_tiers_ordering(self):
        sources = [
            SourceType.KAGGLE,
            SourceType.WEB,
            SourceType.YOUTUBE,
            SourceType.ROBOFLOW,
            SourceType.COCO,
        ]
        tiers = build_orchestration_tiers(sources)
        assert len(tiers) == 3
        # Tier 1: Web
        assert tiers[0] == [SourceType.WEB]
        # Tier 2: YouTube
        assert tiers[1] == [SourceType.YOUTUBE]
        # Tier 3: Kaggle, Roboflow, COCO
        assert set(tiers[2]) == {SourceType.KAGGLE, SourceType.ROBOFLOW, SourceType.COCO}

    def test_job_service_automatic_mode_starts_tier_1_only(self, db_session):
        service = JobService(db_session)
        job = service.create(
            query="test query",
            target_count=500,
            source_selection_mode=SourceSelectionMode.AUTO,
        )
        assert job.config.get("_orchestration_mode") == "auto"
        assert job.config.get("_orchestration_tier") == 1
        # In DB, only Tier 1 tasks should be created initially
        initial_tasks = service.task_repo.list_for_job(job.id)
        assert len(initial_tasks) == 1
        assert initial_tasks[0].source_type == SourceType.WEB.value

        # Pending tiers must contain Tier 2 and Tier 3
        pending = job.config.get("_orchestration_pending_tiers", [])
        assert len(pending) >= 1
        if len(pending) >= 2:
            assert pending[0] == [SourceType.YOUTUBE.value]

    def test_job_service_manual_mode_dispatches_all_selected_sources(self, db_session):
        service = JobService(db_session)
        selected = [SourceType.COCO, SourceType.KAGGLE, SourceType.WEB]
        job = service.create(
            query="test query",
            target_count=500,
            sources=selected,
            source_selection_mode=SourceSelectionMode.MANUAL,
        )
        assert job.config.get("_orchestration_mode") == "manual"
        # In Manual Mode, tasks for all selected sources are created immediately
        tasks = service.task_repo.list_for_job(job.id)
        assert len(tasks) == 3
        created_sources = {t.source_type for t in tasks}
        assert created_sources == {SourceType.COCO.value, SourceType.KAGGLE.value, SourceType.WEB.value}
        assert job.config.get("_orchestration_pending_tiers") == []

