"""Request/response DTOs for the Jobs API. Pydantic doubles as validation and
the OpenAPI schema definition the frontend will codegen a client from."""

import uuid
from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

from app.domain.enums import JobCompletionReason, JobSourceTaskStatus, JobStatus, SourceType

# Discovery/download stops as soon as target_count is hit, but the overfetch
# buffer (see target_overfetch_buffer in app/core/config.py) means
# accepted_count can legitimately sit above target_count while a job is
# still DISCOVERING/RUNNING — relevance filtering and dedup (which only run
# afterward) are what trim it back down to target_count. This flag tells API
# consumers when that trimming hasn't happened yet, so a temporarily-high
# accepted_count doesn't read as a bug.
_FILTERING_IN_PROGRESS_STATUSES = (JobStatus.DISCOVERING, JobStatus.RUNNING)


class JobCreateRequest(BaseModel):
    query: str = Field(..., min_length=1, description='Free-text query, e.g. "people with red bag"')
    sources: list[SourceType] | None = Field(
        default=None,
        description=(
            "Which connectors to pull from. Omit (or pass an empty list) to "
            "automatically fan out across every currently implemented "
            "connector — the platform default, so a user only has to supply "
            "a query, not pick sources by hand."
        ),
    )
    name: str | None = None
    target_count: int | None = Field(default=None, gt=0)
    deadline_at: datetime | None = None
    scheduled_start_at: datetime | None = None
    config: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def _require_count_or_deadline(self):
        if self.target_count is None and self.deadline_at is None:
            raise ValueError(
                "At least one of target_count or deadline_at must be set, "
                "otherwise the job would run until every source is exhausted "
                "with no bound — set one explicitly if that's intended by "
                "passing an explicit deadline far in the future."
            )
        return self


class JobUpdateRequest(BaseModel):
    name: str | None = None
    target_count: int | None = Field(default=None, gt=0)
    deadline_at: datetime | None = None
    scheduled_start_at: datetime | None = None
    config: dict | None = None


class JobSourceTaskResponse(BaseModel):
    id: uuid.UUID
    source_type: SourceType
    status: JobSourceTaskStatus
    discovered_count: int
    downloaded_count: int
    accepted_count: int
    rejected_count: int
    error_count: int
    error_message: str | None
    last_heartbeat_at: datetime | None
    discovered_unit: str = "images"

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def _set_discovered_unit(self):
        # Kaggle's discovered_count counts datasets, not individual images —
        # downloaded/accepted_count still count images extracted from those
        # datasets, so "discovered: 5, downloaded: 750" is correct, not a
        # mismatch. This tells API consumers what unit discovered_count is in.
        if self.source_type == SourceType.KAGGLE:
            self.discovered_unit = "datasets"
        return self


class JobResponse(BaseModel):
    id: uuid.UUID
    name: str | None
    query: str
    sources: list[str]
    target_count: int | None
    deadline_at: datetime | None
    scheduled_start_at: datetime | None
    status: JobStatus
    completion_reason: JobCompletionReason | None
    discovered_count: int
    downloaded_count: int
    accepted_count: int
    rejected_count: int
    duplicate_count: int
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    filtering_in_progress: bool = False

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def _set_filtering_in_progress(self):
        self.filtering_in_progress = self.status in _FILTERING_IN_PROGRESS_STATUSES
        return self


class JobDetailResponse(JobResponse):
    source_tasks: list[JobSourceTaskResponse] = Field(default_factory=list)


class JobProgressResponse(BaseModel):
    job_id: uuid.UUID
    status: JobStatus
    target_count: int | None
    accepted_count: int
    rejected_count: int
    duplicate_count: int
    discovered_count: int
    downloaded_count: int
    filtering_in_progress: bool = False
    source_tasks: list[JobSourceTaskResponse] = Field(default_factory=list)

    @model_validator(mode="after")
    def _set_filtering_in_progress(self):
        self.filtering_in_progress = self.status in _FILTERING_IN_PROGRESS_STATUSES
        return self
