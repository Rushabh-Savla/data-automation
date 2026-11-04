"""Request/response DTOs for the Jobs API. Pydantic doubles as validation and
the OpenAPI schema definition the frontend will codegen a client from."""

import uuid
from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

from app.domain.enums import JobCompletionReason, JobSourceTaskStatus, JobStatus, SourceSelectionMode, SourceType

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
    source_selection_mode: SourceSelectionMode | None = Field(
        default=None,
        description="Source selection strategy: manual, auto, or random.",
    )
    name: str = Field(..., min_length=1, description="Job name — also used to name its NAS storage folder")
    target_count: int | None = Field(default=None, gt=0)
    deadline_at: datetime | None = None
    scheduled_start_at: datetime | None = None
    schedule_cron: str | None = Field(
        default=None,
        description=(
            "Cron expression for a recurring job, e.g. '0 2 * * *' for nightly "
            "at 02:00. A job with this set becomes a template: it never runs "
            "itself, and each firing creates a separate run."
        ),
    )
    schedule_timezone: str = "UTC"
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

    @model_validator(mode="after")
    def _validate_cron(self):
        """Rejects a bad cron at creation time.

        Validating here rather than in the Beat sweep matters: an unparseable
        expression that only fails inside a background task fails silently,
        and the user simply never sees their job run.
        """
        if self.schedule_cron is None:
            return self
        from app.services.schedule_service import InvalidCronError, validate_cron

        try:
            validate_cron(self.schedule_cron, self.schedule_timezone)
        except InvalidCronError as e:
            raise ValueError(str(e)) from e
        return self


class JobUpdateRequest(BaseModel):
    name: str | None = None
    target_count: int | None = Field(default=None, gt=0)
    deadline_at: datetime | None = None
    scheduled_start_at: datetime | None = None
    schedule_cron: str | None = None
    schedule_timezone: str | None = None
    config: dict | None = None

    @model_validator(mode="after")
    def _validate_cron(self):
        if self.schedule_cron is None:
            return self
        from app.services.schedule_service import InvalidCronError, validate_cron

        try:
            validate_cron(self.schedule_cron, self.schedule_timezone or "UTC")
        except InvalidCronError as e:
            raise ValueError(str(e)) from e
        return self


class JobSourceTaskResponse(BaseModel):
    id: uuid.UUID
    source_type: SourceType
    status: JobSourceTaskStatus
    discovered_count: int
    downloaded_count: int
    accepted_count: int
    relevance_accepted_count: int = 0
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
    source_selection_mode: SourceSelectionMode = SourceSelectionMode.MANUAL
    target_count: int | None
    deadline_at: datetime | None
    scheduled_start_at: datetime | None
    status: JobStatus
    completion_reason: JobCompletionReason | None
    discovered_count: int
    downloaded_count: int
    accepted_count: int
    relevance_accepted_count: int = 0
    rejected_count: int
    duplicate_count: int
    review_pending_count: int = 0
    retry_pending_count: int = 0
    upload_pending_count: int = 0
    uploading_count: int = 0
    nas_verified_count: int = 0
    completed_count: int = 0
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    filtering_in_progress: bool = False
    # Which top-up round the job is on, surfaced so the UI can say "collecting
    # more data" instead of leaving the user staring at a static count while a
    # second round runs. Exposed as its own field rather than by returning the
    # whole `config` blob, which is internal orchestration state the client has
    # no business depending on. 1 (or absent) means the original pass.
    topup_round: int | None = None
    schedule_cron: str | None = None
    schedule_timezone: str = "UTC"
    next_run_at: datetime | None = None
    last_run_at: datetime | None = None
    parent_job_id: uuid.UUID | None = None

    model_config = {"from_attributes": True}

    @model_validator(mode="before")
    @classmethod
    def _extract_topup_round(cls, data):
        """Lifts the round number out of the job's internal `_topup` config.

        Done here rather than in the route so every endpoint returning a
        JobResponse gets it consistently, and so `config` itself stays out of
        the API surface. Handles both the ORM object (from_attributes) and a
        plain dict, since both paths construct this model.
        """
        config = data.get("config") if isinstance(data, dict) else getattr(data, "config", None)
        round_number = ((config or {}).get("_topup") or {}).get("round")
        if round_number is None:
            return data
        if isinstance(data, dict):
            data = {**data, "topup_round": round_number}
        else:
            # ORM instance: model_validate reads attributes, so hand Pydantic a
            # dict carrying the extra field alongside the object's own. Only
            # fields the ORM object actually has are copied -- the derived
            # ones (filtering_in_progress, topup_round) have no attribute
            # behind them, and copying them as None would override their
            # defaults and fail validation.
            _sentinel = object()
            data = {
                field: value
                for field in cls.model_fields
                if (value := getattr(data, field, _sentinel)) is not _sentinel
            }
            data["topup_round"] = round_number
        return data

    @model_validator(mode="after")
    def _set_derived_fields(self):
        self.filtering_in_progress = self.status in _FILTERING_IN_PROGRESS_STATUSES
        return self


class JobDetailResponse(JobResponse):
    source_tasks: list[JobSourceTaskResponse] = Field(default_factory=list)


class JobProgressResponse(BaseModel):
    job_id: uuid.UUID
    status: JobStatus
    source_selection_mode: SourceSelectionMode = SourceSelectionMode.MANUAL
    target_count: int | None
    accepted_count: int
    relevance_accepted_count: int = 0
    rejected_count: int
    duplicate_count: int
    discovered_count: int
    downloaded_count: int
    review_pending_count: int = 0
    retry_pending_count: int = 0
    upload_pending_count: int = 0
    uploading_count: int = 0
    nas_verified_count: int = 0
    completed_count: int = 0
    filtering_in_progress: bool = False
    source_tasks: list[JobSourceTaskResponse] = Field(default_factory=list)

    @model_validator(mode="after")
    def _set_filtering_in_progress(self):
        self.filtering_in_progress = self.status in _FILTERING_IN_PROGRESS_STATUSES
        return self
