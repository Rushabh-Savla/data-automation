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
    name: str | None = Field(default=None, description="Job name — also used to name its NAS storage folder")
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
    nas_storage_path: str | None = None
    config: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_config(self):
        import math
        if not self.name or not self.name.strip():
            self.name = self.query
        if self.config is None:
            self.config = {}
        if self.nas_storage_path:
            self.config["nas_storage_path"] = self.nas_storage_path.strip()
            thresh = self.config.get("relevance_threshold")
            if thresh is None:
                thresh = self.config.get("yolo_world_conf_threshold")

            if thresh is not None:
                try:
                    thresh_float = float(thresh)
                    if math.isnan(thresh_float) or not (0.0 <= thresh_float <= 1.0):
                        raise ValueError("Relevance threshold must be a number between 0.00 and 1.00")
                    self.config["relevance_threshold"] = thresh_float
                except (TypeError, ValueError) as e:
                    raise ValueError("Relevance threshold must be a valid number between 0.00 and 1.00") from e
            else:
                self.config["relevance_threshold"] = 0.40
        return self

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

    @model_validator(mode="after")
    def _validate_config(self):
        import math
        if self.config is not None:
            thresh = self.config.get("relevance_threshold")
            if thresh is None:
                thresh = self.config.get("yolo_world_conf_threshold")

            if thresh is not None:
                try:
                    thresh_float = float(thresh)
                    if math.isnan(thresh_float) or not (0.0 <= thresh_float <= 1.0):
                        raise ValueError("Relevance threshold must be a number between 0.00 and 1.00")
                    self.config["relevance_threshold"] = thresh_float
                except (TypeError, ValueError) as e:
                    raise ValueError("Relevance threshold must be a valid number between 0.00 and 1.00") from e
        return self



class JobSourceTaskResponse(BaseModel):
    id: uuid.UUID
    source_type: SourceType
    status: JobSourceTaskStatus
    discovered_count: int = 0
    downloaded_count: int = 0
    accepted_count: int = 0
    relevance_accepted_count: int = 0
    rejected_count: int = 0
    error_count: int = 0
    error_message: str | None
    last_heartbeat_at: datetime | None
    discovered_unit: str = "images"
    pending_count: int = 0
    total_inventoried: int = 0
    processed_count: int = 0
    completion_percentage: float = 0.0
    waiting_reason: str = ""
    is_active: bool = False
    is_complete: bool = False
    unit_label: str = "images"
    is_reschedulable: bool = False
    status_label: str = ""
    status_explanation: str = ""

    model_config = {"from_attributes": True}

    @model_validator(mode="before")
    @classmethod
    def _extract_source_metrics(cls, data):
        cp = getattr(data, "checkpoint", None) if not isinstance(data, dict) else data.get("checkpoint")
        cp = cp or {}
        pending_items = cp.get("_pending_items") or {}
        completed_items = set(cp.get("_completed_items", []))
        unconsumed = [k for k in pending_items.keys() if k not in completed_items]
        pending_cnt = len(unconsumed)

        source_type = getattr(data, "source_type", None) if not isinstance(data, dict) else data.get("source_type")
        st_val = source_type.value if hasattr(source_type, "value") else str(source_type)

        unit = "images"
        if st_val == "kaggle":
            unit = "datasets"
        elif st_val == "youtube":
            unit = "videos"
        elif st_val == "roboflow":
            unit = "datasets"

        status = getattr(data, "status", None) if not isinstance(data, dict) else data.get("status")
        from app.domain.enums import JOB_SOURCE_TASK_TERMINAL_STATUSES
        is_terminal = status in JOB_SOURCE_TASK_TERMINAL_STATUSES
        is_resched = (pending_cnt > 0) and not is_terminal

        status_val = status.value if hasattr(status, "value") else str(status)
        if status_val == "completed_partial" and pending_cnt > 0:
            s_label = "Partially Processed — More Candidates Available"
            s_expl = f"{pending_cnt} pending {unit} in checkpoint; eligible for next candidate window"
        elif is_resched:
            s_label = "Waiting for Window" if status_val == "window_waiting" else status_val.replace("_", " ").title()
            s_expl = f"{pending_cnt} pending {unit} available to download"
        else:
            s_label = status_val.replace("_", " ").title()
            s_expl = ""

        disc_cnt = getattr(data, "discovered_count", 0) if not isinstance(data, dict) else data.get("discovered_count", 0)
        acc_cnt = getattr(data, "accepted_count", 0) if not isinstance(data, dict) else data.get("accepted_count", 0)
        rej_cnt = getattr(data, "rejected_count", 0) if not isinstance(data, dict) else data.get("rejected_count", 0)
        tot_inv = len(pending_items) if pending_items else disc_cnt
        proc_cnt = len(completed_items) if pending_items else (acc_cnt + rej_cnt)
        comp_pct = round((proc_cnt / tot_inv * 100.0), 1) if tot_inv > 0 else (100.0 if is_terminal else 0.0)

        if isinstance(data, dict):
            data["pending_count"] = pending_cnt
            data["total_inventoried"] = tot_inv
            data["processed_count"] = proc_cnt
            data["completion_percentage"] = comp_pct
            data["unit_label"] = unit
            data["discovered_unit"] = unit
            data["is_reschedulable"] = is_resched
            data["status_label"] = s_label
            data["status_explanation"] = s_expl
        else:
            setattr(data, "pending_count", pending_cnt)
            setattr(data, "total_inventoried", tot_inv)
            setattr(data, "processed_count", proc_cnt)
            setattr(data, "completion_percentage", comp_pct)
            setattr(data, "unit_label", unit)
            setattr(data, "discovered_unit", unit)
            setattr(data, "is_reschedulable", is_resched)
            setattr(data, "status_label", s_label)
            setattr(data, "status_explanation", s_expl)
        return data

    @model_validator(mode="after")
    def _set_discovered_unit(self):
        if self.source_type == SourceType.KAGGLE:
            self.discovered_unit = "datasets"
            self.unit_label = "datasets"
        elif self.source_type == SourceType.YOUTUBE:
            self.discovered_unit = "videos"
            self.unit_label = "videos"
        elif self.source_type == SourceType.ROBOFLOW:
            self.discovered_unit = "datasets"
            self.unit_label = "datasets"
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
    config: dict | None = None
    nas_destination: str | None = None
    current_round: int = 1
    active_source: str | None = None
    active_source_position: str | None = None
    completed_sources: list[str] = Field(default_factory=list)
    next_source_in_queue: str | None = None
    sequential_waiting_reason: str | None = None

    model_config = {"from_attributes": True}

    @model_validator(mode="before")
    @classmethod
    def _extract_topup_round(cls, data):
        """Lifts topup round and authoritative nas_destination into the model."""
        from app.domain.nas_resolver import resolve_job_nas_destination

        config = data.get("config") if isinstance(data, dict) else getattr(data, "config", None)
        round_number = ((config or {}).get("_topup") or {}).get("round")
        resolved_nas = resolve_job_nas_destination(data)

        if isinstance(data, dict):
            data = {**data, "nas_destination": resolved_nas}
            if round_number is not None:
                data["topup_round"] = round_number
        else:
            _sentinel = object()
            data = {
                field: value
                for field in cls.model_fields
                if (value := getattr(data, field, _sentinel)) is not _sentinel
            }
            data["nas_destination"] = resolved_nas
            if round_number is not None:
                data["topup_round"] = round_number
        return data

    @model_validator(mode="after")
    def _set_derived_fields(self):
        self.filtering_in_progress = self.status in _FILTERING_IN_PROGRESS_STATUSES
        from app.domain.enums import JOB_TERMINAL_STATUSES
        if self.status not in JOB_TERMINAL_STATUSES:
            self.completion_reason = None
            self.completed_at = None
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
    current_round: int = 1
    active_source: str | None = None
    active_source_position: str | None = None
    completed_sources: list[str] = Field(default_factory=list)
    next_source_in_queue: str | None = None
    sequential_waiting_reason: str | None = None
    source_tasks: list[JobSourceTaskResponse] = Field(default_factory=list)

    @model_validator(mode="after")
    def _set_filtering_in_progress(self):
        self.filtering_in_progress = self.status in _FILTERING_IN_PROGRESS_STATUSES
        return self
