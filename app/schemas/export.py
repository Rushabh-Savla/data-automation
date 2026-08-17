import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from app.domain.enums import ExportFormat, ExportJobStatus


class ExportCreateRequest(BaseModel):
    format: ExportFormat = ExportFormat.RAW
    filter: dict = Field(default_factory=dict)


class ExportResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    status: ExportJobStatus
    format: ExportFormat
    file_size_bytes: int | None
    asset_count: int | None
    processed_count: int
    requested_at: datetime
    completed_at: datetime | None
    error_message: str | None

    model_config = {"from_attributes": True}
