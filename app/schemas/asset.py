import uuid
from datetime import datetime

from pydantic import BaseModel

from app.domain.enums import AnnotationFormat, AssetStatus, SourceType


class AssetResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    source_type: SourceType
    source_item_id: str
    source_url: str | None
    dataset_identifier: str | None
    license: str | None
    original_name: str | None
    width: int | None
    height: int | None
    file_format: str | None
    nas_path: str | None
    annotation_format: AnnotationFormat
    classes: list
    status: AssetStatus
    relevance_score: float | None
    discovered_at: datetime
    promoted_at: datetime | None

    model_config = {"from_attributes": True}
