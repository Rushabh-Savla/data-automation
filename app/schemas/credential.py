import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from app.domain.enums import SourceType


class CredentialCreateRequest(BaseModel):
    source_type: SourceType
    label: str
    key_name: str = Field(..., description='e.g. "ROBOFLOW_API_KEY", "KAGGLE_KEY"')
    value: str = Field(..., min_length=1)


class CredentialUpdateRequest(BaseModel):
    label: str | None = None
    value: str | None = None
    is_active: bool | None = None


class CredentialResponse(BaseModel):
    id: uuid.UUID
    source_type: SourceType
    label: str
    key_name: str
    masked_value: str
    is_active: bool
    created_at: datetime
    last_used_at: datetime | None

    model_config = {"from_attributes": True}
