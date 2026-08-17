"""Meta endpoint: which sources exist, whether each has a connector implemented
yet, and whether a usable credential is configured — drives the frontend's
source picker."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.security import require_api_key
from app.db.repositories.credential_repository import CredentialRepository
from app.domain.enums import SourceType
from app.services.job_service import get_implemented_sources

router = APIRouter(prefix="/sources", tags=["sources"], dependencies=[Depends(require_api_key)])


@router.get("")
def list_sources(db: Session = Depends(get_db)):
    cred_repo = CredentialRepository(db)
    implemented = get_implemented_sources()
    result = []
    for source in SourceType:
        has_credential = len(cred_repo.list_active_for_source(source)) > 0
        result.append(
            {
                "source_type": source.value,
                "connector_implemented": source in implemented,
                "has_credential": has_credential,
            }
        )
    return result
