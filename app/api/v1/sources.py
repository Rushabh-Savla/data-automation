"""Meta endpoint: which sources exist, whether each has a connector implemented
yet, and whether a usable credential is configured — drives the frontend's
source picker."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.security import require_api_key
from app.db.repositories.credential_repository import CredentialRepository
from app.db.repositories.nas_config_repository import NasConfigRepository
from app.core.config import get_settings
from app.domain.enums import SourceType
from app.services.credential_service import _SOURCE_ENV_KEYS
from app.services.job_service import get_implemented_sources

router = APIRouter(prefix="/sources", tags=["sources"], dependencies=[Depends(require_api_key)])


def _has_env_credential(source: SourceType) -> bool:
    """True if the source's key(s) are set via .env / environment (the
    fallback the connectors actually use). Without this the picker only saw
    credentials saved through the Credentials page and showed "no credential"
    even when .env was filled in."""
    settings = get_settings()
    return any(getattr(settings, attr, "") for attr in _SOURCE_ENV_KEYS.get(source, {}).values())


@router.get("")
def list_sources(db: Session = Depends(get_db)):
    cred_repo = CredentialRepository(db)
    implemented = get_implemented_sources()
    result = []
    for source in SourceType:
        has_credential = len(cred_repo.list_active_for_source(source)) > 0 or _has_env_credential(source)
        if source == SourceType.NAS_SHARE:
            # "Credential" for the NAS means a saved connection (configured on the NAS page).
            has_credential = NasConfigRepository(db).get() is not None
        result.append(
            {
                "source_type": source.value,
                "connector_implemented": source in implemented,
                "has_credential": has_credential,
            }
        )
    return result
