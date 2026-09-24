"""NAS connection management — save, read, remove and test the single NAS the
platform stores filtered images on (and can read source images from)."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.connectors.nas.diagnostics import run_connection_test
from app.core.security import require_api_key
from app.db.models.nas_config import NasConfig
from app.schemas.nas import NasConfigInput, NasConfigResponse, NasTestResponse
from app.services.nas_config_service import NasConfigService, NasPasswordRequired

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/nas", tags=["nas"], dependencies=[Depends(require_api_key)])


def _to_response(config: NasConfig | None) -> NasConfigResponse:
    if config is None:
        return NasConfigResponse(configured=False)
    return NasConfigResponse(
        configured=True,
        protocol=config.protocol,
        host=config.host,
        port=config.port,
        share=config.share,
        base_path=config.base_path,
        storage_path=config.storage_path,
        use_as_storage=config.use_as_storage,
        auth_mode=config.auth_mode,
        username=config.username,
        domain=config.domain,
        has_password=bool(config.encrypted_password),
        use_tls=config.use_tls,
        verify_tls=config.verify_tls,
        updated_at=config.updated_at,
    )


@router.get("/config", response_model=NasConfigResponse)
def get_nas_config(db: Session = Depends(get_db)):
    return _to_response(NasConfigService(db).get())


@router.put("/config", response_model=NasConfigResponse)
def save_nas_config(body: NasConfigInput, db: Session = Depends(get_db)):
    try:
        config = NasConfigService(db).save(body)
    except NasPasswordRequired as e:
        raise HTTPException(422, str(e)) from e
    except RuntimeError as e:
        # CredentialCipher raises this when CREDENTIAL_ENCRYPTION_KEY is unset: a
        # server setup problem, reported as such instead of an opaque 500.
        logger.error(f"Cannot save NAS password: {e}")
        raise HTTPException(
            503, "The server has no CREDENTIAL_ENCRYPTION_KEY configured, so a NAS password cannot be stored."
        ) from e
    db.commit()
    return _to_response(config)


@router.delete("/config", status_code=204)
def delete_nas_config(db: Session = Depends(get_db)):
    NasConfigService(db).delete()
    db.commit()
    return Response(status_code=204)


@router.post("/test", response_model=NasTestResponse)
def test_nas_connection(body: NasConfigInput | None = None, db: Session = Depends(get_db)):
    """Tries the given (possibly unsaved) settings, or the saved config when no
    body is sent. Failures are reported in the payload (HTTP 200, ok=false) so the
    form can show them inline; only malformed input is an HTTP error."""
    service = NasConfigService(db)
    try:
        if body is not None:
            params, require_write = service.params_from_input(body), body.use_as_storage
        else:
            saved = service.get()
            if saved is None:
                raise HTTPException(404, "No NAS is configured yet")
            params, require_write = service.params_for(saved), saved.use_as_storage
    except RuntimeError as e:
        logger.error(f"Cannot read stored NAS password: {e}")
        raise HTTPException(
            503, "The stored NAS password could not be decrypted — check CREDENTIAL_ENCRYPTION_KEY."
        ) from e
    outcome = run_connection_test(params, require_write=require_write)
    return NasTestResponse(
        ok=outcome.ok,
        can_read=outcome.can_read,
        can_write=outcome.can_write,
        message=outcome.message,
        latency_ms=outcome.latency_ms,
        sample_files=outcome.sample_files,
    )
