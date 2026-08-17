"""Credential CRUD — API keys managed from the UI instead of editing .env."""

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.security import CredentialCipher, require_api_key
from app.db.models.credential import Credential
from app.db.repositories.credential_repository import CredentialRepository
from app.domain.enums import SourceType
from app.schemas.credential import CredentialCreateRequest, CredentialResponse, CredentialUpdateRequest

router = APIRouter(prefix="/credentials", tags=["credentials"], dependencies=[Depends(require_api_key)])


def _mask(value_len_hint: str) -> str:
    return "****" + value_len_hint[-4:] if len(value_len_hint) > 4 else "****"


@router.get("", response_model=list[CredentialResponse])
def list_credentials(source_type: SourceType | None = None, db: Session = Depends(get_db)):
    creds = CredentialRepository(db).list(source_type=source_type)
    return [_to_response(c) for c in creds]


@router.post("", response_model=CredentialResponse, status_code=201)
def create_credential(payload: CredentialCreateRequest, db: Session = Depends(get_db)):
    cipher = CredentialCipher()
    cred = Credential(
        id=uuid.uuid4(),
        source_type=payload.source_type.value,
        label=payload.label,
        key_name=payload.key_name,
        encrypted_value=cipher.encrypt(payload.value),
    )
    CredentialRepository(db).create(cred)
    db.commit()
    db.refresh(cred)
    return _to_response(cred, plaintext_hint=payload.value)


@router.patch("/{credential_id}", response_model=CredentialResponse)
def update_credential(credential_id: uuid.UUID, payload: CredentialUpdateRequest, db: Session = Depends(get_db)):
    repo = CredentialRepository(db)
    cred = repo.get(credential_id)
    if cred is None:
        raise HTTPException(404, f"Credential {credential_id} not found")
    if payload.label is not None:
        cred.label = payload.label
    if payload.is_active is not None:
        cred.is_active = payload.is_active
    plaintext_hint = None
    if payload.value is not None:
        cipher = CredentialCipher()
        cred.encrypted_value = cipher.encrypt(payload.value)
        plaintext_hint = payload.value
    db.commit()
    db.refresh(cred)
    return _to_response(cred, plaintext_hint=plaintext_hint)


@router.delete("/{credential_id}", status_code=204)
def delete_credential(credential_id: uuid.UUID, db: Session = Depends(get_db)):
    repo = CredentialRepository(db)
    if repo.get(credential_id) is None:
        raise HTTPException(404, f"Credential {credential_id} not found")
    repo.delete(credential_id)
    db.commit()


def _to_response(cred: Credential, plaintext_hint: str | None = None) -> CredentialResponse:
    masked = _mask(plaintext_hint) if plaintext_hint else "****"
    return CredentialResponse(
        id=cred.id,
        source_type=cred.source_type,
        label=cred.label,
        key_name=cred.key_name,
        masked_value=masked,
        is_active=cred.is_active,
        created_at=cred.created_at,
        last_used_at=cred.last_used_at,
    )
