# Postponed annotation evaluation: this class defines a method literally named
# `list`, which would otherwise rebind the name `list` inside the class
# namespace as soon as that def executes, breaking `list[...]` annotations on
# methods defined afterward in the same class body.
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models.credential import Credential
from app.domain.enums import SourceType


class CredentialRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, credential: Credential) -> Credential:
        self.db.add(credential)
        self.db.flush()
        return credential

    def get(self, credential_id: uuid.UUID) -> Credential | None:
        return self.db.get(Credential, credential_id)

    def list(self, source_type: SourceType | None = None) -> list[Credential]:
        stmt = select(Credential).order_by(Credential.created_at.desc())
        if source_type is not None:
            stmt = stmt.where(Credential.source_type == source_type)
        return list(self.db.execute(stmt).scalars().all())

    def list_active_for_source(self, source_type: SourceType) -> list[Credential]:
        # Least-recently-used first, so a source with several active
        # credentials for the same key_name (e.g. rotating Roboflow API
        # keys) offers the least-recently-used one first instead of
        # whatever arbitrary order Postgres happens to return.
        stmt = (
            select(Credential)
            .where(Credential.source_type == source_type, Credential.is_active.is_(True))
            .order_by(Credential.last_used_at.asc().nullsfirst(), Credential.created_at.asc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def update_value(self, credential_id: uuid.UUID, encrypted_value: bytes) -> None:
        self.db.execute(
            update(Credential).where(Credential.id == credential_id).values(encrypted_value=encrypted_value)
        )

    def mark_used(self, credential_id: uuid.UUID) -> None:
        self.db.execute(
            update(Credential)
            .where(Credential.id == credential_id)
            .values(last_used_at=datetime.now(timezone.utc))
        )

    def delete(self, credential_id: uuid.UUID) -> None:
        credential = self.db.get(Credential, credential_id)
        if credential is not None:
            self.db.delete(credential)
