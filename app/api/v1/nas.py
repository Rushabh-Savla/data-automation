"""NAS connection management — save, read, remove and test the single NAS the
platform stores filtered images on (and can read source images from)."""

import logging
import time
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.connectors.nas.base import (
    IMAGE_EXTENSIONS,
    join_relpath,
    normalize_relpath,
    open_client,
)
from app.connectors.nas.diagnostics import run_connection_test
from app.core.exceptions import ConnectorError
from app.core.security import require_api_key
from app.db.models.nas_config import NasConfig
from app.schemas.nas import (
    NasConfigInput,
    NasConfigResponse,
    NasFoldersResponse,
    NasTestResponse,
    NasTransferInput,
    NasTransferResponse,
    NasVerifyFolderRequest,
    NasVerifyFolderResponse,
)
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


@router.post("/transfer", response_model=NasTransferResponse)
def transfer_nas_images(body: NasTransferInput, db: Session = Depends(get_db)):
    """Transfers/copies images between NAS and local storage over a single connection."""
    service = NasConfigService(db)
    saved = service.get()
    if saved is None:
        return NasTransferResponse(
            ok=False,
            direction=body.direction,
            transferred_count=0,
            total_bytes=0,
            duration_s=0.0,
            message="No NAS is configured yet. Please configure NAS connection first.",
            sample_files=[],
        )
    try:
        params = service.params_for(saved)
    except RuntimeError as e:
        return NasTransferResponse(
            ok=False,
            direction=body.direction,
            transferred_count=0,
            total_bytes=0,
            duration_s=0.0,
            message=f"Could not decrypt stored NAS password: {e}",
            sample_files=[],
        )

    started = time.monotonic()
    transferred_files: list[str] = []
    total_bytes = 0

    try:
        if body.direction == "local_to_nas":
            raw_local = body.source_folder.strip() or "work_data"
            local_dir = Path(raw_local).resolve()
            if not local_dir.exists():
                return NasTransferResponse(
                    ok=False,
                    direction=body.direction,
                    transferred_count=0,
                    total_bytes=0,
                    duration_s=0.0,
                    message=f"Local source directory does not exist: {local_dir}",
                    sample_files=[],
                )

            # Discover image files locally
            local_files = [
                f for f in local_dir.rglob("*")
                if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
            ][:body.max_images]

            if not local_files:
                return NasTransferResponse(
                    ok=True,
                    direction=body.direction,
                    transferred_count=0,
                    total_bytes=0,
                    duration_s=round(time.monotonic() - started, 3),
                    message=f"No image files found in {local_dir} matching {', '.join(sorted(IMAGE_EXTENSIONS))}",
                    sample_files=[],
                )

            dest_folder = normalize_relpath(body.destination_folder.strip() or params.storage_path or "data_automation")
            logger.info(
                f"[NAS TRANSFER] Starting upload of {len(local_files)} images from {local_dir} "
                f"to NAS destination '{dest_folder}' over single connection..."
            )
            # Exactly one connection opened; all images transferred over this single session
            with open_client(params) as client:
                for f in local_files:
                    rel = join_relpath(dest_folder, f.name)
                    client.upload(f, rel)
                    size = f.stat().st_size
                    total_bytes += size
                    transferred_files.append(f.name)

        elif body.direction == "nas_to_local":
            raw_dest = body.destination_folder.strip() or "work_data/nas_transfers"
            local_dest = Path(raw_dest).resolve()
            local_dest.mkdir(parents=True, exist_ok=True)

            nas_src = normalize_relpath(body.source_folder.strip() or params.base_path or "")
            logger.info(
                f"[NAS TRANSFER] Starting download of images from NAS folder '{nas_src}' "
                f"to local destination {local_dest} over single connection..."
            )
            # Exactly one connection opened; all images transferred over this single session
            with open_client(params) as client:
                remote_files = list(client.walk(nas_src, recursive=True, extensions=IMAGE_EXTENSIONS, max_files=body.max_images))
                if not remote_files:
                    return NasTransferResponse(
                        ok=True,
                        direction=body.direction,
                        transferred_count=0,
                        total_bytes=0,
                        duration_s=round(time.monotonic() - started, 3),
                        message=f"No images found on NAS in folder '{nas_src or '/'}'",
                        sample_files=[],
                    )
                for rf in remote_files:
                    dest_file = local_dest / Path(rf.path).name
                    client.download(rf.path, dest_file)
                    total_bytes += rf.size
                    transferred_files.append(Path(rf.path).name)

        duration = round(time.monotonic() - started, 3)
        logger.info(
            f"[NAS TRANSFER] Successfully transferred {len(transferred_files)} image(s) ({total_bytes} bytes) "
            f"in {duration}s over single connection."
        )
        return NasTransferResponse(
            ok=True,
            direction=body.direction,
            transferred_count=len(transferred_files),
            total_bytes=total_bytes,
            duration_s=duration,
            message=f"Successfully transferred {len(transferred_files)} image(s) in {duration}s over single connection.",
            sample_files=transferred_files[:10],
        )
    except ConnectorError as exc:
        duration = round(time.monotonic() - started, 3)
        logger.error(f"[NAS TRANSFER] Transfer failed: {exc}")
        return NasTransferResponse(
            ok=False,
            direction=body.direction,
            transferred_count=len(transferred_files),
            total_bytes=total_bytes,
            duration_s=duration,
            message=params.redact(str(exc)),
            sample_files=transferred_files[:10],
        )
    except Exception as exc:
        duration = round(time.monotonic() - started, 3)
        logger.exception(f"[NAS TRANSFER] Unexpected error during transfer: {exc}")
        return NasTransferResponse(
            ok=False,
            direction=body.direction,
            transferred_count=len(transferred_files),
            total_bytes=total_bytes,
            duration_s=duration,
            message=f"Transfer error: {type(exc).__name__} - {exc}",
            sample_files=transferred_files[:10],
        )


@router.get("/folders", response_model=NasFoldersResponse)
def list_nas_folders(path: str = "", db: Session = Depends(get_db)):
    """Explores directories on the connected NAS in a single connection session."""
    service = NasConfigService(db)
    saved = service.get()
    if saved is None:
        raise HTTPException(404, "No NAS is configured yet")
    try:
        params = service.params_for(saved)
    except RuntimeError as e:
        raise HTTPException(503, f"Could not decrypt stored NAS password: {e}") from e

    clean_path = path.strip().strip("/")
    try:
        with open_client(params) as client:
            entries = client.list_dir(clean_path)
            folders = sorted([
                e.name for e in entries
                if e.is_dir and not e.name.startswith(".") and not e.name.startswith("#")
            ])
            parent = clean_path.rpartition("/")[0] if "/" in clean_path else ("" if clean_path else None)
            return NasFoldersResponse(
                current_path=clean_path,
                parent_path=parent,
                folders=folders,
            )
    except Exception as exc:
        logger.warning(f"Error exploring NAS folders at '{clean_path}': {exc}")
        raise HTTPException(400, f"Could not read NAS folders: {exc}") from exc


@router.post("/verify-folder", response_model=NasVerifyFolderResponse)
def verify_nas_folder(body: NasVerifyFolderRequest, db: Session = Depends(get_db)):
    """Verifies whether a specific folder path exists on the NAS in a single connection session."""
    service = NasConfigService(db)
    saved = service.get()
    if saved is None:
        return NasVerifyFolderResponse(exists=False, path=body.folder_path, message="No NAS is configured yet")
    try:
        params = service.params_for(saved)
    except RuntimeError as e:
        return NasVerifyFolderResponse(exists=False, path=body.folder_path, message=f"Credential error: {e}")

    clean_path = body.folder_path.strip().strip("/")
    if not clean_path:
        return NasVerifyFolderResponse(exists=True, path="", message="Root directory exists")

    try:
        with open_client(params) as client:
            # Check by listing the path directly
            client.list_dir(clean_path)
            return NasVerifyFolderResponse(exists=True, path=clean_path, message="Folder exists on NAS")
    except Exception:
        # Also try checking if parent has this folder
        try:
            parent = clean_path.rpartition("/")[0]
            target_name = clean_path.rpartition("/")[2]
            with open_client(params) as client:
                parent_entries = client.list_dir(parent)
                exists = any(e.name == target_name and e.is_dir for e in parent_entries)
                if exists:
                    return NasVerifyFolderResponse(exists=True, path=clean_path, message="Folder exists on NAS")
        except Exception:
            pass
        return NasVerifyFolderResponse(exists=False, path=clean_path, message="Folder does not exist on NAS")


