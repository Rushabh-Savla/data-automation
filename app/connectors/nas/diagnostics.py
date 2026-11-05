"""Connection test behind the UI's "Test connection" button.

Checks the three things that actually go wrong, in the order a user would fix
them: can we reach and log in, can we read the source folder, can we write to the
storage folder. The write check uploads, re-downloads and deletes a tiny probe
file, so a read-only account is reported before a job discovers it the hard way."""

from __future__ import annotations

import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from app.connectors.nas.base import (
    IMAGE_EXTENSIONS,
    NasClient,
    NasConnectionParams,
    NasNotFoundError,
    join_relpath,
    open_client,
)
from app.core.exceptions import ConnectorError

_SAMPLE_LIMIT = 5
_PROBE_DIR_BUDGET = 50


@dataclass
class NasTestOutcome:
    ok: bool
    can_read: bool = False
    can_write: bool = False
    message: str = ""
    latency_ms: int | None = None
    sample_files: list[str] = field(default_factory=list)


def _probe_write(client: NasClient, storage_path: str) -> None:
    rel = join_relpath(storage_path, f".data_automation_probe_{uuid.uuid4().hex[:8]}")
    payload = b"data-automation write probe"
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "probe.bin"
        src.write_bytes(payload)
        client.upload(src, rel)
        try:
            if client.size(rel) != len(payload):
                raise ConnectorError("The NAS accepted the test file but it was stored incorrectly")
            back = Path(tmp) / "back.bin"
            client.download(rel, back)
            if back.read_bytes() != payload:
                raise ConnectorError("The NAS returned different data than was written")
        finally:
            client.delete(rel)


def run_connection_test(params: NasConnectionParams, require_write: bool = False) -> NasTestOutcome:
    started = time.monotonic()
    try:
        client = open_client(params)
        client.connect()
    except ConnectorError as exc:
        return NasTestOutcome(ok=False, message=params.redact(str(exc)))
    except Exception as exc:  # noqa: BLE001 # never let an adapter bug surface as a 500 with internals
        return NasTestOutcome(ok=False, message=params.redact(f"Unexpected error: {type(exc).__name__}"))

    latency_ms = int((time.monotonic() - started) * 1000)
    try:
        # 1. Test reading source folder
        can_read = False
        sample_files: list[str] = []
        read_err: str | None = None
        try:
            for rf in client.walk(
                rel=params.base_path,
                recursive=True,
                extensions=IMAGE_EXTENSIONS,
                max_files=_SAMPLE_LIMIT,
                max_dirs=_PROBE_DIR_BUDGET,
            ):
                sample_files.append(rf.path)
            can_read = True
        except NasNotFoundError as exc:
            can_read = False
            read_err = f"Source folder '{params.base_path}' not found: {exc}"
        except Exception as exc:  # noqa: BLE001
            can_read = False
            read_err = f"Could not read source folder '{params.base_path}': {exc}"

        # 2. Test writing storage folder
        can_write = False
        write_err: str | None = None
        has_storage_folder = bool(
            params.storage_path
            and params.storage_path.strip()
            and params.storage_path.strip() != "data_automation"
        )
        if has_storage_folder:
            try:
                _probe_write(client, params.storage_path)
                can_write = True
            except Exception as exc:  # noqa: BLE001
                can_write = False
                write_err = f"cannot write to storage folder '{params.storage_path}': {exc}"
        else:
            can_write = True

        # 3. Determine overall success (ok) and message
        if require_write and has_storage_folder:
            ok = can_write and can_read
        else:
            ok = can_read

        messages: list[str] = []
        if can_read and (can_write or not has_storage_folder):
            if not sample_files and params.base_path:
                messages.append("Source folder contains no images.")
            messages.append("NAS connection verified and accessible.")
        elif can_read and not can_write:
            if require_write and has_storage_folder:
                messages.append(write_err or "cannot write to storage folder")
            else:
                if not sample_files and params.base_path:
                    messages.append("Source folder contains no images.")
                messages.append("Source folder can be read.")
        elif not can_read and can_write:
            if read_err:
                messages.append(read_err)
            else:
                messages.append(f"Source folder '{params.base_path}' not found")
        else:
            if read_err:
                messages.append(read_err)
            if write_err:
                messages.append(write_err)

        final_msg = params.redact(" ".join(messages))
        return NasTestOutcome(
            ok=ok,
            can_read=can_read,
            can_write=can_write,
            message=final_msg,
            latency_ms=latency_ms,
            sample_files=sample_files,
        )
    finally:
        client.close()
