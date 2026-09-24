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
    NasNotFoundError,
    NasConnectionParams,
    join_relpath,
    normalize_relpath,
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


def run_connection_test(params: NasConnectionParams, require_write: bool) -> NasTestOutcome:
    started = time.monotonic()
    try:
        client = open_client(params)
        client.connect()
    except ConnectorError as exc:
        return NasTestOutcome(ok=False, message=params.redact(str(exc)))
    except Exception as exc:  # never let an adapter bug surface as a 500 with internals
        return NasTestOutcome(ok=False, message=params.redact(f"Unexpected error: {type(exc).__name__}"))

    outcome = NasTestOutcome(ok=False)
    notes: list[str] = []
    try:
        outcome.latency_ms = int((time.monotonic() - started) * 1000)

        try:
            base = normalize_relpath(params.base_path)
            outcome.sample_files = [
                f.path
                for f in client.walk(base, True, IMAGE_EXTENSIONS, max_files=_SAMPLE_LIMIT, max_dirs=_PROBE_DIR_BUDGET)
            ]
            outcome.can_read = True
            if not outcome.sample_files:
                notes.append("no images found in the source folder yet")
        except NasNotFoundError:
            notes.append(f"source folder '{params.base_path or '/'}' was not found")
        except ConnectorError as exc:
            notes.append(f"could not read the source folder: {params.redact(str(exc))}")

        try:
            _probe_write(client, params.storage_path)
            outcome.can_write = True
        except ConnectorError as exc:
            notes.append(f"cannot write to the storage folder: {params.redact(str(exc))}")

        # "ok" means usable for what the user turned on. Storing images needs write
        # access; a source folder that is missing or unreadable only matters when the
        # NAS is used as an image source, so with storage on it is reported as a note.
        outcome.ok = outcome.can_write if require_write else outcome.can_read
        if outcome.ok and not notes:
            outcome.message = "Connected. The NAS can be read and written."
        elif outcome.ok:
            outcome.message = "Connected — " + "; ".join(notes) + "."
        else:
            outcome.message = "Connected, but " + "; ".join(notes) + "."
    except Exception as exc:
        outcome.ok = False
        outcome.message = params.redact(f"Unexpected error: {type(exc).__name__}")
    finally:
        client.close()
    return outcome
