"""Authoritative NAS Destination Resolver.

Provides centralized resolution, sanitization, and path normalization
for user-selected or system-default NAS storage destinations.
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
import re
from typing import Any

from app.services.nas_config_service import get_active_storage_params

logger = logging.getLogger(__name__)

DEFAULT_NAS_DESTINATION = "data_automation"


def sanitize_nas_path(path: str | None) -> str | None:
    """Sanitizes user-provided or configured NAS path.

    Rules:
      - Strips leading and trailing slashes and whitespace
      - Normalizes forward slashes and converts backslashes
      - Disallows path traversal sequences ('..')
      - Returns normalized POSIX relative path, or None if empty or invalid.
    """
    if not path or not isinstance(path, str):
        return None

    cleaned = path.strip().replace("\\", "/")
    if not cleaned:
        return None

    posix_path = PurePosixPath(cleaned)
    parts: list[str] = []

    for part in posix_path.parts:
        part_clean = part.strip()
        if part_clean in ("/", "\\", ".", ""):
            continue
        if part_clean == "..":
            # Strip directory traversal component
            continue
        # Remove dangerous control characters or null bytes
        safe_part = re.sub(r"[\0\r\n\t]", "", part_clean)
        if safe_part:
            parts.append(safe_part)

    if not parts:
        return None

    return "/".join(parts)


def resolve_job_nas_destination(
    job_or_config: Any,
    default_storage: str | None = None,
) -> str | None:
    """Authoritatively resolves the base NAS storage folder for a job.

    Precedence Order:
      1. Explicit user selection: job.config['nas_storage_path']
      2. Explicit default_storage parameter (e.g. transfer.remote_path)
      3. If neither is provided, returns None (images stay in local storage only).
         The platform does NOT automatically fall back to any previously configured NAS path.
    """
    custom_path: str | None = None

    if hasattr(job_or_config, "config") and isinstance(job_or_config.config, dict):
        custom_path = job_or_config.config.get("nas_storage_path")
    elif isinstance(job_or_config, dict):
        cfg = job_or_config.get("config")
        if isinstance(cfg, dict):
            custom_path = cfg.get("nas_storage_path")
        if not custom_path:
            custom_path = job_or_config.get("nas_storage_path") or job_or_config.get("nas_destination")

    sanitized_custom = sanitize_nas_path(custom_path)
    if sanitized_custom:
        return sanitized_custom

    if default_storage:
        sanitized_default = sanitize_nas_path(default_storage)
        if sanitized_default:
            return sanitized_default

    return None
