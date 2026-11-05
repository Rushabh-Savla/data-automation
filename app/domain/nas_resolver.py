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
) -> str:
    """Authoritatively resolves the base NAS storage folder for a job.

    Precedence Order:
      1. Explicit user selection: job.config['nas_storage_path']
      2. Explicit default_storage parameter (e.g. transfer.remote_path)
      3. Active NAS configuration: params.storage_path
      4. Hardcoded fallback default: 'data_automation'
    """
    custom_path: str | None = None

    if hasattr(job_or_config, "config") and isinstance(job_or_config.config, dict):
        custom_path = job_or_config.config.get("nas_storage_path")
    elif isinstance(job_or_config, dict):
        custom_path = job_or_config.get("nas_storage_path")

    sanitized_custom = sanitize_nas_path(custom_path)
    if sanitized_custom:
        return sanitized_custom

    if default_storage:
        sanitized_default = sanitize_nas_path(default_storage)
        if sanitized_default:
            return sanitized_default

    try:
        params = get_active_storage_params()
        if params and params.storage_path:
            sanitized_params = sanitize_nas_path(params.storage_path)
            if sanitized_params:
                return sanitized_params
    except Exception as e:
        logger.debug("Could not read active NAS storage params: %s", e)

    return DEFAULT_NAS_DESTINATION
