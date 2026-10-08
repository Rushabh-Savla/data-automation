"""Turn a free-text job name into a filesystem-safe, unique-enough folder name."""

import re
import uuid

_SLUG_INVALID = re.compile(r"[^a-z0-9]+")
_MAX_SLUG_LEN = 60


def job_folder_name(name: str | None, job_id: uuid.UUID) -> str:
    """<slug-of-name>-<4-char-id>, or just the full job_id if name is blank
    (job name is mandatory at job-creation time, but older rows may predate that).

    The id suffix guarantees uniqueness even when two jobs share a name."""
    slug = _SLUG_INVALID.sub("-", (name or "").strip().lower()).strip("-")[:_MAX_SLUG_LEN].strip("-")
    if not slug:
        return str(job_id)
    return f"{slug}-{str(job_id)[:4]}"
