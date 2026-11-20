"""NasShareConnector — images that already live on the user's NAS, as a job source.

There are no labels to query, so discovery works like the other unlabelled
sources: files whose path or file name mentions the query come first (a folder
called "cars/" is a strong signal), then the rest of the share in a deterministic
pseudo-random order, and the relevance filter makes the final call on each image.

Only root-relative file paths ever leave this class in `external_ref` (it is stored
in Celery messages and task checkpoints). Credentials stay in the connection params
held in memory by the worker that built this connector."""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterator
from pathlib import Path

from app.connectors.base import (
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    register_connector,
)
from app.connectors.nas.base import (
    IMAGE_EXTENSIONS,
    NasConnectionParams,
    NasNotFoundError,
    normalize_relpath,
    open_client,
)
from app.connectors.query_terms import expand_terms, singularize
from app.core.exceptions import CandidateWindowFull, ConnectorError, FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

_SHARD_SIZE = 50
# Upper bound on files listed per discovery, so pointing a job at a share with
# millions of files cannot make discovery run for hours.
_MAX_SCAN_FILES = 200_000


def _path_words(rel_path: str) -> set[str]:
    return {singularize(w) for w in re.split(r"[\s._/\\-]+", rel_path.lower()) if w}


@register_connector(SourceType.NAS_SHARE)
class NasShareConnector:
    def __init__(self, params: NasConnectionParams, min_resolution: tuple[int, int] = (64, 64)):
        self.params = params
        self.min_w, self.min_h = min_resolution

    @property
    def dataset_identifier(self) -> str:
        p = self.params
        return f"nas:{p.protocol}://{p.host or 'local'}/{p.share.strip('/')}"

    def discover(self, query: str, cursor: str | None, limit: int) -> DiscoveryPage:
        base = normalize_relpath(self.params.base_path)
        try:
            with open_client(self.params) as client:
                files = [f.path for f in client.walk(base, True, IMAGE_EXTENSIONS, max_files=_MAX_SCAN_FILES)]
        except ConnectorError as e:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True, error_reason=self.params.redact(str(e)))

        # Filtered images are stored on this same NAS; never feed them back in as candidates.
        storage = normalize_relpath(self.params.storage_path)
        if storage and not (base == storage or base.startswith(storage + "/")):
            files = [p for p in files if not (p == storage or p.startswith(storage + "/"))]

        if not files:
            where = f"'{base}'" if base else "the NAS root"
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True, error_reason=f"No images found in {where}"
            )

        terms = set(expand_terms(query))
        matched = sorted(p for p in files if terms and _path_words(p) & terms)
        matched_set = set(matched)
        # Hash order: deterministic (so top-up rounds re-derive the same list and
        # only extend it) but not biased toward whichever folder sorts first.
        rest = sorted((p for p in files if p not in matched_set), key=lambda p: hashlib.sha1(p.encode()).hexdigest())

        chosen_matched = matched[:limit]
        chosen_rest = rest[: max(0, limit - len(chosen_matched))]

        items: list[SourceItem] = []
        for group, unlabelled in ((chosen_matched, False), (chosen_rest, True)):
            for start in range(0, len(group), _SHARD_SIZE):
                items.append(
                    SourceItem(
                        source_type=SourceType.NAS_SHARE,
                        external_ref={"paths": group[start : start + _SHARD_SIZE], "unlabelled": unlabelled},
                        license_hint="user-provided",
                        dataset_identifier=self.dataset_identifier,
                    )
                )
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
        admit_candidate=None,
    ) -> Iterator[RawAssetDraft]:
        from PIL import Image

        paths: list[str] = item.external_ref["paths"]
        unlabelled = bool(item.external_ref.get("unlabelled"))
        workdir.mkdir(parents=True, exist_ok=True)

        with open_client(self.params) as client:
            for i, rel in enumerate(paths):
                if i < checkpoint.get("index", 0):
                    continue
                if should_stop():
                    on_progress({"checkpoint": {"index": i}})
                    raise JobStopped("should_stop() signalled during NAS share shard walk")

                identity = f"{self.params.protocol}://{self.params.host}/{self.params.share.strip('/')}/{rel}"
                source_item_id = f"nas_{hashlib.sha1(identity.encode()).hexdigest()[:32]}"

                admission = None
                if admit_candidate is not None:
                    admission = admit_candidate(source_item_id)
                    raw_status = getattr(admission, "status", "")
                    status = str(getattr(raw_status, "value", raw_status)).upper()
                    if status == "DUPLICATE":
                        on_progress({"checkpoint": {"index": i + 1}})
                        continue
                    if status == "WINDOW_FULL":
                        on_progress({"checkpoint": {"index": i}})
                        raise CandidateWindowFull(
                            getattr(admission, "window_id", None),
                            getattr(admission, "window_index", None),
                            i,
                        )
                    if status == "INACTIVE":
                            raise JobStopped(f"Candidate admission closed for {source_item_id}")
                    if status != "ADMITTED":
                        continue

                suffix = Path(rel).suffix.lower() or ".jpg"
                dest = workdir / f"{hashlib.sha1(rel.encode()).hexdigest()[:16]}{suffix}"
                try:
                    client.download(rel, dest)
                    with Image.open(dest) as img:
                        img.verify()
                    with Image.open(dest) as img:
                        w, h = img.size
                    if w < self.min_w or h < self.min_h:
                        dest.unlink(missing_ok=True)
                        if admission is not None and getattr(admission, "reservation_id", None):
                            from app.services.candidate_reservation_service import mark_candidate_failed
                            mark_candidate_failed(admission.reservation_id)
                        on_progress({"rejected": 1})
                        continue
                except NasNotFoundError:
                    if admission is not None and getattr(admission, "reservation_id", None):
                        from app.services.candidate_reservation_service import mark_candidate_failed
                        mark_candidate_failed(admission.reservation_id)
                    on_progress({"rejected": 1})
                    continue
                except TransientConnectorError:
                    dest.unlink(missing_ok=True)
                    raise
                except ConnectorError:
                    dest.unlink(missing_ok=True)
                    raise
                except Exception as e:
                    logger.warning(f"NAS image {rel} could not be read as an image: {e}")
                    dest.unlink(missing_ok=True)
                    if admission is not None and getattr(admission, "reservation_id", None):
                        from app.services.candidate_reservation_service import mark_candidate_failed
                        mark_candidate_failed(admission.reservation_id)
                    on_progress({"rejected": 1})
                    continue

                metadata = {"nas_path": rel, "matched_by": "vlm" if unlabelled else "path"}
                if admission is not None:
                    metadata["candidate_reservation_id"] = str(admission.reservation_id)
                    metadata["candidate_window_id"] = str(admission.window_id)
                    metadata["candidate_window_index"] = int(admission.window_index)

                draft = RawAssetDraft(
                    source_item_id=source_item_id,
                    source_url=f"nas://{self.params.host or 'local'}/{self.params.share.strip('/')}/{rel}",
                    dataset_identifier=item.dataset_identifier,
                    license=item.license_hint,
                    local_tmp_path=dest,
                    original_name=Path(rel).name,
                    width=w,
                    height=h,
                    metadata=metadata,
                )
                on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
                yield draft
                # The driver has promoted the file to staging by the time we resume,
                # so the temp copy is dead weight — free it now, not at end of task.
                dest.unlink(missing_ok=True)
