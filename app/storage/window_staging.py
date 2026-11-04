"""Durable, isolated SSD staging for individual candidate acquisition windows."""

import shutil
import uuid
from pathlib import Path

from app.core.config import get_settings


class WindowStagingManager:
    """Keeps each acquisition window's files isolated on persistent SSD."""

    def __init__(self, staging_path: Path | None = None):
        settings = get_settings()
        self.root = Path(staging_path or settings.local_staging_path).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def get_window_staging_dir(
        self,
        job_id: uuid.UUID | str,
        batch_index: int,
        window_index: int,
    ) -> Path:
        if batch_index < 1 or window_index < 1:
            raise ValueError("batch_index and window_index must be positive")

        job_uuid = uuid.UUID(str(job_id))

        return (
            self.root
            / f"job_{job_uuid}"
            / f"batch_{batch_index:03d}"
            / f"window_{window_index:04d}"
        )

    def get_window_manifest_path(
        self,
        job_id: uuid.UUID | str,
        batch_index: int,
        window_index: int,
    ) -> Path:
        return (
            self.get_window_staging_dir(job_id, batch_index, window_index)
            / "manifest.json"
        )

    def stage_accepted_image(
        self,
        job_id: uuid.UUID | str,
        batch_index: int,
        window_index: int,
        asset_id: uuid.UUID | str,
        file_ext: str,
        source,
    ) -> Path:
        """Atomically stage an accepted image on local SSD."""

        asset_uuid = uuid.UUID(str(asset_id))
        ext = file_ext.lstrip(".").lower() if file_ext else "jpg"

        if not ext.isalnum():
            raise ValueError("Invalid image file extension")

        images_dir = (
            self.get_window_staging_dir(job_id, batch_index, window_index)
            / "images"
        )
        images_dir.mkdir(parents=True, exist_ok=True)

        destination = images_dir / f"{asset_uuid}.{ext}"
        temporary = images_dir / f".{asset_uuid}.{uuid.uuid4().hex}.tmp"

        try:
            if hasattr(source, "stage_to_file"):
                source.stage_to_file(temporary)
            elif isinstance(source, Path):
                shutil.copyfile(source, temporary)
            elif isinstance(source, (bytes, bytearray)):
                temporary.write_bytes(source)
            else:
                raise TypeError(f"Unsupported staging source: {type(source)}")

            temporary.replace(destination)
            return destination

        except Exception:
            temporary.unlink(missing_ok=True)
            raise

    def cleanup_window_staging(
        self,
        job_id: uuid.UUID | str,
        batch_index: int,
        window_index: int,
    ) -> bool:
        """Remove only this window's directory after verified durable upload."""

        window_dir = self.get_window_staging_dir(
            job_id, batch_index, window_index
        ).resolve()

        if self.root not in window_dir.parents:
            raise ValueError("Window directory is outside the staging root")

        if not window_dir.exists():
            return True

        try:
            shutil.rmtree(window_dir)
        except OSError:
            return False

        return not window_dir.exists()
