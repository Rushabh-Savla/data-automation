"""LocalFilesystemStorage — writes to a mounted NAS path (or a local dev path).

Per the confirmed NAS access decision: the NAS is reachable as a local/mapped
filesystem path, so "promoting to NAS" is a plain, validated file copy.
"""

import shutil
from pathlib import Path

from app.storage.base import StorageBackend


class LocalFilesystemStorage(StorageBackend):
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, local_src: Path, relative_dest: str) -> str:
        dest = self._safe_join(relative_dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_src, dest)
        return str(dest)

    def exists(self, relative_path: str) -> bool:
        return self._safe_join(relative_path).exists()

    def delete(self, relative_path: str) -> None:
        path = self._safe_join(relative_path)
        path.unlink(missing_ok=True)

    def resolve(self, relative_path: str) -> Path:
        return self._safe_join(relative_path)

    def _safe_join(self, relative_path: str) -> Path:
        """Resolve relative_path under root, rejecting any attempt to escape
        it (the same Zip-Slip-style guard used for archive extraction)."""
        candidate = (self.root / relative_path).resolve()
        if not str(candidate).startswith(str(self.root.resolve())):
            raise ValueError(f"Path '{relative_path}' escapes storage root")
        return candidate
