"""StorageBackend — abstraction over "where do promoted bytes live."

A LocalFilesystemStorage implementation (writing to a mounted NAS path) is
provided now; an S3-compatible implementation can be added later purely by
implementing this interface — no connector or service code would change
(Open/Closed, Dependency Inversion).
"""

from abc import ABC, abstractmethod
from pathlib import Path


class StorageBackend(ABC):
    @abstractmethod
    def write(self, local_src: Path, relative_dest: str) -> str:
        """Copy/move a local file into durable storage at `relative_dest`
        (relative to the storage root). Returns the absolute/canonical path
        recorded as Asset.nas_path."""

    @abstractmethod
    def exists(self, relative_path: str) -> bool: ...

    @abstractmethod
    def delete(self, relative_path: str) -> None: ...

    @abstractmethod
    def resolve(self, relative_path: str) -> Path:
        """Return the absolute local path for reading (e.g. streaming to an
        API response). Only meaningful for backends where reads are local-file
        reads; remote backends could instead return a signed URL — not needed yet."""
