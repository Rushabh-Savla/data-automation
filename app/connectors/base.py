"""Shared connector abstractions.

Orchestration code (app/tasks/discovery_tasks.py, app/tasks/ingest_tasks.py)
resolves connectors through CONNECTOR_REGISTRY / get_connector() and never
imports a concrete connector class directly — adding a new source means
writing one class and registering it, with zero changes to Celery tasks,
API routes, or the Job state machine (Open/Closed principle).
"""

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from app.domain.enums import SourceType


@dataclass
class SourceItem:
    """One unit of discovered work (a dataset/project/video/image-batch reference)."""

    source_type: SourceType
    external_ref: dict
    license_hint: str | None = None
    dataset_identifier: str | None = None


@dataclass
class DiscoveryPage:
    items: list[SourceItem]
    next_cursor: str | None
    exhausted: bool


@dataclass
class RawAssetDraft:
    """Connector output before standardization: local temp files + metadata,
    not yet validated/checksummed/written to NAS/recorded in the DB."""

    source_item_id: str
    source_url: str | None
    dataset_identifier: str | None
    license: str | None
    local_tmp_path: Path
    original_name: str | None = None
    annotation_tmp_path: Path | None = None
    annotation_format: str | None = None
    width: int | None = None
    height: int | None = None
    classes: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


# Injected by the task wrapper: returns True if the job has been paused/cancelled.
ShouldStopFn = Callable[[], bool]
# Injected by the task wrapper: called with a progress delta dict
# (e.g. {"discovered": 1} or {"checkpoint": {...}}) after each unit of work.
OnProgressFn = Callable[[dict], None]


class DiscoveryConnector(Protocol):
    def discover(self, query: str, cursor: str | None, limit: int) -> DiscoveryPage: ...


class DownloadConnector(Protocol):
    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]: ...


CONNECTOR_REGISTRY: dict[SourceType, type] = {}


def register_connector(source_type: SourceType):
    def decorator(cls):
        CONNECTOR_REGISTRY[source_type] = cls
        return cls

    return decorator


def get_connector(source_type: SourceType, **kwargs):
    cls = CONNECTOR_REGISTRY.get(source_type)
    if cls is None:
        raise ValueError(f"No connector registered for source type: {source_type}")
    return cls(**kwargs)
