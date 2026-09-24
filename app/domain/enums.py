"""Enums shared across the domain, DB models, and API schemas."""

import enum


class SourceType(str, enum.Enum):
    ROBOFLOW = "roboflow"
    KAGGLE = "kaggle"
    YOUTUBE = "youtube"
    WEB = "web"
    COCO = "coco"
    OPEN_IMAGES = "open_images"
    PASCAL_VOC = "pascal_voc"
    VISUAL_GENOME = "visual_genome"
    DAVIS = "davis"
    SUN_RGBD = "sun_rgbd"
    NUSCENES = "nuscenes"
    KITTI = "kitti"
    NAS_SHARE = "nas_share"


class JobStatus(str, enum.Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    DISCOVERING = "discovering"
    RUNNING = "running"
    PAUSED = "paused"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    COMPLETED_PARTIAL = "completed_partial"
    FAILED = "failed"


# States from which a job may still be started/resumed/paused/cancelled.
JOB_ACTIVE_STATUSES = {
    JobStatus.SCHEDULED,
    JobStatus.DISCOVERING,
    JobStatus.RUNNING,
    JobStatus.PAUSED,
    JobStatus.CANCELLING,
}

JOB_TERMINAL_STATUSES = {
    JobStatus.CANCELLED,
    JobStatus.COMPLETED,
    JobStatus.COMPLETED_PARTIAL,
    JobStatus.FAILED,
}


class JobCompletionReason(str, enum.Enum):
    TARGET_COUNT_REACHED = "target_count_reached"
    DEADLINE_REACHED = "deadline_reached"
    SOURCES_EXHAUSTED = "sources_exhausted"
    CANCELLED = "cancelled"
    FAILED = "failed"


class JobSourceTaskStatus(str, enum.Enum):
    PENDING = "pending"
    DISCOVERING = "discovering"
    DOWNLOADING = "downloading"
    STANDARDIZING = "standardizing"
    COMPLETED = "completed"
    COMPLETED_PARTIAL = "completed_partial"
    FAILED = "failed"
    CANCELLED = "cancelled"
    STALLED = "stalled"


JOB_SOURCE_TASK_TERMINAL_STATUSES = {
    JobSourceTaskStatus.COMPLETED,
    JobSourceTaskStatus.COMPLETED_PARTIAL,
    JobSourceTaskStatus.FAILED,
    JobSourceTaskStatus.CANCELLED,
}


class AssetStatus(str, enum.Enum):
    DISCOVERED = "discovered"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    STANDARDIZING = "standardizing"
    PROMOTED = "promoted"
    RELEVANCE_PENDING = "relevance_pending"
    RELEVANCE_PASSED = "relevance_passed"
    RELEVANCE_REJECTED = "relevance_rejected"
    DUPLICATE = "duplicate"
    FAILED = "failed"


# Assets in these statuses count as "accepted" toward a job's target_count
# and are what export/asset-browsing defaults to including.
ASSET_ACCEPTED_STATUSES = {
    AssetStatus.PROMOTED,
    AssetStatus.RELEVANCE_PENDING,
    AssetStatus.RELEVANCE_PASSED,
}


class AnnotationFormat(str, enum.Enum):
    NONE = "none"
    YOLO_TXT = "yolo_txt"
    VOC_XML = "voc_xml"
    COCO_JSON = "coco_json"


class ExportFormat(str, enum.Enum):
    RAW = "raw"
    YOLO = "yolo"
    COCO = "coco"


class ExportJobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
