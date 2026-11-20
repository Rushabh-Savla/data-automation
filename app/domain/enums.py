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


class SourceSelectionMode(str, enum.Enum):
    MANUAL = "manual"
    AUTO = "auto"
    RANDOM = "random"


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
    WINDOW_WAITING = "window_waiting"
    STANDARDIZING = "standardizing"
    DISCOVERY_COMPLETED = "discovery_completed"
    PARTIALLY_PROCESSED = "partially_processed"
    WAITING_FOR_DOWNLOAD = "waiting_for_download"
    SOURCE_EXHAUSTED = "source_exhausted"
    COMPLETED = "completed"
    COMPLETED_PARTIAL = "completed_partial"
    FAILED = "failed"
    CANCELLED = "cancelled"
    STALLED = "stalled"


JOB_SOURCE_TASK_TERMINAL_STATUSES = {
    JobSourceTaskStatus.COMPLETED,
    JobSourceTaskStatus.SOURCE_EXHAUSTED,
    JobSourceTaskStatus.FAILED,
    JobSourceTaskStatus.CANCELLED,
}


def is_source_task_exhausted(task) -> bool:
    """A source is genuinely exhausted only when discovery is exhausted and all pending items are consumed."""
    status = getattr(task, "status", None)
    if status in (JobSourceTaskStatus.COMPLETED, JobSourceTaskStatus.SOURCE_EXHAUSTED):
        return True
    if status in (JobSourceTaskStatus.FAILED, JobSourceTaskStatus.CANCELLED):
        return True
    chk = getattr(task, "checkpoint", None) or {}
    pending = chk.get("_pending_items") or {}
    completed = set(chk.get("_completed_items", []))
    if isinstance(pending, dict):
        unconsumed = len(set(pending.keys()) - completed)
    elif isinstance(pending, (list, set)):
        unconsumed = len(set(pending) - completed)
    else:
        unconsumed = 0
    discovery_exhausted = bool(chk.get("discovery_exhausted", False))
    return discovery_exhausted and unconsumed == 0



class AssetStatus(str, enum.Enum):
    DISCOVERED = "discovered"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    STANDARDIZING = "standardizing"
    STANDARDIZED = "standardized"
    PROMOTED = "promoted"
    RELEVANCE_PENDING = "relevance_pending"
    RELEVANCE_PASSED = "relevance_passed"
    RELEVANCE_REJECTED = "relevance_rejected"
    REVIEW_PENDING = "review_pending"
    RETRY_PENDING = "retry_pending"
    READY_FOR_UPLOAD = "ready_for_upload"
    UPLOADING = "uploading"
    NAS_VERIFIED = "nas_verified"
    COMPLETED = "completed"
    DUPLICATE = "duplicate"
    FAILED = "failed"
    SURPLUS = "surplus"


# Assets in these statuses count as "accepted" toward a job's target_count
# and are what export/asset-browsing defaults to including.
# Strictly excludes unclassified (PROMOTED, RELEVANCE_PENDING), duplicates,
# rejections, uncertain reviews, errors, and unverified assets.
RELEVANCE_ACCEPTED_STATUSES = {
    AssetStatus.RELEVANCE_PASSED,
    AssetStatus.READY_FOR_UPLOAD,
    AssetStatus.UPLOADING,
    AssetStatus.NAS_VERIFIED,
    AssetStatus.COMPLETED,
}
ASSET_ACCEPTED_STATUSES = RELEVANCE_ACCEPTED_STATUSES

# Disjoint metric status sets for detailed lifecycle accounting
UPLOAD_PENDING_STATUSES = {AssetStatus.READY_FOR_UPLOAD}
UPLOADING_STATUSES = {AssetStatus.UPLOADING}
NAS_VERIFIED_STATUSES = {AssetStatus.NAS_VERIFIED}
COMPLETED_STATUSES = {AssetStatus.COMPLETED}
REJECTED_STATUSES = {AssetStatus.RELEVANCE_REJECTED}
DUPLICATE_STATUSES = {AssetStatus.DUPLICATE}
REVIEW_PENDING_STATUSES = {AssetStatus.REVIEW_PENDING}
RETRY_PENDING_STATUSES = {AssetStatus.RETRY_PENDING}
SURPLUS_STATUSES = {AssetStatus.SURPLUS}


class BatchStatus(str, enum.Enum):
    CREATED = "created"
    DISCOVERING = "discovering"
    DOWNLOADING = "downloading"
    PROCESSING = "processing"
    FILTERING = "filtering"
    FILLING_SHORTFALL = "filling_shortfall"
    REPLENISHING = "replenishing"
    READY_FOR_UPLOAD = "ready_for_upload"
    UPLOADING = "uploading"
    NAS_UPLOADING = "nas_uploading"
    NAS_PENDING = "nas_pending"
    VERIFYING = "verifying"
    NAS_VERIFIED = "nas_verified"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    PAUSED = "paused"


class TransferStatus(str, enum.Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    VERIFIED = "verified"
    COMPLETED = "completed"
    FAILED = "failed"


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
