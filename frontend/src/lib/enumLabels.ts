import type {
  AnnotationFormat,
  AssetStatus,
  ExportFormat,
  ExportJobStatus,
  JobCompletionReason,
  JobSourceTaskStatus,
  JobStatus,
  SourceType,
} from '@/api/types'

export const sourceTypeLabels: Record<SourceType, string> = {
  roboflow: 'Roboflow',
  kaggle: 'Kaggle',
  youtube: 'YouTube',
  web: 'Web',
  coco: 'COCO',
  open_images: 'Open Images V7',
  pascal_voc: 'Pascal VOC',
  visual_genome: 'Visual Genome',
  davis: 'DAVIS 2017',
  sun_rgbd: 'SUN RGB-D',
  nuscenes: 'nuScenes',
  kitti: 'KITTI',
  nas_share: 'NAS Share',
}

export const jobStatusLabels: Record<JobStatus, string> = {
  draft: 'Draft',
  scheduled: 'Scheduled',
  discovering: 'Discovering',
  running: 'Running',
  paused: 'Paused',
  cancelling: 'Cancelling',
  cancelled: 'Cancelled',
  completed: 'Completed',
  completed_partial: 'Completed (partial)',
  failed: 'Failed',
}

export const jobCompletionReasonLabels: Record<JobCompletionReason, string> = {
  target_count_reached: 'Target count reached',
  deadline_reached: 'Deadline reached',
  sources_exhausted: 'Sources exhausted',
  cancelled: 'Cancelled',
  failed: 'Failed',
}

export const jobSourceTaskStatusLabels: Record<JobSourceTaskStatus, string> = {
  pending: 'Pending',
  discovering: 'Discovering',
  downloading: 'Downloading',
  standardizing: 'Standardizing',
  completed: 'Completed',
  completed_partial: 'Completed (partial)',
  failed: 'Failed',
  cancelled: 'Cancelled',
  stalled: 'Stalled',
}

export const assetStatusLabels: Record<AssetStatus, string> = {
  discovered: 'Discovered',
  downloading: 'Downloading',
  downloaded: 'Downloaded',
  standardizing: 'Standardizing',
  promoted: 'Promoted',
  relevance_pending: 'Relevance pending',
  relevance_passed: 'Relevance passed',
  relevance_rejected: 'Relevance rejected',
  duplicate: 'Duplicate',
  failed: 'Failed',
}

export const annotationFormatLabels: Record<AnnotationFormat, string> = {
  none: 'None',
  yolo_txt: 'YOLO (txt)',
  voc_xml: 'Pascal VOC (xml)',
  coco_json: 'COCO (json)',
}

export const exportFormatLabels: Record<ExportFormat, string> = {
  raw: 'Raw (original files)',
  yolo: 'YOLO',
  coco: 'COCO',
}

export const exportJobStatusLabels: Record<ExportJobStatus, string> = {
  pending: 'Pending',
  running: 'Running',
  completed: 'Completed',
  failed: 'Failed',
}
