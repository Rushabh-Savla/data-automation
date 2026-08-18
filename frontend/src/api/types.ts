// Hand-authored types mirroring the backend's exact schemas/enums
// (app/schemas/*.py, app/domain/enums.py). Kept in sync manually since the
// `config` field on JobCreateRequest is an untyped dict backend-side with no
// OpenAPI-level shape to codegen against.

export type SourceType = 'roboflow' | 'kaggle' | 'youtube' | 'web'

export type JobStatus =
  | 'draft'
  | 'scheduled'
  | 'discovering'
  | 'running'
  | 'paused'
  | 'cancelling'
  | 'cancelled'
  | 'completed'
  | 'completed_partial'
  | 'failed'

export type JobCompletionReason =
  | 'target_count_reached'
  | 'deadline_reached'
  | 'sources_exhausted'
  | 'cancelled'
  | 'failed'

export type JobSourceTaskStatus =
  | 'pending'
  | 'discovering'
  | 'downloading'
  | 'standardizing'
  | 'completed'
  | 'completed_partial'
  | 'failed'
  | 'cancelled'
  | 'stalled'

export type AssetStatus =
  | 'discovered'
  | 'downloading'
  | 'downloaded'
  | 'standardizing'
  | 'promoted'
  | 'relevance_pending'
  | 'relevance_passed'
  | 'relevance_rejected'
  | 'duplicate'
  | 'failed'

export type AnnotationFormat = 'none' | 'yolo_txt' | 'voc_xml' | 'coco_json'

export type ExportFormat = 'raw' | 'yolo' | 'coco'

export type ExportJobStatus = 'pending' | 'running' | 'completed' | 'failed'

export const JOB_ACTIVE_STATUSES: JobStatus[] = [
  'scheduled',
  'discovering',
  'running',
  'paused',
  'cancelling',
]
export const JOB_TERMINAL_STATUSES: JobStatus[] = [
  'cancelled',
  'completed',
  'completed_partial',
  'failed',
]

// -- Job config (freeform `dict` backend-side; this is the shape the
// frontend actually reads/writes — see app/tasks/discovery_tasks.py and
// app/tasks/ingest_tasks.py for the exact keys consumed).

export interface RoboflowConfig {
  max_projects?: number
  export_format?: string
  projects?: [workspace: string, project: string, version: number, license: string][]
}

export interface KaggleConfig {
  max_datasets?: number
  slugs?: [slug: string, license: string][]
}

export interface YoutubeConfig {
  max_videos?: number
  cc_only?: boolean
  frame_mode?: string
  frames_per_video?: number
  urls?: string[]
}

export interface WebConfig {
  max_results?: number
  safesearch?: string
}

export interface JobConfig {
  roboflow?: RoboflowConfig
  kaggle?: KaggleConfig
  youtube?: YoutubeConfig
  web?: WebConfig
  min_resolution?: [width: number, height: number]
  yolo_world_conf_threshold?: number
  dedup_hamming_threshold?: number
}

// -- Jobs

export interface JobCreateRequest {
  query: string
  sources?: SourceType[]
  name?: string | null
  target_count?: number | null
  deadline_at?: string | null
  scheduled_start_at?: string | null
  config?: JobConfig
}

export interface JobUpdateRequest {
  name?: string | null
  target_count?: number | null
  deadline_at?: string | null
  scheduled_start_at?: string | null
  config?: JobConfig
}

export interface JobSourceTaskResponse {
  id: string
  source_type: SourceType
  status: JobSourceTaskStatus
  discovered_count: number
  downloaded_count: number
  accepted_count: number
  rejected_count: number
  error_count: number
  error_message: string | null
  last_heartbeat_at: string | null
}

export interface JobResponse {
  id: string
  name: string | null
  query: string
  sources: string[]
  target_count: number | null
  deadline_at: string | null
  scheduled_start_at: string | null
  status: JobStatus
  completion_reason: JobCompletionReason | null
  discovered_count: number
  downloaded_count: number
  accepted_count: number
  rejected_count: number
  duplicate_count: number
  error_message: string | null
  created_at: string
  updated_at: string
  started_at: string | null
  completed_at: string | null
}

export interface JobDetailResponse extends JobResponse {
  source_tasks: JobSourceTaskResponse[]
}

export interface JobProgressResponse {
  job_id: string
  status: JobStatus
  target_count: number | null
  accepted_count: number
  rejected_count: number
  duplicate_count: number
  discovered_count: number
  downloaded_count: number
  source_tasks: JobSourceTaskResponse[]
}

// -- Assets

export interface AssetResponse {
  id: string
  job_id: string
  source_type: SourceType
  source_item_id: string
  source_url: string | null
  dataset_identifier: string | null
  license: string | null
  width: number | null
  height: number | null
  file_format: string | null
  nas_path: string | null
  annotation_format: AnnotationFormat
  classes: string[]
  status: AssetStatus
  relevance_score: number | null
  discovered_at: string
  promoted_at: string | null
}

// -- Credentials

export interface CredentialCreateRequest {
  source_type: SourceType
  label: string
  key_name: string
  value: string
}

export interface CredentialUpdateRequest {
  label?: string
  value?: string
  is_active?: boolean
}

export interface CredentialResponse {
  id: string
  source_type: SourceType
  label: string
  key_name: string
  masked_value: string
  is_active: boolean
  created_at: string
  last_used_at: string | null
}

// -- Exports

export interface ExportCreateRequest {
  format?: ExportFormat
  filter?: { min_relevance_score?: number }
}

export interface ExportResponse {
  id: string
  job_id: string
  status: ExportJobStatus
  format: ExportFormat
  file_size_bytes: number | null
  asset_count: number | null
  processed_count: number
  requested_at: string
  completed_at: string | null
  error_message: string | null
}

// -- Sources / Stats (plain dicts backend-side, no named schema)

export interface SourceInfo {
  source_type: SourceType
  connector_implemented: boolean
  has_credential: boolean
}

export interface StatsOverview {
  active_jobs: number
  total_jobs: number
  total_assets: number
}

// -- WebSocket event payloads (app/services/progress_service.py publishers)

export interface WsProgressDelta {
  type: 'progress_delta'
  job_source_task_id: string
  discovered?: number
  downloaded?: number
  accepted?: number
  rejected?: number
  errors?: number
  duplicate?: number
}

export interface WsJobStatus {
  type: 'job_status'
  status: JobStatus
  completion_reason?: JobCompletionReason
}

export interface WsConnectorFinished {
  type: 'connector_finished'
}

export interface WsRelevanceFilterComplete {
  type: 'relevance_filter_complete'
  passed: number
  rejected: number
}

export interface WsDedupComplete {
  type: 'dedup_complete'
  duplicate_count: number
}

export interface WsExportProgress {
  type: 'export_progress'
  export_id: string
  processed: number
}

export interface WsExportComplete {
  type: 'export_complete'
  export_id: string
  asset_count: number
}

export interface WsExportFailed {
  type: 'export_failed'
  export_id: string
  error: string
}

export type WsEvent =
  | WsProgressDelta
  | WsJobStatus
  | WsConnectorFinished
  | WsRelevanceFilterComplete
  | WsDedupComplete
  | WsExportProgress
  | WsExportComplete
  | WsExportFailed
