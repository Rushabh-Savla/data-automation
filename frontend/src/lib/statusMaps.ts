import type { AssetStatus, ExportJobStatus, JobSourceTaskStatus, JobStatus } from '@/api/types'
import type { BadgeProps } from '@/components/ui/badge'

type BadgeVariant = NonNullable<BadgeProps['variant']>

export const jobStatusVariant: Record<JobStatus, BadgeVariant> = {
  draft: 'outline',
  scheduled: 'secondary',
  discovering: 'secondary',
  running: 'default',
  paused: 'warning',
  cancelling: 'warning',
  cancelled: 'outline',
  completed: 'success',
  completed_partial: 'success',
  failed: 'destructive',
}

export const jobSourceTaskStatusVariant: Record<JobSourceTaskStatus, BadgeVariant> = {
  pending: 'outline',
  discovering: 'secondary',
  downloading: 'secondary',
  window_waiting: 'secondary',
  standardizing: 'secondary',
  discovery_completed: 'secondary',
  partially_processed: 'warning',
  waiting_for_download: 'secondary',
  source_exhausted: 'outline',
  completed: 'success',
  completed_partial: 'warning',
  failed: 'destructive',
  cancelled: 'outline',
  stalled: 'warning',
}

export const assetStatusVariant: Record<AssetStatus, BadgeVariant> = {
  discovered: 'outline',
  downloading: 'secondary',
  downloaded: 'secondary',
  standardizing: 'secondary',
  promoted: 'default',
  relevance_pending: 'secondary',
  relevance_passed: 'success',
  relevance_rejected: 'destructive',
  review_pending: 'warning',
  retry_pending: 'warning',
  ready_for_upload: 'secondary',
  uploading: 'secondary',
  nas_verified: 'success',
  completed: 'success',
  duplicate: 'warning',
  failed: 'destructive',
}

export const exportJobStatusVariant: Record<ExportJobStatus, BadgeVariant> = {
  pending: 'outline',
  running: 'secondary',
  completed: 'success',
  failed: 'destructive',
}
