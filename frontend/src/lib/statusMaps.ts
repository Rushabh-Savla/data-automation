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
  standardizing: 'secondary',
  completed: 'success',
  completed_partial: 'success',
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
  duplicate: 'warning',
  failed: 'destructive',
}

export const exportJobStatusVariant: Record<ExportJobStatus, BadgeVariant> = {
  pending: 'outline',
  running: 'secondary',
  completed: 'success',
  failed: 'destructive',
}
