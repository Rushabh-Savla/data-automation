import type { ListAssetsFilters } from '@/api/assets'
import type { ListJobsFilters } from '@/api/jobs'

export const jobKeys = {
  all: ['jobs'] as const,
  lists: () => [...jobKeys.all, 'list'] as const,
  list: (filters: ListJobsFilters) => [...jobKeys.lists(), filters] as const,
  details: () => [...jobKeys.all, 'detail'] as const,
  detail: (jobId: string) => [...jobKeys.details(), jobId] as const,
  progress: (jobId: string) => [...jobKeys.all, 'progress', jobId] as const,
}

export const assetKeys = {
  all: ['assets'] as const,
  list: (jobId: string, filters: ListAssetsFilters) => [...assetKeys.all, jobId, 'list', filters] as const,
  detail: (assetId: string) => [...assetKeys.all, 'detail', assetId] as const,
}

export const credentialKeys = {
  all: ['credentials'] as const,
  list: (sourceType?: string) => [...credentialKeys.all, 'list', sourceType] as const,
}

export const exportKeys = {
  all: ['exports'] as const,
  detail: (exportId: string) => [...exportKeys.all, 'detail', exportId] as const,
}

export const sourceKeys = {
  all: ['sources'] as const,
}

export const statsKeys = {
  overview: ['stats', 'overview'] as const,
}
