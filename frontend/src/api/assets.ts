import { apiFetch, fetchBlobUrl } from '@/api/client'
import type { AssetResponse, AssetStatus, SourceType } from '@/api/types'

export interface ListAssetsFilters {
  status?: AssetStatus
  source_type?: SourceType
  limit?: number
  offset?: number
}

export function listAssets(jobId: string, filters: ListAssetsFilters = {}): Promise<AssetResponse[]> {
  return apiFetch<AssetResponse[]>(`/jobs/${jobId}/assets`, { query: filters })
}

export function getAsset(assetId: string): Promise<AssetResponse> {
  return apiFetch<AssetResponse>(`/assets/${assetId}`)
}

export function fetchAssetImageUrl(assetId: string): Promise<string> {
  return fetchBlobUrl(`/assets/${assetId}/file`)
}

export function deleteAsset(assetId: string): Promise<void> {
  return apiFetch<void>(`/assets/${assetId}`, { method: 'DELETE' })
}
