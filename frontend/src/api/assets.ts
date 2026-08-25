import { apiFetch, downloadFile, fetchBlobUrl } from '@/api/client'
import type { AssetResponse, AssetStatus, PageResponse, SourceType } from '@/api/types'

export interface ListAssetsFilters {
  status?: AssetStatus
  source_type?: SourceType
  search?: string
  limit?: number
  offset?: number
}

export function listAssets(jobId: string, filters: ListAssetsFilters = {}): Promise<PageResponse<AssetResponse>> {
  return apiFetch<PageResponse<AssetResponse>>(`/jobs/${jobId}/assets`, { query: filters })
}

export function getAsset(assetId: string): Promise<AssetResponse> {
  return apiFetch<AssetResponse>(`/assets/${assetId}`)
}

export function fetchAssetImageUrl(assetId: string, variant: 'full' | 'thumbnail' = 'full'): Promise<string> {
  return fetchBlobUrl(`/assets/${assetId}/${variant === 'thumbnail' ? 'thumbnail' : 'file'}`)
}

export function downloadAsset(asset: AssetResponse): Promise<void> {
  return downloadFile(`/assets/${asset.id}/file`, asset.original_name || `${asset.source_item_id}.jpg`)
}

export function deleteAsset(assetId: string): Promise<void> {
  return apiFetch<void>(`/assets/${assetId}`, { method: 'DELETE' })
}
