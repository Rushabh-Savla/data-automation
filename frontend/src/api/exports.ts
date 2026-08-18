import { apiFetch, downloadFile } from '@/api/client'
import type { ExportCreateRequest, ExportResponse } from '@/api/types'

export function createExport(jobId: string, payload: ExportCreateRequest): Promise<ExportResponse> {
  return apiFetch<ExportResponse>(`/jobs/${jobId}/export`, { method: 'POST', body: payload })
}

export function getExport(exportId: string): Promise<ExportResponse> {
  return apiFetch<ExportResponse>(`/exports/${exportId}`)
}

export function downloadExport(exportId: string, format: string, jobId: string): Promise<void> {
  return downloadFile(`/exports/${exportId}/download`, `export_${jobId}_${format}.zip`)
}
