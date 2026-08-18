import { apiFetch } from '@/api/client'
import type {
  JobCreateRequest,
  JobDetailResponse,
  JobProgressResponse,
  JobResponse,
  JobStatus,
  JobUpdateRequest,
  PageResponse,
} from '@/api/types'

export interface ListJobsFilters {
  status?: JobStatus
  source?: string
  search?: string
  limit?: number
  offset?: number
}

export function listJobs(filters: ListJobsFilters = {}): Promise<PageResponse<JobResponse>> {
  return apiFetch<PageResponse<JobResponse>>('/jobs', { query: filters })
}

export function getJob(jobId: string): Promise<JobDetailResponse> {
  return apiFetch<JobDetailResponse>(`/jobs/${jobId}`)
}

export function getJobProgress(jobId: string): Promise<JobProgressResponse> {
  return apiFetch<JobProgressResponse>(`/jobs/${jobId}/progress`)
}

export function createJob(payload: JobCreateRequest): Promise<JobDetailResponse> {
  return apiFetch<JobDetailResponse>('/jobs', { method: 'POST', body: payload })
}

export function updateJob(jobId: string, payload: JobUpdateRequest): Promise<JobDetailResponse> {
  return apiFetch<JobDetailResponse>(`/jobs/${jobId}`, { method: 'PATCH', body: payload })
}

export function deleteJob(jobId: string): Promise<void> {
  return apiFetch<void>(`/jobs/${jobId}`, { method: 'DELETE' })
}

export function startJob(jobId: string): Promise<JobDetailResponse> {
  return apiFetch<JobDetailResponse>(`/jobs/${jobId}/start`, { method: 'POST' })
}

export function pauseJob(jobId: string): Promise<JobDetailResponse> {
  return apiFetch<JobDetailResponse>(`/jobs/${jobId}/pause`, { method: 'POST' })
}

export function resumeJob(jobId: string): Promise<JobDetailResponse> {
  return apiFetch<JobDetailResponse>(`/jobs/${jobId}/resume`, { method: 'POST' })
}

export function cancelJob(jobId: string): Promise<JobDetailResponse> {
  return apiFetch<JobDetailResponse>(`/jobs/${jobId}/cancel`, { method: 'POST' })
}
