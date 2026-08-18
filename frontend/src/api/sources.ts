import { apiFetch } from '@/api/client'
import type { SourceInfo } from '@/api/types'

export function listSources(): Promise<SourceInfo[]> {
  return apiFetch<SourceInfo[]>('/sources')
}
