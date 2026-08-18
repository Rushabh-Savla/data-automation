import { apiFetch } from '@/api/client'
import type { StatsOverview } from '@/api/types'

export function getStatsOverview(): Promise<StatsOverview> {
  return apiFetch<StatsOverview>('/stats/overview')
}
