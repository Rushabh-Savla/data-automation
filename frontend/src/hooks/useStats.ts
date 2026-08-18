import { useQuery } from '@tanstack/react-query'

import { getStatsOverview } from '@/api/stats'
import { statsKeys } from '@/hooks/queryKeys'

export function useStats() {
  return useQuery({
    queryKey: statsKeys.overview,
    queryFn: getStatsOverview,
    refetchInterval: 15_000,
  })
}
