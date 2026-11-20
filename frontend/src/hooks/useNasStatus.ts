import { useQuery } from '@tanstack/react-query'

import { getNasStatus } from '@/api/nas'
import { nasKeys } from '@/hooks/queryKeys'

export function useNasStatus() {
  return useQuery({
    queryKey: nasKeys.status(),
    queryFn: getNasStatus,
    refetchInterval: 15_000,
    staleTime: 10_000,
    refetchIntervalInBackground: false,
  })
}
