import { useQuery } from '@tanstack/react-query'

import { listSources } from '@/api/sources'
import { sourceKeys } from '@/hooks/queryKeys'

export function useSources() {
  return useQuery({
    queryKey: sourceKeys.all,
    queryFn: listSources,
    staleTime: 5 * 60_000, // rarely changes within a session
  })
}
