import { useQuery } from '@tanstack/react-query'

import { listAssets, type ListAssetsFilters } from '@/api/assets'
import { assetKeys } from '@/hooks/queryKeys'

export function useAssets(jobId: string | undefined, filters: ListAssetsFilters = {}) {
  return useQuery({
    queryKey: assetKeys.list(jobId ?? '', filters),
    queryFn: () => listAssets(jobId!, filters),
    enabled: !!jobId,
  })
}
