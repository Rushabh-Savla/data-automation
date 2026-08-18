import { useQuery } from '@tanstack/react-query'

import { listCredentials } from '@/api/credentials'
import { credentialKeys } from '@/hooks/queryKeys'
import type { SourceType } from '@/api/types'

export function useCredentials(sourceType?: SourceType) {
  return useQuery({
    queryKey: credentialKeys.list(sourceType),
    queryFn: () => listCredentials(sourceType),
  })
}
