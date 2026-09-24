import { useQuery } from '@tanstack/react-query'

import { getNasConfig } from '@/api/nas'
import { nasKeys } from '@/hooks/queryKeys'

export function useNasConfig() {
  return useQuery({
    queryKey: nasKeys.config(),
    queryFn: getNasConfig,
  })
}
