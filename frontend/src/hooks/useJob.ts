import { useQuery } from '@tanstack/react-query'

import { getJob } from '@/api/jobs'
import { jobKeys } from '@/hooks/queryKeys'
import { JOB_TERMINAL_STATUSES } from '@/api/types'

export function useJob(jobId: string | undefined) {
  return useQuery({
    queryKey: jobKeys.detail(jobId ?? ''),
    queryFn: () => getJob(jobId!),
    enabled: !!jobId,
    // Safety-net poll in case the WebSocket layer (Phase 3) isn't mounted
    // or connected — stops once the job is terminal.
    refetchInterval: (query) => {
      const status = query.state.data?.status
      if (status && JOB_TERMINAL_STATUSES.includes(status)) return false
      return 5_000
    },
  })
}
