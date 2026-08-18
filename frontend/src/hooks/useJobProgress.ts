import { useQuery } from '@tanstack/react-query'

import { getJobProgress } from '@/api/jobs'
import { jobKeys } from '@/hooks/queryKeys'
import { JOB_TERMINAL_STATUSES } from '@/api/types'

/**
 * REST snapshot of live counts. In Phase 1 this is the only progress
 * mechanism (fixed 3s poll). From Phase 3 onward, useJobProgressSocket
 * takes over as the live source and this hook's poll interval is only
 * re-enabled while the socket isn't connected (see that hook for the
 * exact handoff).
 */
export function useJobProgress(jobId: string | undefined, pollIntervalMs: number | false = 3_000) {
  return useQuery({
    queryKey: jobKeys.progress(jobId ?? ''),
    queryFn: () => getJobProgress(jobId!),
    enabled: !!jobId,
    refetchInterval: (query) => {
      if (pollIntervalMs === false) return false
      const status = query.state.data?.status
      if (status && JOB_TERMINAL_STATUSES.includes(status)) return false
      return pollIntervalMs
    },
  })
}
