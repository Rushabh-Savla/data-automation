import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { cancelJob, pauseJob, resumeJob, startJob } from '@/api/jobs'
import { jobKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'
import type { JobDetailResponse } from '@/api/types'

function useJobAction(jobId: string, action: (id: string) => Promise<JobDetailResponse>, successMessage: string) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: () => action(jobId),
    // Optimistic status patch — the button should feel instant, the
    // mutation's own response (or a 409) reconciles it a moment later.
    onMutate: async (): Promise<{ previous: JobDetailResponse | undefined }> => {
      await queryClient.cancelQueries({ queryKey: jobKeys.detail(jobId) })
      const previous = queryClient.getQueryData<JobDetailResponse>(jobKeys.detail(jobId))
      return { previous }
    },
    onSuccess: (job) => {
      queryClient.setQueryData(jobKeys.detail(jobId), job)
      queryClient.invalidateQueries({ queryKey: jobKeys.lists() })
      toast.success(successMessage)
    },
    onError: (error, _vars, context) => {
      if (context?.previous) queryClient.setQueryData(jobKeys.detail(jobId), context.previous)
      const message =
        error instanceof ApiError ? String(error.detail ?? error.message) : 'Action failed'
      toast.error(message)
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: jobKeys.detail(jobId) })
      queryClient.invalidateQueries({ queryKey: jobKeys.progress(jobId) })
    },
  })
}

export function useJobControl(jobId: string) {
  const start = useJobAction(jobId, startJob, 'Job started')
  const pause = useJobAction(jobId, pauseJob, 'Job paused')
  const resume = useJobAction(jobId, resumeJob, 'Job resumed')
  const cancel = useJobAction(jobId, cancelJob, 'Job cancelled')

  return { start, pause, resume, cancel }
}
