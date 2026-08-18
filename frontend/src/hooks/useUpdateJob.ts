import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { updateJob } from '@/api/jobs'
import { jobKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'
import type { JobUpdateRequest } from '@/api/types'

export function useUpdateJob(jobId: string) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (payload: JobUpdateRequest) => updateJob(jobId, payload),
    onSuccess: (job) => {
      queryClient.setQueryData(jobKeys.detail(jobId), job)
      queryClient.invalidateQueries({ queryKey: jobKeys.lists() })
      toast.success('Job updated')
    },
    onError: (error) => {
      const message = error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to update job'
      toast.error(message)
    },
  })
}
