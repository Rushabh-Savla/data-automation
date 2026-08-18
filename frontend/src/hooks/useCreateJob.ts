import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { createJob } from '@/api/jobs'
import { jobKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'

export function useCreateJob() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: createJob,
    onSuccess: (job) => {
      queryClient.invalidateQueries({ queryKey: jobKeys.lists() })
      toast.success('Job created')
      return job
    },
    onError: (error) => {
      const message = error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to create job'
      toast.error(message)
    },
  })
}
