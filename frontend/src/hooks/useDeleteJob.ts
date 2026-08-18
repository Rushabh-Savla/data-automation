import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { deleteJob } from '@/api/jobs'
import { jobKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'

export function useDeleteJob() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: deleteJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: jobKeys.lists() })
      toast.success('Job deleted')
    },
    onError: (error) => {
      const message = error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to delete job'
      toast.error(message)
    },
  })
}
