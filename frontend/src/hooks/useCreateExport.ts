import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { createExport } from '@/api/exports'
import { exportKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'
import type { ExportCreateRequest } from '@/api/types'

export function useCreateExport(jobId: string) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (payload: ExportCreateRequest) => createExport(jobId, payload),
    onSuccess: (exportJob) => {
      queryClient.setQueryData(exportKeys.detail(exportJob.id), exportJob)
      toast.success('Export started')
    },
    onError: (error) => {
      const message = error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to start export'
      toast.error(message)
    },
  })
}
