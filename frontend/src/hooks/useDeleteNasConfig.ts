import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { deleteNasConfig } from '@/api/nas'
import { nasKeys, sourceKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'

export function useDeleteNasConfig() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: deleteNasConfig,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: nasKeys.all })
      queryClient.invalidateQueries({ queryKey: sourceKeys.all })
      toast.success('NAS connection removed')
    },
    onError: (error) => {
      const message =
        error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to remove NAS connection'
      toast.error(message)
    },
  })
}
