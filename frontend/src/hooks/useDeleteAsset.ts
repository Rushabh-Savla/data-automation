import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { deleteAsset } from '@/api/assets'
import { assetKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'

export function useDeleteAsset() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: deleteAsset,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: assetKeys.all })
      toast.success('Asset deleted')
    },
    onError: (error) => {
      const message = error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to delete asset'
      toast.error(message)
    },
  })
}
