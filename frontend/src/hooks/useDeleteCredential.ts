import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { deleteCredential } from '@/api/credentials'
import { credentialKeys, sourceKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'

export function useDeleteCredential() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: deleteCredential,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: credentialKeys.all })
      queryClient.invalidateQueries({ queryKey: sourceKeys.all })
      toast.success('Credential deleted')
    },
    onError: (error) => {
      const message =
        error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to delete credential'
      toast.error(message)
    },
  })
}
