import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { createCredential } from '@/api/credentials'
import { credentialKeys, sourceKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'

export function useCreateCredential() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: createCredential,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: credentialKeys.all })
      // has_credential on GET /sources depends on this, used by the job
      // create form's source picker.
      queryClient.invalidateQueries({ queryKey: sourceKeys.all })
      toast.success('Credential saved')
    },
    onError: (error) => {
      const message = error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to save credential'
      toast.error(message)
    },
  })
}
