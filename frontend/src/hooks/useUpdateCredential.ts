import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { updateCredential } from '@/api/credentials'
import { credentialKeys, sourceKeys } from '@/hooks/queryKeys'
import { ApiError } from '@/api/client'
import type { CredentialUpdateRequest } from '@/api/types'

export function useUpdateCredential() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ credentialId, payload }: { credentialId: string; payload: CredentialUpdateRequest }) =>
      updateCredential(credentialId, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: credentialKeys.all })
      queryClient.invalidateQueries({ queryKey: sourceKeys.all })
      toast.success('Credential updated')
    },
    onError: (error) => {
      const message =
        error instanceof ApiError ? String(error.detail ?? error.message) : 'Failed to update credential'
      toast.error(message)
    },
  })
}
