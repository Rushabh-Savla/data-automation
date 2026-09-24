import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { saveNasConfig } from '@/api/nas'
import { nasKeys, sourceKeys } from '@/hooks/queryKeys'
import { apiErrorMessage } from '@/lib/apiErrorMessage'

export function useSaveNasConfig() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: saveNasConfig,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: nasKeys.all })
      // has_credential on GET /sources reflects whether a NAS is configured,
      // used by the job create form's source picker.
      queryClient.invalidateQueries({ queryKey: sourceKeys.all })
      toast.success('NAS connection saved')
    },
    onError: (error) => {
      toast.error(apiErrorMessage(error, 'Failed to save NAS connection'))
    },
  })
}
