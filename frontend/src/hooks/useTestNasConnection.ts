import { useMutation } from '@tanstack/react-query'
import { toast } from 'sonner'

import { testNasConnection } from '@/api/nas'
import { apiErrorMessage } from '@/lib/apiErrorMessage'

// No success/failure toast for a completed test: ok:false is a normal result
// that the form renders inline. Only transport/validation errors toast.
export function useTestNasConnection() {
  return useMutation({
    mutationFn: testNasConnection,
    onError: (error) => {
      toast.error(apiErrorMessage(error, 'Failed to test connection'))
    },
  })
}
