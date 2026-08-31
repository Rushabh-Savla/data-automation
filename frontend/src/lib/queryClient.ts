import { QueryClient } from '@tanstack/react-query'
import { ApiError } from '@/api/client'

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (failureCount, error) => {
        // Don't retry client errors (404/409/422/401) — only transient
        // network/5xx failures are worth a retry.
        if (error instanceof ApiError && error.status < 500) return false
        return failureCount < 2
      },
      staleTime: 10_000,
      refetchOnWindowFocus: false,
    },
    mutations: {
      retry: false,
    },
  },
})
