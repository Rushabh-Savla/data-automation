import { useQuery } from '@tanstack/react-query'

import { listJobs, type ListJobsFilters } from '@/api/jobs'
import { jobKeys } from '@/hooks/queryKeys'

export function useJobs(filters: ListJobsFilters = {}) {
  return useQuery({
    queryKey: jobKeys.list(filters),
    queryFn: () => listJobs(filters),
  })
}
