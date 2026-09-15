import type { JobSourceTaskResponse, JobStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { jobStatusLabels } from '@/lib/enumLabels'
import { isFilteringPhase } from '@/lib/jobPhase'
import { jobStatusVariant } from '@/lib/statusMaps'

export function JobStatusBadge({
  status,
  sourceTasks,
}: {
  status: JobStatus
  sourceTasks?: JobSourceTaskResponse[]
}) {
  if (isFilteringPhase(status, sourceTasks)) {
    return <Badge variant="secondary">Filtering</Badge>
  }
  return <Badge variant={jobStatusVariant[status]}>{jobStatusLabels[status]}</Badge>
}
