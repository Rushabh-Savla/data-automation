import type { JobStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { jobStatusLabels } from '@/lib/enumLabels'
import { jobStatusVariant } from '@/lib/statusMaps'

export function JobStatusBadge({ status }: { status: JobStatus }) {
  return <Badge variant={jobStatusVariant[status]}>{jobStatusLabels[status]}</Badge>
}
