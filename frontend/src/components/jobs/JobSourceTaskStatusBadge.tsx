import type { JobSourceTaskStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { jobSourceTaskStatusLabels } from '@/lib/enumLabels'
import { jobSourceTaskStatusVariant } from '@/lib/statusMaps'

export function JobSourceTaskStatusBadge({ status }: { status: JobSourceTaskStatus }) {
  return <Badge variant={jobSourceTaskStatusVariant[status]}>{jobSourceTaskStatusLabels[status]}</Badge>
}
