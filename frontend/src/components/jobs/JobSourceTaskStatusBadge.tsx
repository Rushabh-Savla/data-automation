import { JOB_TERMINAL_STATUSES, type JobSourceTaskStatus, type JobStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { jobSourceTaskStatusLabels } from '@/lib/enumLabels'
import { jobSourceTaskStatusVariant } from '@/lib/statusMaps'

// Only a *successful* source finish gets held back — a failed or cancelled
// source is a real, final outcome for that source regardless of what the
// rest of the job's pipeline is doing, so those keep showing immediately.
const HOLDABLE_STATUSES: JobSourceTaskStatus[] = ['completed', 'completed_partial']

export function JobSourceTaskStatusBadge({
  status,
  jobStatus,
}: {
  status: JobSourceTaskStatus
  // A source finishing its download doesn't mean the job is done —
  // relevance filtering and dedup still run afterward, keyed off the job
  // as a whole rather than per source. Showing this source as green
  // "Completed" while that's still in flight reads as more final than it
  // is, so it stays "Running" until the job itself reaches a terminal
  // status.
  jobStatus?: JobStatus
}) {
  const holdAsRunning =
    jobStatus !== undefined && !JOB_TERMINAL_STATUSES.includes(jobStatus) && HOLDABLE_STATUSES.includes(status)

  if (holdAsRunning) {
    return <Badge variant="default">Running</Badge>
  }
  return <Badge variant={jobSourceTaskStatusVariant[status]}>{jobSourceTaskStatusLabels[status]}</Badge>
}
