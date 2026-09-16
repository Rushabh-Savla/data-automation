import { type JobSourceTaskStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { jobSourceTaskStatusLabels } from '@/lib/enumLabels'
import { jobSourceTaskStatusVariant } from '@/lib/statusMaps'

/**
 * Shows a source's own status, truthfully.
 *
 * This used to hold `completed`/`completed_partial` sources at "Running"
 * until the whole job went terminal. The concern behind that was real — a
 * source finishing its download doesn't mean the job is done, since relevance
 * filtering and dedup still run afterwards — but the fix misreported the
 * source to say something about the job. In practice it made a genuine
 * failure indistinguishable from work in progress: a job stranded for 17
 * hours, whose YouTube source had actually completed after 8 minutes,
 * displayed "Running" the entire time with no indication anything was wrong.
 *
 * Job-level phase belongs at job level. JobPhaseBanner says what the job is
 * doing now ("Filtering 340 / 754", "Top-up round 2 of 5"); this badge says
 * what this source did.
 */
export function JobSourceTaskStatusBadge({ status }: { status: JobSourceTaskStatus }) {
  return <Badge variant={jobSourceTaskStatusVariant[status]}>{jobSourceTaskStatusLabels[status]}</Badge>
}
