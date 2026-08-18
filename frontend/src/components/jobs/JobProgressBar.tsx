import { Progress } from '@/components/ui/progress'
import type { JobResponse } from '@/api/types'

export function JobProgressBar({ job }: { job: Pick<JobResponse, 'accepted_count' | 'target_count'> }) {
  if (job.target_count) {
    const pct = Math.min(100, (job.accepted_count / job.target_count) * 100)
    return (
      <div className="flex flex-col gap-1">
        <Progress value={pct} />
        <span className="text-xs text-muted-foreground">
          {job.accepted_count} / {job.target_count} accepted
        </span>
      </div>
    )
  }
  return <span className="text-xs text-muted-foreground">{job.accepted_count} accepted so far</span>
}
