import type { JobDetailResponse } from '@/api/types'
import { Card, CardContent } from '@/components/ui/card'
import { JobStatusBadge } from '@/components/jobs/JobStatusBadge'
import { JobProgressBar } from '@/components/jobs/JobProgressBar'
import { sourceTypeLabels, jobCompletionReasonLabels } from '@/lib/enumLabels'
import { formatDateTime } from '@/lib/dateFormat'

export function JobSummaryCard({ job }: { job: JobDetailResponse }) {
  return (
    <Card>
      <CardContent className="flex flex-col gap-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="flex flex-col gap-1">
            <h2 className="text-lg font-semibold">{job.name || job.query}</h2>
            {job.name && <p className="text-sm text-muted-foreground">{job.query}</p>}
          </div>
          <JobStatusBadge status={job.status} />
        </div>

        <JobProgressBar job={job} />

        <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm sm:grid-cols-4">
          <Field label="Sources" value={job.sources.map((s) => sourceTypeLabels[s as keyof typeof sourceTypeLabels] ?? s).join(', ')} />
          <Field label="Rejected" value={String(job.rejected_count)} />
          <Field label="Duplicates" value={String(job.duplicate_count)} />
          <Field label="Deadline" value={job.deadline_at ? formatDateTime(job.deadline_at) : 'None'} />
          <Field label="Created" value={formatDateTime(job.created_at)} />
          <Field label="Started" value={job.started_at ? formatDateTime(job.started_at) : '—'} />
          <Field label="Completed" value={job.completed_at ? formatDateTime(job.completed_at) : '—'} />
          <Field
            label="Completion reason"
            value={job.completion_reason ? jobCompletionReasonLabels[job.completion_reason] : '—'}
          />
        </div>

        {job.error_message && (
          <p className="rounded-md bg-destructive/10 p-2 text-sm text-destructive">{job.error_message}</p>
        )}
      </CardContent>
    </Card>
  )
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="font-medium">{value}</span>
    </div>
  )
}
