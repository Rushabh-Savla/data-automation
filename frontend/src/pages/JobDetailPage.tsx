import { Link, useParams } from 'react-router-dom'

import { Topbar } from '@/components/layout/Topbar'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { ConnectionStatusBadge } from '@/components/layout/ConnectionStatusBadge'
import { JobSummaryCard } from '@/components/jobs/JobSummaryCard'
import { JobSourceTaskTable } from '@/components/jobs/JobSourceTaskTable'
import { JobControlBar } from '@/components/jobs/JobControlBar'
import { JobEventLog } from '@/components/jobs/JobEventLog'
import { DeleteJobDialog } from '@/components/jobs/DeleteJobDialog'
import { EditJobDialog } from '@/components/jobs/EditJobDialog'
import { useJob } from '@/hooks/useJob'
import { useJobProgress } from '@/hooks/useJobProgress'
import { useJobProgressSocket } from '@/hooks/useJobProgressSocket'
import { getJobActions } from '@/lib/transitions'
import { JOB_TERMINAL_STATUSES } from '@/api/types'

export function JobDetailPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const { data: job, isLoading, error } = useJob(jobId)
  const { status: connectionStatus, events } = useJobProgressSocket(jobId)
  // Progress polling is a fallback: disabled while the socket is genuinely
  // connected, re-enabled at a 3s interval whenever it isn't (connecting,
  // reconnecting, or given up in favor of pure polling).
  useJobProgress(jobId, connectionStatus === 'connected' ? false : 3_000)

  return (
    <>
      <Topbar
        title="Job detail"
        right={
          job && (
            <>
              {!JOB_TERMINAL_STATUSES.includes(job.status) && <ConnectionStatusBadge status={connectionStatus} />}
              {getJobActions(job.status).canEdit && <EditJobDialog job={job} />}
              <JobControlBar jobId={job.id} status={job.status} />
              {getJobActions(job.status).canDelete && (
                <DeleteJobDialog jobId={job.id} jobLabel={job.name || job.query} />
              )}
              <Button asChild variant="outline" size="sm">
                <Link to={`/jobs/${job.id}/assets`}>View assets</Link>
              </Button>
            </>
          )
        }
      />

      <div className="flex flex-col gap-4 p-6">
        {isLoading && <Skeleton className="h-40 w-full" />}

        {error && (
          <p className="rounded-md bg-destructive/10 p-4 text-sm text-destructive">
            Failed to load job: {error instanceof Error ? error.message : 'Unknown error'}
          </p>
        )}

        {job && (
          <>
            <JobSummaryCard job={job} />

            <Card>
              <CardHeader>
                <CardTitle>Per-source breakdown</CardTitle>
              </CardHeader>
              <CardContent className="px-0">
                <JobSourceTaskTable sourceTasks={job.source_tasks} />
              </CardContent>
            </Card>

            <JobEventLog events={events} />
          </>
        )}
      </div>
    </>
  )
}
