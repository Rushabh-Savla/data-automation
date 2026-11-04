import type { JobResponse } from '@/api/types'
import { Table, TableBody, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Skeleton } from '@/components/ui/skeleton'
import { Button } from '@/components/ui/button'
import { JobListRow } from '@/components/jobs/JobListRow'
import { apiErrorMessage } from '@/lib/apiErrorMessage'
import { AlertCircle, RotateCcw } from 'lucide-react'

export interface JobListProps {
  jobs: JobResponse[] | undefined
  isLoading: boolean
  error?: unknown
  onRetry?: () => void
}

export function JobList({ jobs, isLoading, error, onRetry }: JobListProps) {
  if (isLoading) {
    return (
      <div className="flex flex-col gap-2 p-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    )
  }

  if (error) {
    const message = apiErrorMessage(error, 'Failed to load jobs')
    return (
      <div className="flex flex-col items-center justify-center gap-3 p-8 text-center text-destructive">
        <AlertCircle className="h-8 w-8 text-destructive" />
        <p className="font-medium">{message}</p>
        {onRetry && (
          <Button variant="outline" size="sm" onClick={onRetry} className="mt-2 text-foreground">
            <RotateCcw className="mr-2 h-4 w-4" />
            Retry
          </Button>
        )}
      </div>
    )
  }

  if (!jobs || jobs.length === 0) {
    return (
      <div className="flex flex-col items-center gap-1 p-12 text-center text-muted-foreground">
        <p className="font-medium">No jobs found</p>
        <p className="text-sm">Create a job to start collecting a dataset.</p>
      </div>
    )
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Job</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Sources</TableHead>
          <TableHead>Progress</TableHead>
          <TableHead>Created</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {jobs.map((job) => (
          <JobListRow key={job.id} job={job} />
        ))}
      </TableBody>
    </Table>
  )
}
