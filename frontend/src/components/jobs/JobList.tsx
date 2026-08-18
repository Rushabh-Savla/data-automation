import type { JobResponse } from '@/api/types'
import { Table, TableBody, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Skeleton } from '@/components/ui/skeleton'
import { JobListRow } from '@/components/jobs/JobListRow'

export function JobList({ jobs, isLoading }: { jobs: JobResponse[] | undefined; isLoading: boolean }) {
  if (isLoading) {
    return (
      <div className="flex flex-col gap-2 p-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    )
  }

  if (!jobs || jobs.length === 0) {
    return (
      <div className="flex flex-col items-center gap-1 p-12 text-center text-muted-foreground">
        <p className="font-medium">No jobs yet</p>
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
