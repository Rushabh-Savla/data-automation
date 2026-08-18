import { useNavigate } from 'react-router-dom'

import type { JobResponse } from '@/api/types'
import { TableCell, TableRow } from '@/components/ui/table'
import { JobStatusBadge } from '@/components/jobs/JobStatusBadge'
import { JobProgressBar } from '@/components/jobs/JobProgressBar'
import { sourceTypeLabels } from '@/lib/enumLabels'
import { formatRelative } from '@/lib/dateFormat'

export function JobListRow({ job }: { job: JobResponse }) {
  const navigate = useNavigate()
  return (
    <TableRow className="cursor-pointer" onClick={() => navigate(`/jobs/${job.id}`)}>
      <TableCell className="max-w-72 truncate font-medium">{job.name || job.query}</TableCell>
      <TableCell>
        <JobStatusBadge status={job.status} />
      </TableCell>
      <TableCell className="text-muted-foreground">
        {job.sources.map((s) => sourceTypeLabels[s as keyof typeof sourceTypeLabels] ?? s).join(', ')}
      </TableCell>
      <TableCell>
        <JobProgressBar job={job} />
      </TableCell>
      <TableCell className="text-muted-foreground">{formatRelative(job.created_at)}</TableCell>
    </TableRow>
  )
}
