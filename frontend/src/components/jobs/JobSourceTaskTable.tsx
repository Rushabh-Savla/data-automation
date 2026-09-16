import type { JobSourceTaskResponse } from '@/api/types'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { JobSourceTaskStatusBadge } from '@/components/jobs/JobSourceTaskStatusBadge'
import { sourceTypeLabels } from '@/lib/enumLabels'
import { formatRelative } from '@/lib/dateFormat'

export function JobSourceTaskTable({ sourceTasks }: { sourceTasks: JobSourceTaskResponse[] }) {
  if (sourceTasks.length === 0) {
    return <p className="p-4 text-sm text-muted-foreground">No source tasks yet.</p>
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Source</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Discovered</TableHead>
          <TableHead className="text-right">Downloaded</TableHead>
          <TableHead className="text-right">Accepted</TableHead>
          <TableHead className="text-right">Rejected</TableHead>
          <TableHead className="text-right">Errors</TableHead>
          <TableHead>Last activity</TableHead>
          <TableHead>Error</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {sourceTasks.map((task) => (
          <TableRow key={task.id}>
            <TableCell className="font-medium">{sourceTypeLabels[task.source_type]}</TableCell>
            <TableCell>
              <JobSourceTaskStatusBadge status={task.status} />
            </TableCell>
            <TableCell className="text-right">{task.discovered_count}</TableCell>
            <TableCell className="text-right">{task.downloaded_count}</TableCell>
            <TableCell className="text-right">{task.accepted_count}</TableCell>
            <TableCell className="text-right">{task.rejected_count}</TableCell>
            <TableCell className="text-right">{task.error_count}</TableCell>
            <TableCell className="text-muted-foreground">{formatRelative(task.last_heartbeat_at)}</TableCell>
            <TableCell className="max-w-64 truncate text-destructive">{task.error_message ?? ''}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}
