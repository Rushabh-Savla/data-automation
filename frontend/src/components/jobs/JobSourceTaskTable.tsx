import type { JobSourceTaskResponse } from '@/api/types'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { JobSourceTaskStatusBadge } from '@/components/jobs/JobSourceTaskStatusBadge'
import { sourceTypeLabels } from '@/lib/enumLabels'
import { formatRelative } from '@/lib/dateFormat'

export function JobSourceTaskTable({
  sourceTasks,
  jobStatus,
}: {
  sourceTasks: JobSourceTaskResponse[]
  jobStatus?: string
}) {
  if (sourceTasks.length === 0) {
    return <p className="p-4 text-sm text-muted-foreground">No source tasks yet.</p>
  }

  const isJobDone = jobStatus === 'completed' || jobStatus === 'completed_partial'

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Source</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Discovered</TableHead>
          <TableHead className="text-right">Inventoried</TableHead>
          <TableHead className="text-right">Pending</TableHead>
          <TableHead className="text-right">Downloaded</TableHead>
          <TableHead className="text-right">Accepted</TableHead>
          <TableHead className="text-right">Progress</TableHead>
          <TableHead>Last activity</TableHead>
          <TableHead>Error</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {sourceTasks.map((task) => {
          const isActive = !isJobDone && task.is_active
          const displayStatus =
            isJobDone && task.status !== 'failed' && task.status !== 'cancelled' ? 'completed' : task.status
          const displayLabel =
            isJobDone && task.status !== 'failed' && task.status !== 'cancelled' ? 'Completed' : task.status_label
          const displayExplanation = isJobDone ? undefined : task.status_explanation
          const displayError =
            isJobDone || task.status === 'completed' || task.status === 'completed_partial'
              ? ''
              : (task.error_message ?? '')

          return (
            <TableRow key={task.id} className={isActive ? 'bg-primary/5 font-medium' : undefined}>
              <TableCell className="font-medium">
                <div className="flex items-center gap-1.5">
                  {isActive && (
                    <span className="relative flex h-2 w-2" title="Active Source">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-primary"></span>
                    </span>
                  )}
                  <span>{sourceTypeLabels[task.source_type]}</span>
                </div>
              </TableCell>
              <TableCell>
                <JobSourceTaskStatusBadge
                  status={displayStatus}
                  label={displayLabel}
                  explanation={displayExplanation}
                />
              </TableCell>
              <TableCell className="text-right">
                {task.discovered_count}
                {task.unit_label && task.unit_label !== 'images' && (
                  <span className="text-xs text-muted-foreground ml-1">{task.unit_label}</span>
                )}
              </TableCell>
              <TableCell className="text-right">
                {task.total_inventoried ?? task.discovered_count}
              </TableCell>
              <TableCell className="text-right">
                {task.pending_count ?? 0}
                {task.unit_label && task.unit_label !== 'images' && (
                  <span className="text-xs text-muted-foreground ml-1">{task.unit_label}</span>
                )}
              </TableCell>
              <TableCell className="text-right">{task.downloaded_count}</TableCell>
              <TableCell className="text-right">{task.accepted_count}</TableCell>
              <TableCell className="text-right">
                {task.completion_percentage != null ? `${task.completion_percentage}%` : '—'}
              </TableCell>
              <TableCell className="text-muted-foreground">{formatRelative(task.last_heartbeat_at)}</TableCell>
              <TableCell className="max-w-64 truncate text-destructive">
                {displayError}
              </TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}
