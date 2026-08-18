import { PauseIcon, PlayIcon, XIcon } from 'lucide-react'

import type { JobStatus } from '@/api/types'
import { Button } from '@/components/ui/button'
import { useJobControl } from '@/hooks/useJobControl'
import { getJobActions } from '@/lib/transitions'

export function JobControlBar({ jobId, status }: { jobId: string; status: JobStatus }) {
  const actions = getJobActions(status)
  const { start, pause, resume, cancel } = useJobControl(jobId)

  if (!actions.canStart && !actions.canPause && !actions.canResume && !actions.canCancel) {
    return null
  }

  return (
    <div className="flex gap-2">
      {actions.canStart && (
        <Button size="sm" className="gap-1.5" onClick={() => start.mutate()} disabled={start.isPending}>
          <PlayIcon className="size-4" /> Start
        </Button>
      )}
      {actions.canPause && (
        <Button
          size="sm"
          variant="outline"
          className="gap-1.5"
          onClick={() => pause.mutate()}
          disabled={pause.isPending}
        >
          <PauseIcon className="size-4" /> Pause
        </Button>
      )}
      {actions.canResume && (
        <Button size="sm" className="gap-1.5" onClick={() => resume.mutate()} disabled={resume.isPending}>
          <PlayIcon className="size-4" /> Resume
        </Button>
      )}
      {actions.canCancel && (
        <Button
          size="sm"
          variant="destructive"
          className="gap-1.5"
          onClick={() => {
            if (confirm('Cancel this job? In-flight work will be stopped.')) cancel.mutate()
          }}
          disabled={cancel.isPending}
        >
          <XIcon className="size-4" /> Cancel
        </Button>
      )}
    </div>
  )
}
