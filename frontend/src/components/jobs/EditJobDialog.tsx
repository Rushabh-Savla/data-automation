import { useState } from 'react'
import { PencilIcon } from 'lucide-react'

import type { JobDetailResponse } from '@/api/types'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useUpdateJob } from '@/hooks/useUpdateJob'

/** Only draft/scheduled jobs are editable (enforced both here via
 * JobControlBar's canEdit gate and server-side — PATCH /jobs/{id} 409s
 * outside those two statuses), so the only fields worth exposing are the
 * ones a user would plausibly want to tweak before committing to Start. */
export function EditJobDialog({ job }: { job: JobDetailResponse }) {
  const [open, setOpen] = useState(false)
  const [name, setName] = useState(job.name ?? '')
  const [targetCount, setTargetCount] = useState(job.target_count?.toString() ?? '')
  const [deadline, setDeadline] = useState(job.deadline_at ? toDatetimeLocal(job.deadline_at) : '')

  const updateJob = useUpdateJob(job.id)

  async function handleSubmit() {
    await updateJob.mutateAsync({
      name: name.trim() ? name.trim() : undefined,
      target_count: targetCount ? Number(targetCount) : undefined,
      deadline_at: deadline ? new Date(deadline).toISOString() : undefined,
    })
    setOpen(false)
  }

  const canSubmit = targetCount.trim() !== '' || deadline.trim() !== ''

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="gap-1.5">
          <PencilIcon className="size-4" /> Edit
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Edit job</DialogTitle>
          <DialogDescription>
            Only available while the job hasn't started yet. Query and sources can't be changed after
            creation — delete and recreate if those need to change.
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <Label>Job name</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="(optional)" />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label>Target image count</Label>
            <Input
              type="number"
              min={1}
              value={targetCount}
              onChange={(e) => setTargetCount(e.target.value)}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label>Deadline</Label>
            <Input type="datetime-local" value={deadline} onChange={(e) => setDeadline(e.target.value)} />
          </div>
          {!canSubmit && (
            <p className="text-xs text-destructive">Keep a target count or a deadline set — at least one is required.</p>
          )}
        </div>

        <DialogFooter>
          <Button onClick={handleSubmit} disabled={!canSubmit || updateJob.isPending}>
            {updateJob.isPending ? 'Saving…' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function toDatetimeLocal(iso: string): string {
  const d = new Date(iso)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
}
