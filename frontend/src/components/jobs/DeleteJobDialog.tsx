import { useNavigate } from 'react-router-dom'
import { TrashIcon } from 'lucide-react'

import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogClose,
} from '@/components/ui/dialog'
import { useDeleteJob } from '@/hooks/useDeleteJob'

export function DeleteJobDialog({ jobId, jobLabel }: { jobId: string; jobLabel: string }) {
  const navigate = useNavigate()
  const deleteJob = useDeleteJob()

  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="gap-1.5 text-destructive hover:text-destructive">
          <TrashIcon className="size-4" /> Delete
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete job</DialogTitle>
          <DialogDescription>
            Delete "{jobLabel}"? This cannot be undone. Its downloaded assets on disk are not removed.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <DialogClose asChild>
            <Button variant="outline">Cancel</Button>
          </DialogClose>
          <Button
            variant="destructive"
            disabled={deleteJob.isPending}
            onClick={() =>
              deleteJob.mutate(jobId, {
                onSuccess: () => navigate('/jobs'),
              })
            }
          >
            Delete
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
