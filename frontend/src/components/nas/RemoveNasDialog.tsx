import { useState } from 'react'
import { TrashIcon } from 'lucide-react'

import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { useDeleteNasConfig } from '@/hooks/useDeleteNasConfig'

export function RemoveNasDialog() {
  const [open, setOpen] = useState(false)
  const deleteNasConfig = useDeleteNasConfig()

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button type="button" variant="ghost" className="gap-1.5 text-destructive hover:text-destructive">
          <TrashIcon className="size-4" /> Remove connection
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Remove NAS connection</DialogTitle>
          <DialogDescription>
            Forget the saved NAS connection and its password? Files on the NAS are not deleted, but images
            already stored there will not be viewable until a connection is saved again, new images will go to
            local storage, and the NAS source will no longer be available to jobs.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <DialogClose asChild>
            <Button variant="outline">Cancel</Button>
          </DialogClose>
          <Button
            variant="destructive"
            disabled={deleteNasConfig.isPending}
            onClick={() => deleteNasConfig.mutate(undefined, { onSuccess: () => setOpen(false) })}
          >
            Remove
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
