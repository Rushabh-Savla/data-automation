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
import { useDeleteCredential } from '@/hooks/useDeleteCredential'

export function DeleteCredentialDialog({ credentialId, label }: { credentialId: string; label: string }) {
  const deleteCredential = useDeleteCredential()

  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="ghost" size="icon" className="text-destructive hover:text-destructive">
          <TrashIcon className="size-4" />
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete credential</DialogTitle>
          <DialogDescription>
            Delete "{label}"? Connectors relying on it will fall back to any .env-configured key, or degrade
            to no results for that source.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <DialogClose asChild>
            <Button variant="outline">Cancel</Button>
          </DialogClose>
          <Button
            variant="destructive"
            disabled={deleteCredential.isPending}
            onClick={() => deleteCredential.mutate(credentialId)}
          >
            Delete
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
