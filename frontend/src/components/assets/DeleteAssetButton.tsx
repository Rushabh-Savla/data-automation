import { TrashIcon } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useDeleteAsset } from '@/hooks/useDeleteAsset'

export function DeleteAssetButton({ assetId, onDeleted }: { assetId: string; onDeleted?: () => void }) {
  const deleteAsset = useDeleteAsset()

  return (
    <Button
      variant="outline"
      size="sm"
      className="gap-1.5 text-destructive hover:text-destructive"
      disabled={deleteAsset.isPending}
      onClick={() => {
        if (!confirm('Delete this asset? This cannot be undone.')) return
        deleteAsset.mutate(assetId, { onSuccess: onDeleted })
      }}
    >
      <TrashIcon className="size-4" /> Delete
    </Button>
  )
}
