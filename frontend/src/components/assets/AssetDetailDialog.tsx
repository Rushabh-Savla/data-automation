import { useState } from 'react'
import { DownloadIcon, Loader2Icon } from 'lucide-react'
import { toast } from 'sonner'

import type { AssetResponse } from '@/api/types'
import { downloadAsset } from '@/api/assets'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useAssetImageUrl } from '@/hooks/useAssetImageUrl'
import { assetStatusVariant } from '@/lib/statusMaps'
import { assetStatusLabels, annotationFormatLabels, sourceTypeLabels } from '@/lib/enumLabels'
import { formatDateTime } from '@/lib/dateFormat'
import { DeleteAssetButton } from '@/components/assets/DeleteAssetButton'

export function AssetDetailDialog({
  asset,
  open,
  onOpenChange,
}: {
  asset: AssetResponse | null
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const { url } = useAssetImageUrl(asset?.id ?? '')
  const [downloading, setDownloading] = useState(false)

  if (!asset) return null

  async function handleDownload() {
    if (!asset) return
    setDownloading(true)
    try {
      await downloadAsset(asset)
    } catch {
      toast.error('Download failed')
    } finally {
      setDownloading(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            {asset.source_item_id}
            <Badge variant={assetStatusVariant[asset.status]}>{assetStatusLabels[asset.status]}</Badge>
          </DialogTitle>
        </DialogHeader>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <div className="flex aspect-square items-center justify-center overflow-hidden rounded-md bg-muted">
            {url ? (
              <img src={url} alt="" className="h-full w-full object-contain" />
            ) : (
              <span className="text-sm text-muted-foreground">Loading…</span>
            )}
          </div>

          <div className="flex flex-col gap-2 text-sm">
            <Field label="Source" value={sourceTypeLabels[asset.source_type]} />
            <Field label="Dataset" value={asset.dataset_identifier ?? '—'} />
            <Field label="License" value={asset.license ?? '—'} />
            <Field label="Resolution" value={asset.width && asset.height ? `${asset.width}×${asset.height}` : '—'} />
            <Field label="Format" value={asset.file_format ?? '—'} />
            <Field label="Annotation" value={annotationFormatLabels[asset.annotation_format]} />
            <Field
              label="Relevance score"
              value={asset.relevance_score !== null ? `${(asset.relevance_score * 100).toFixed(1)}%` : '—'}
            />
            <Field label="Discovered" value={formatDateTime(asset.discovered_at)} />
            <Field label="Promoted" value={asset.promoted_at ? formatDateTime(asset.promoted_at) : '—'} />
            {asset.source_url && (
              <div className="flex flex-col gap-0.5">
                <span className="text-xs text-muted-foreground">Source URL</span>
                <a
                  href={asset.source_url}
                  target="_blank"
                  rel="noreferrer"
                  className="truncate text-primary underline-offset-2 hover:underline"
                >
                  {asset.source_url}
                </a>
              </div>
            )}

            <div className="mt-auto flex items-center gap-2 pt-2">
              <Button type="button" size="sm" variant="outline" className="gap-1.5" disabled={downloading} onClick={handleDownload}>
                {downloading ? <Loader2Icon className="size-4 animate-spin" /> : <DownloadIcon className="size-4" />}
                Download
              </Button>
              <DeleteAssetButton assetId={asset.id} onDeleted={() => onOpenChange(false)} />
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="font-medium">{value}</span>
    </div>
  )
}
