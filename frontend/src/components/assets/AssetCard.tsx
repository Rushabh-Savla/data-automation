import { useState } from 'react'
import { DownloadIcon, ImageOffIcon, Loader2Icon } from 'lucide-react'
import { toast } from 'sonner'

import type { AssetResponse } from '@/api/types'
import { downloadAsset } from '@/api/assets'
import { useAssetImageUrl } from '@/hooks/useAssetImageUrl'
import { useInView } from '@/hooks/useInView'
import { Badge } from '@/components/ui/badge'
import { assetStatusVariant } from '@/lib/statusMaps'
import { assetStatusLabels, sourceTypeLabels } from '@/lib/enumLabels'
import { Skeleton } from '@/components/ui/skeleton'

export function AssetCard({ asset, onClick }: { asset: AssetResponse; onClick: () => void }) {
  const [imgContainerRef, inView] = useInView<HTMLDivElement>()
  const { url, error } = useAssetImageUrl(asset.id, { variant: 'thumbnail', enabled: inView })
  const [downloading, setDownloading] = useState(false)

  async function handleDownload(e: React.MouseEvent) {
    e.stopPropagation()
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
    <div className="group relative flex flex-col overflow-hidden rounded-lg border text-left transition-shadow hover:shadow-md">
      <button type="button" onClick={onClick} className="contents">
        <div ref={imgContainerRef} className="relative aspect-square w-full overflow-hidden bg-muted">
          {!url && !error && <Skeleton className="h-full w-full rounded-none" />}
          {error && (
            <div className="flex h-full w-full items-center justify-center text-muted-foreground">
              <ImageOffIcon className="size-6" />
            </div>
          )}
          {url && (
            <img
              src={url}
              alt={asset.source_item_id}
              className="h-full w-full object-cover transition-transform group-hover:scale-105"
            />
          )}
          <Badge variant={assetStatusVariant[asset.status] ?? 'secondary'} className="absolute top-2 right-2 shadow-sm">
            {assetStatusLabels[asset.status] ?? asset.status}
          </Badge>
        </div>
        <div className="flex flex-col gap-0.5 p-2 text-xs">
          <span className="truncate font-medium">{sourceTypeLabels[asset.source_type]}</span>
          <span className="truncate text-muted-foreground">
            {asset.width && asset.height ? `${asset.width}×${asset.height}` : '—'}
            {asset.relevance_score !== null && ` · ${(asset.relevance_score * 100).toFixed(0)}%`}
          </span>
        </div>
      </button>

      <button
        type="button"
        onClick={handleDownload}
        disabled={downloading || !url}
        title="Download"
        className="absolute bottom-14 right-2 flex size-7 items-center justify-center rounded-full bg-background/90 opacity-0 shadow-sm transition-opacity group-hover:opacity-100 hover:bg-accent disabled:opacity-50"
      >
        {downloading ? <Loader2Icon className="size-3.5 animate-spin" /> : <DownloadIcon className="size-3.5" />}
      </button>
    </div>
  )
}
