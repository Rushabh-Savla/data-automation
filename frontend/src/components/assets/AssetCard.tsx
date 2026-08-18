import { ImageOffIcon } from 'lucide-react'

import type { AssetResponse } from '@/api/types'
import { useAssetImageUrl } from '@/hooks/useAssetImageUrl'
import { Badge } from '@/components/ui/badge'
import { assetStatusVariant } from '@/lib/statusMaps'
import { assetStatusLabels, sourceTypeLabels } from '@/lib/enumLabels'
import { Skeleton } from '@/components/ui/skeleton'

export function AssetCard({ asset, onClick }: { asset: AssetResponse; onClick: () => void }) {
  const { url, error } = useAssetImageUrl(asset.id)

  return (
    <button
      type="button"
      onClick={onClick}
      className="group flex flex-col overflow-hidden rounded-lg border text-left transition-shadow hover:shadow-md"
    >
      <div className="relative aspect-square w-full overflow-hidden bg-muted">
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
        <Badge variant={assetStatusVariant[asset.status]} className="absolute top-2 right-2 shadow-sm">
          {assetStatusLabels[asset.status]}
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
  )
}
