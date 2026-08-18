import { useState } from 'react'

import type { AssetResponse } from '@/api/types'
import { Skeleton } from '@/components/ui/skeleton'
import { AssetCard } from '@/components/assets/AssetCard'
import { AssetDetailDialog } from '@/components/assets/AssetDetailDialog'

export function AssetGrid({ assets, isLoading }: { assets: AssetResponse[] | undefined; isLoading: boolean }) {
  const [selected, setSelected] = useState<AssetResponse | null>(null)

  if (isLoading) {
    return (
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6">
        {Array.from({ length: 12 }).map((_, i) => (
          <Skeleton key={i} className="aspect-square w-full" />
        ))}
      </div>
    )
  }

  if (!assets || assets.length === 0) {
    return (
      <div className="flex flex-col items-center gap-1 p-12 text-center text-muted-foreground">
        <p className="font-medium">No assets yet</p>
        <p className="text-sm">Assets will appear here once the job starts downloading.</p>
      </div>
    )
  }

  return (
    <>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6">
        {assets.map((asset) => (
          <AssetCard key={asset.id} asset={asset} onClick={() => setSelected(asset)} />
        ))}
      </div>
      <AssetDetailDialog asset={selected} open={!!selected} onOpenChange={(open) => !open && setSelected(null)} />
    </>
  )
}
