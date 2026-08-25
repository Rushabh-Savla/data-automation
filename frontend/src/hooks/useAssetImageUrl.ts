import { useEffect, useState } from 'react'

import { fetchAssetImageUrl } from '@/api/assets'

/**
 * Asset image bytes require the X-API-Key header (every backend route does),
 * so a plain <img src="/api/v1/assets/{id}/file"> can never work — the
 * browser has no way to attach a custom header to that request. This fetches
 * the file with the header and exposes an object URL instead, revoking it on
 * unmount/id-change to avoid leaking blob URLs while browsing a large grid.
 *
 * `variant: 'thumbnail'` fetches the small resized JPEG instead of the
 * original file — used by grid cards, which only ever render the image at
 * thumbnail size (see AssetCard.tsx). `enabled: false` skips fetching
 * entirely, e.g. while a grid card hasn't scrolled near the viewport yet
 * (see useInView.ts) — both options default to today's behavior (full
 * image, always enabled) so existing callers need no changes.
 */
export function useAssetImageUrl(
  assetId: string,
  options: { variant?: 'full' | 'thumbnail'; enabled?: boolean } = {},
): { url: string | null; error: boolean } {
  const { variant = 'full', enabled = true } = options
  const [url, setUrl] = useState<string | null>(null)
  const [error, setError] = useState(false)

  useEffect(() => {
    let cancelled = false
    let objectUrl: string | null = null

    setUrl(null)
    setError(false)

    // Callers (e.g. AssetDetailDialog) may pass '' momentarily while
    // closing (asset prop goes null before the component unmounts) — don't
    // fire a request to the invalid /assets//file URL for it. Likewise,
    // skip entirely while not yet enabled (e.g. a grid card not in view).
    if (!assetId || !enabled) return

    fetchAssetImageUrl(assetId, variant)
      .then((u) => {
        if (cancelled) {
          URL.revokeObjectURL(u)
          return
        }
        objectUrl = u
        setUrl(u)
      })
      .catch(() => {
        if (!cancelled) setError(true)
      })

    return () => {
      cancelled = true
      if (objectUrl) URL.revokeObjectURL(objectUrl)
    }
  }, [assetId, variant, enabled])

  return { url, error }
}
