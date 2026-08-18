import { useEffect, useState } from 'react'

import { fetchAssetImageUrl } from '@/api/assets'

/**
 * Asset image bytes require the X-API-Key header (every backend route does),
 * so a plain <img src="/api/v1/assets/{id}/file"> can never work — the
 * browser has no way to attach a custom header to that request. This fetches
 * the file with the header and exposes an object URL instead, revoking it on
 * unmount/id-change to avoid leaking blob URLs while browsing a large grid.
 */
export function useAssetImageUrl(assetId: string): { url: string | null; error: boolean } {
  const [url, setUrl] = useState<string | null>(null)
  const [error, setError] = useState(false)

  useEffect(() => {
    let cancelled = false
    let objectUrl: string | null = null

    setUrl(null)
    setError(false)

    // Callers (e.g. AssetDetailDialog) may pass '' momentarily while
    // closing (asset prop goes null before the component unmounts) — don't
    // fire a request to the invalid /assets//file URL for it.
    if (!assetId) return

    fetchAssetImageUrl(assetId)
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
  }, [assetId])

  return { url, error }
}
