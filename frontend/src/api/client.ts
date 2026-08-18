import { API_BASE_URL } from '@/lib/env'
import { getApiKey } from '@/lib/apiKey'

export class ApiError extends Error {
  status: number
  detail: unknown

  constructor(status: number, detail: unknown) {
    super(typeof detail === 'string' ? detail : `Request failed with status ${status}`)
    this.status = status
    this.detail = detail
    this.name = 'ApiError'
  }
}

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PATCH' | 'DELETE'
  body?: unknown
  // Accepts any flat options-bag object (e.g. ListJobsFilters) — named
  // filter interfaces don't carry an explicit index signature, so `object`
  // here (rather than Record<string,X>) avoids forcing every caller to cast.
  // The values are trusted to be string/number/boolean/undefined by
  // convention (enforced by each api/*.ts filter interface's own typing).
  query?: object
}

function buildUrl(path: string, query?: object): string {
  const url = new URL(`${API_BASE_URL}${path}`, window.location.origin)
  if (query) {
    for (const [key, value] of Object.entries(query as Record<string, string | number | boolean | undefined>)) {
      if (value !== undefined) url.searchParams.set(key, String(value))
    }
  }
  return url.pathname + url.search
}

/** JSON request/response helper. Not used for the two blob-streaming
 * endpoints (asset file, export download) — see api/assets.ts / api/exports.ts. */
export async function apiFetch<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { method = 'GET', body, query } = options

  const response = await fetch(buildUrl(path, query), {
    method,
    headers: {
      'X-API-Key': getApiKey(),
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })

  if (!response.ok) {
    let detail: unknown
    try {
      const data = await response.json()
      detail = data.detail ?? data
    } catch {
      detail = await response.text().catch(() => undefined)
    }
    throw new ApiError(response.status, detail)
  }

  if (response.status === 204) return undefined as T
  return (await response.json()) as T
}

/**
 * Fetches a blob-streaming endpoint (asset file, export download) and
 * returns an object URL for it. Every backend route requires the X-API-Key
 * header (confirmed by audit — no exception for file/download endpoints),
 * so a plain `<img src>` or `<a href>` pointing at the API can never work:
 * the browser has no way to attach a custom header to those requests. This
 * is the one correct way to display/download an authenticated file — fetch
 * it with the header, wrap the response in an object URL, and let the
 * caller revoke it (`URL.revokeObjectURL`) when done (see hooks/useAssetImage.ts).
 */
export async function fetchBlobUrl(path: string): Promise<string> {
  const response = await fetch(buildUrl(path), {
    headers: { 'X-API-Key': getApiKey() },
  })
  if (!response.ok) {
    throw new ApiError(response.status, await response.text().catch(() => undefined))
  }
  const blob = await response.blob()
  return URL.createObjectURL(blob)
}

/**
 * Triggers a browser "Save As" download for an authenticated blob endpoint —
 * used for export zip downloads, where a plain `<a href download>` can't
 * carry the required X-API-Key header either.
 */
export async function downloadFile(path: string, filename: string): Promise<void> {
  const objectUrl = await fetchBlobUrl(path)
  const link = document.createElement('a')
  link.href = objectUrl
  link.download = filename
  document.body.appendChild(link)
  link.click()
  link.remove()
  URL.revokeObjectURL(objectUrl)
}
