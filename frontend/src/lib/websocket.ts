import { WS_BASE_URL } from '@/lib/env'
import { getApiKey } from '@/lib/apiKey'

export function buildJobWsUrl(jobId: string): string {
  const url = new URL(`${WS_BASE_URL}/jobs/${jobId}`, window.location.origin)
  url.searchParams.set('api_key', getApiKey())
  // Resolve relative WS_BASE_URL (e.g. "/ws") against the page's own
  // ws(s):// scheme, matching the dev-proxy / prod-nginx same-origin setup.
  if (url.protocol === 'http:') url.protocol = 'ws:'
  if (url.protocol === 'https:') url.protocol = 'wss:'
  return url.toString()
}

const MAX_RECONNECT_ATTEMPTS = 5
const BASE_DELAY_MS = 500
const MAX_DELAY_MS = 30_000

/** Exponential backoff with jitter, capped at 30s. Returns null once the
 * attempt count exceeds the retry budget — the caller should give up and
 * fall back to REST polling at that point. */
export function reconnectDelayMs(attempt: number): number | null {
  if (attempt > MAX_RECONNECT_ATTEMPTS) return null
  const exp = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** (attempt - 1))
  return exp + Math.random() * 300
}
