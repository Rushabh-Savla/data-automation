import { apiFetch } from '@/api/client'
import type { NasConfig, NasConfigInput, NasTestResult } from '@/api/types'

export function getNasConfig(): Promise<NasConfig> {
  return apiFetch<NasConfig>('/nas/config')
}

export function saveNasConfig(payload: NasConfigInput): Promise<NasConfig> {
  return apiFetch<NasConfig>('/nas/config', { method: 'PUT', body: payload })
}

export function deleteNasConfig(): Promise<void> {
  return apiFetch<void>('/nas/config', { method: 'DELETE' })
}

/** Tests the given (possibly unsaved) values. A failed connection comes back
 * as `ok: false` with a message, not as an HTTP error. */
export function testNasConnection(payload: NasConfigInput): Promise<NasTestResult> {
  return apiFetch<NasTestResult>('/nas/test', { method: 'POST', body: payload })
}
