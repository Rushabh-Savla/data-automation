import { apiFetch } from '@/api/client'
import type {
  NasConfig,
  NasConfigInput,
  NasFoldersResponse,
  NasStatus,
  NasTestResult,
  NasTransferInput,
  NasTransferResult,
  NasVerifyFolderResponse,
} from '@/api/types'

export function getNasStatus(): Promise<NasStatus> {
  return apiFetch<NasStatus>('/nas/status')
}

export function getNasConfig(): Promise<NasConfig> {
  return apiFetch<NasConfig>('/nas/config')
}

export function saveNasConfig(payload: NasConfigInput): Promise<NasConfig> {
  return apiFetch<NasConfig>('/nas/config', { method: 'PUT', body: payload })
}

export function deleteNasConfig(): Promise<void> {
  return apiFetch<void>('/nas/config', { method: 'DELETE' })
}

/** Tests the given (possibly unsaved) values, or tests the saved configuration if omitted.
 * A failed connection comes back as `ok: false` with a message, not as an HTTP error. */
export function testNasConnection(payload?: NasConfigInput): Promise<NasTestResult> {
  return apiFetch<NasTestResult>('/nas/test', { method: 'POST', body: payload })
}

/** Explores directories on the connected NAS in a single connection session. */
export function listNasFolders(path?: string): Promise<NasFoldersResponse> {
  return apiFetch<NasFoldersResponse>('/nas/folders', {
    query: path ? { path } : undefined,
  })
}

/** Verifies whether a specific folder path exists on the NAS in a single connection session. */
export function verifyNasFolder(folderPath: string): Promise<NasVerifyFolderResponse> {
  return apiFetch<NasVerifyFolderResponse>('/nas/verify-folder', {
    method: 'POST',
    body: { folder_path: folderPath },
  })
}

/** Transfers/copies images between NAS and local storage in a single connection session. */
export function transferNasImages(payload: NasTransferInput): Promise<NasTransferResult> {
  return apiFetch<NasTransferResult>('/nas/transfer', { method: 'POST', body: payload })
}
