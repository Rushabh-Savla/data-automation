import { apiFetch } from '@/api/client'
import type {
  CredentialCreateRequest,
  CredentialResponse,
  CredentialUpdateRequest,
  SourceType,
} from '@/api/types'

export function listCredentials(sourceType?: SourceType): Promise<CredentialResponse[]> {
  return apiFetch<CredentialResponse[]>('/credentials', { query: { source_type: sourceType } })
}

export function createCredential(payload: CredentialCreateRequest): Promise<CredentialResponse> {
  return apiFetch<CredentialResponse>('/credentials', { method: 'POST', body: payload })
}

export function updateCredential(
  credentialId: string,
  payload: CredentialUpdateRequest,
): Promise<CredentialResponse> {
  return apiFetch<CredentialResponse>(`/credentials/${credentialId}`, { method: 'PATCH', body: payload })
}

export function deleteCredential(credentialId: string): Promise<void> {
  return apiFetch<void>(`/credentials/${credentialId}`, { method: 'DELETE' })
}
