// The backend uses a single shared API key (no per-user accounts), so the
// simplest correct thing to do here is store it in localStorage, seeded
// from a dev-only env var, with a small settings affordance to change it —
// not a partial/fake login screen, and not hardcoding it into the bundle.
import { DEV_API_KEY } from '@/lib/env'

const STORAGE_KEY = 'data-automation.api-key'

export function getApiKey(): string {
  return localStorage.getItem(STORAGE_KEY) ?? DEV_API_KEY
}

export function setApiKey(key: string): void {
  localStorage.setItem(STORAGE_KEY, key)
}
