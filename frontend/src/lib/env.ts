// Fails loud rather than silently hitting a wrong/relative URL in a
// misconfigured production build (see plan §9).
function readEnv(key: string, fallback?: string): string {
  const value = import.meta.env[key] as string | undefined
  if (value !== undefined && value !== '') return value
  if (fallback !== undefined) return fallback
  throw new Error(`Missing required env var: ${key}`)
}

export const API_BASE_URL = readEnv('VITE_API_BASE_URL', '/api/v1')
export const WS_BASE_URL = readEnv('VITE_WS_BASE_URL', `${location.origin.replace(/^http/, 'ws')}/ws/v1`)
// Dev-only convenience; in a real deployment the key should be entered via
// the app (e.g. a login/settings screen), not baked into the build.
export const DEV_API_KEY = (import.meta.env.VITE_API_KEY as string | undefined) || 'dev-local-key'
