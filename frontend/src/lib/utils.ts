import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * crypto.randomUUID() only exists in secure contexts (HTTPS or localhost) —
 * accessing the app over plain HTTP via a LAN/hostname address (e.g.
 * http://<host>:3000) makes `crypto.randomUUID` undefined and throws
 * "crypto.randomUUID is not a function". Falls back to crypto.getRandomValues
 * (available in more contexts) and finally to Math.random for callers that
 * only need a unique-enough client-side id (e.g. React list keys), not a
 * cryptographically strong one.
 */
export function randomId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    const bytes = crypto.getRandomValues(new Uint8Array(16))
    bytes[6] = (bytes[6] & 0x0f) | 0x40
    bytes[8] = (bytes[8] & 0x3f) | 0x80
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
  }
  return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`
}
