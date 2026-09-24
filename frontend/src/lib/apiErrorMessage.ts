import { ApiError } from '@/api/client'

/** A readable message for a failed API call. FastAPI reports validation
 * failures (422) as a list of `{ loc, msg, ... }` objects and other errors as a
 * plain string, so `String(error.detail)` would show "[object Object]". */
export function apiErrorMessage(error: unknown, fallback: string): string {
  if (!(error instanceof ApiError)) return fallback
  const { detail } = error
  if (typeof detail === 'string' && detail) return detail
  if (Array.isArray(detail)) {
    const messages = detail
      .map((item) => (item && typeof item === 'object' && 'msg' in item ? String(item.msg) : ''))
      .map((message) => message.replace(/^Value error, /, ''))
      .filter(Boolean)
    if (messages.length) return messages.join('. ')
  }
  return error.message || fallback
}
