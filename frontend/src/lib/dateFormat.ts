import { format, formatDistanceToNow, isValid, parseISO } from 'date-fns'

function toDate(value: string | null | undefined): Date | null {
  if (!value) return null
  const d = parseISO(value)
  return isValid(d) ? d : null
}

export function formatDateTime(value: string | null | undefined): string {
  const d = toDate(value)
  return d ? format(d, 'MMM d, yyyy HH:mm') : '—'
}

export function formatRelative(value: string | null | undefined): string {
  const d = toDate(value)
  return d ? formatDistanceToNow(d, { addSuffix: true }) : '—'
}
