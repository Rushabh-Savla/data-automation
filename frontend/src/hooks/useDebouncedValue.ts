import { useEffect, useState } from 'react'

/** Delays reflecting `value` until it's stopped changing for `delayMs` —
 * used to avoid firing a search request on every keystroke. */
export function useDebouncedValue<T>(value: T, delayMs = 300): T {
  const [debounced, setDebounced] = useState(value)

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs)
    return () => clearTimeout(timer)
  }, [value, delayMs])

  return debounced
}
