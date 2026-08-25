import { useEffect, useRef, useState } from 'react'
import type { RefObject } from 'react'

/** Tracks whether the returned ref's element has scrolled near the
 * viewport — used to defer expensive work (e.g. fetching an asset
 * thumbnail) until a card is actually about to be seen, instead of doing
 * it for every card the moment a page renders. `rootMargin` pre-loads
 * slightly before the element is on-frame. Stops observing once it's
 * been seen once — the caller doesn't need to know if it scrolls away
 * again, only whether it's ever been in view. */
export function useInView<T extends Element>(rootMargin = '200px'): [RefObject<T | null>, boolean] {
  const ref = useRef<T | null>(null)
  const [inView, setInView] = useState(false)

  useEffect(() => {
    const el = ref.current
    if (!el || inView) return

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setInView(true)
          observer.disconnect()
        }
      },
      { rootMargin },
    )
    observer.observe(el)

    return () => observer.disconnect()
  }, [rootMargin, inView])

  return [ref, inView]
}
