import { act, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { useInView } from '@/hooks/useInView'

class MockIntersectionObserver {
  static instances: MockIntersectionObserver[] = []
  callback: IntersectionObserverCallback
  options: IntersectionObserverInit | undefined
  disconnected = false

  constructor(callback: IntersectionObserverCallback, options?: IntersectionObserverInit) {
    this.callback = callback
    this.options = options
    MockIntersectionObserver.instances.push(this)
  }

  observe() {}
  unobserve() {}

  disconnect() {
    this.disconnected = true
  }

  triggerIntersecting(isIntersecting: boolean) {
    this.callback([{ isIntersecting } as IntersectionObserverEntry], this as unknown as IntersectionObserver)
  }
}

function Probe({ rootMargin }: { rootMargin?: string }) {
  const [ref, inView] = useInView<HTMLDivElement>(rootMargin)
  return <div ref={ref} data-testid="probe" data-in-view={inView} />
}

describe('useInView', () => {
  beforeEach(() => {
    MockIntersectionObserver.instances = []
    vi.stubGlobal('IntersectionObserver', MockIntersectionObserver)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  function latestObserver() {
    return MockIntersectionObserver.instances[MockIntersectionObserver.instances.length - 1]
  }

  it('starts false', () => {
    render(<Probe />)
    expect(screen.getByTestId('probe')).toHaveAttribute('data-in-view', 'false')
  })

  it('flips true once the observer reports intersecting', () => {
    render(<Probe />)
    act(() => {
      latestObserver().triggerIntersecting(true)
    })
    expect(screen.getByTestId('probe')).toHaveAttribute('data-in-view', 'true')
  })

  it('stays false if the observer reports not intersecting', () => {
    render(<Probe />)
    act(() => {
      latestObserver().triggerIntersecting(false)
    })
    expect(screen.getByTestId('probe')).toHaveAttribute('data-in-view', 'false')
  })

  it('disconnects the observer after first becoming visible', () => {
    render(<Probe />)
    const observer = latestObserver()
    act(() => {
      observer.triggerIntersecting(true)
    })
    expect(observer.disconnected).toBe(true)
  })

  it('passes rootMargin through to the observer options', () => {
    render(<Probe rootMargin="50px" />)
    expect(latestObserver().options?.rootMargin).toBe('50px')
  })

  it('defaults rootMargin to 200px', () => {
    render(<Probe />)
    expect(latestObserver().options?.rootMargin).toBe('200px')
  })
})
