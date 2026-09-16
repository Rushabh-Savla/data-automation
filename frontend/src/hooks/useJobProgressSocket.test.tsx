import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'

import { useJobProgressSocket } from '@/hooks/useJobProgressSocket'
import { jobKeys } from '@/hooks/queryKeys'
import type { JobDetailResponse, JobProgressResponse } from '@/api/types'

class MockWebSocket {
  static instances: MockWebSocket[] = []
  url: string
  onopen: (() => void) | null = null
  onmessage: ((e: { data: string }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  closeCalled = false

  constructor(url: string) {
    this.url = url
    MockWebSocket.instances.push(this)
  }

  send() {}

  close() {
    this.closeCalled = true
    this.onclose?.()
  }

  triggerOpen() {
    this.onopen?.()
  }

  triggerMessage(payload: unknown) {
    this.onmessage?.({ data: JSON.stringify(payload) })
  }
}

const JOB_ID = 'job-1'
const TASK_ID = 'task-1'

function seedProgress(queryClient: QueryClient): JobProgressResponse {
  const progress: JobProgressResponse = {
    job_id: JOB_ID,
    status: 'running',
    target_count: 100,
    accepted_count: 10,
    rejected_count: 2,
    duplicate_count: 0,
    discovered_count: 20,
    downloaded_count: 15,
    source_tasks: [
      {
        id: TASK_ID,
        source_type: 'kaggle',
        status: 'downloading',
        discovered_count: 20,
        downloaded_count: 15,
        accepted_count: 10,
        rejected_count: 2,
        error_count: 0,
        error_message: null,
        last_heartbeat_at: null,
      },
    ],
  }
  queryClient.setQueryData(jobKeys.progress(JOB_ID), progress)
  return progress
}

function seedDetail(queryClient: QueryClient): JobDetailResponse {
  const detail: JobDetailResponse = {
    id: JOB_ID,
    name: null,
    query: 'test',
    sources: ['kaggle'],
    target_count: 100,
    filtering_in_progress: false,
    topup_round: null,
    deadline_at: null,
    scheduled_start_at: null,
    status: 'running',
    completion_reason: null,
    discovered_count: 20,
    downloaded_count: 15,
    accepted_count: 10,
    rejected_count: 2,
    duplicate_count: 0,
    error_message: null,
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
    started_at: null,
    completed_at: null,
    source_tasks: seedProgress(queryClient).source_tasks,
  }
  queryClient.setQueryData(jobKeys.detail(JOB_ID), detail)
  return detail
}

describe('useJobProgressSocket', () => {
  beforeEach(() => {
    MockWebSocket.instances = []
    vi.stubGlobal('WebSocket', MockWebSocket)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  function setup() {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    seedDetail(queryClient)

    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    )

    const view = renderHook(() => useJobProgressSocket(JOB_ID), { wrapper })
    return { queryClient, view }
  }

  it('additively merges progress_delta onto both the progress and detail caches', async () => {
    const { queryClient, view } = setup()

    await act(async () => {
      MockWebSocket.instances[0].triggerOpen()
    })
    await waitFor(() => expect(view.result.current.status).toBe('connected'))

    act(() => {
      MockWebSocket.instances[0].triggerMessage({
        type: 'progress_delta',
        job_source_task_id: TASK_ID,
        downloaded: 3,
        accepted: 2,
        errors: 1,
      })
    })

    const progress = queryClient.getQueryData<JobProgressResponse>(jobKeys.progress(JOB_ID))!
    expect(progress.downloaded_count).toBe(18) // 15 + 3
    expect(progress.accepted_count).toBe(12) // 10 + 2
    expect(progress.source_tasks[0].error_count).toBe(1) // WS `errors` -> REST `error_count`

    const detail = queryClient.getQueryData<JobDetailResponse>(jobKeys.detail(JOB_ID))!
    expect(detail.downloaded_count).toBe(18)
    expect(detail.source_tasks[0].downloaded_count).toBe(18)
  })

  it('does not hand-merge relevance_filter_complete — it invalidates instead', async () => {
    const { queryClient, view } = setup()
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

    await act(async () => {
      MockWebSocket.instances[0].triggerOpen()
    })
    await waitFor(() => expect(view.result.current.status).toBe('connected'))
    invalidateSpy.mockClear() // drop the on-open re-sync invalidate

    act(() => {
      MockWebSocket.instances[0].triggerMessage({ type: 'relevance_filter_complete', passed: 8, rejected: 2 })
    })

    expect(invalidateSpy).toHaveBeenCalledWith(
      expect.objectContaining({ queryKey: jobKeys.progress(JOB_ID) }),
    )
    expect(invalidateSpy).toHaveBeenCalledWith(
      expect.objectContaining({ queryKey: jobKeys.detail(JOB_ID) }),
    )

    // Crucially: accepted_count in the cache is untouched by this handler —
    // only a fresh REST fetch (not exercised here) may change it, per the
    // backend's direct-DB-mutation-with-no-progress_delta behavior.
    const progress = queryClient.getQueryData<JobProgressResponse>(jobKeys.progress(JOB_ID))!
    expect(progress.accepted_count).toBe(10)
  })

  it('closes the socket intentionally on a terminal job_status and does not reconnect', async () => {
    const { view } = setup()

    await act(async () => {
      MockWebSocket.instances[0].triggerOpen()
    })
    await waitFor(() => expect(view.result.current.status).toBe('connected'))

    act(() => {
      MockWebSocket.instances[0].triggerMessage({
        type: 'job_status',
        status: 'completed',
        completion_reason: 'target_count_reached',
      })
    })

    expect(MockWebSocket.instances[0].closeCalled).toBe(true)
    // Only the one instance was ever created — no reconnect attempt followed.
    expect(MockWebSocket.instances.length).toBe(1)
  })
})
