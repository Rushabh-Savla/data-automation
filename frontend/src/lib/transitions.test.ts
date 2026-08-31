import { describe, expect, it } from 'vitest'

import { getJobActions } from '@/lib/transitions'
import type { JobStatus } from '@/api/types'

describe('getJobActions', () => {
  it('draft: can start, edit, delete, cancel — not pause/resume', () => {
    const actions = getJobActions('draft')
    expect(actions).toEqual({
      canStart: true,
      canPause: false,
      canResume: false,
      canCancel: true,
      canEdit: true,
      canDelete: true,
    })
  })

  it('running: can pause and cancel — not start/resume/edit/delete', () => {
    const actions = getJobActions('running')
    expect(actions.canPause).toBe(true)
    expect(actions.canCancel).toBe(true)
    expect(actions.canStart).toBe(false)
    expect(actions.canResume).toBe(false)
    expect(actions.canEdit).toBe(false)
    expect(actions.canDelete).toBe(false)
  })

  it('paused: can resume and cancel — not pause again', () => {
    const actions = getJobActions('paused')
    expect(actions.canResume).toBe(true)
    expect(actions.canCancel).toBe(true)
    expect(actions.canPause).toBe(false)
  })

  it.each<JobStatus>(['completed', 'completed_partial', 'failed', 'cancelled'])(
    'terminal status %s: only delete is available',
    (status) => {
      const actions = getJobActions(status)
      expect(actions.canDelete).toBe(true)
      expect(actions.canStart).toBe(false)
      expect(actions.canPause).toBe(false)
      expect(actions.canResume).toBe(false)
      expect(actions.canCancel).toBe(false)
      expect(actions.canEdit).toBe(false)
    },
  )

  it('cancelling: no actions offered — the backend only accepts the cancel event from active states, not from cancelling itself', () => {
    const actions = getJobActions('cancelling')
    expect(actions.canCancel).toBe(false)
    expect(actions.canStart).toBe(false)
    expect(actions.canPause).toBe(false)
    expect(actions.canResume).toBe(false)
    expect(actions.canDelete).toBe(false)
  })
})
