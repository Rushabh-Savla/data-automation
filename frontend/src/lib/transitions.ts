// Mirrors the backend's legal JobStatus transition table
// (app/domain/state_machine.py) so the UI only ever offers actions the API
// will actually accept — avoids a round-trip just to learn an action is
// illegal via a 409.
import type { JobStatus } from '@/api/types'

export interface JobActions {
  canStart: boolean
  canPause: boolean
  canResume: boolean
  canCancel: boolean
  canEdit: boolean
  canDelete: boolean
}

const TERMINAL: JobStatus[] = ['cancelled', 'completed', 'completed_partial', 'failed']

export function getJobActions(status: JobStatus): JobActions {
  return {
    canStart: status === 'draft' || status === 'scheduled',
    canPause: status === 'discovering' || status === 'running',
    canResume: status === 'paused',
    canCancel:
      status === 'draft' ||
      status === 'scheduled' ||
      status === 'discovering' ||
      status === 'running' ||
      status === 'paused',
    canEdit: status === 'draft' || status === 'scheduled',
    canDelete: status === 'draft' || TERMINAL.includes(status),
  }
}
