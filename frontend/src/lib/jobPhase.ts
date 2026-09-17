import { JOB_SOURCE_TASK_TERMINAL_STATUSES, type JobSourceTaskResponse, type JobStatus } from '@/api/types'
import { sourceTypeLabels } from '@/lib/enumLabels'

// Every source finishing downloading doesn't mean the job is done —
// relevance filtering (YOLO-World + SmolVLM) and dedup still run
// afterward, and can take much longer than the download itself. Both
// phases report the same 'discovering'/'running' job status, so without
// this check the UI looks frozen on "Running" for the entire filtering
// pass even though every per-source row already shows "Completed".
export function isFilteringPhase(status: JobStatus, sourceTasks: JobSourceTaskResponse[] | undefined): boolean {
  if (status !== 'discovering' && status !== 'running') return false
  if (!sourceTasks || sourceTasks.length === 0) return false
  return sourceTasks.every((t) => JOB_SOURCE_TASK_TERMINAL_STATUSES.includes(t.status))
}

const ACTIVITY_VERBS: Partial<Record<JobSourceTaskResponse['status'], string>> = {
  pending: 'queued to start',
  discovering: 'searching for matching datasets',
  downloading: 'downloading files',
  standardizing: 'processing downloaded files',
}

// A plain-English "what's happening right now" sentence built from each
// source's own status — for the common case where the job is actively
// working but not yet in the post-download filtering phase, which
// previously showed nothing beyond the raw status badges (see
// JobPhaseBanner's default branch). Non-technical users reading "Running" /
// "Downloading" have no way to know a per-item stall is normal, or what
// happens once downloading finishes, without this.
export function describeSourceActivity(sourceTasks: JobSourceTaskResponse[]): string {
  const active = sourceTasks.filter(
    (t) => !JOB_SOURCE_TASK_TERMINAL_STATUSES.includes(t.status) && t.status !== 'stalled'
  )
  if (active.length === 0) return 'Wrapping up'
  return active
    .map((t) => `${sourceTypeLabels[t.source_type] ?? t.source_type} is ${ACTIVITY_VERBS[t.status] ?? t.status}`)
    .join('; ')
}
