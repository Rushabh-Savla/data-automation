import { JOB_SOURCE_TASK_TERMINAL_STATUSES, type JobSourceTaskResponse, type JobStatus } from '@/api/types'

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
