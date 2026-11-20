import type { FilteringProgress } from '@/hooks/useJobProgressSocket'
import { JOB_ACTIVE_STATUSES, type JobDetailResponse } from '@/api/types'
import { describeSourceActivity, isFilteringPhase } from '@/lib/jobPhase'
import { formatRelative } from '@/lib/dateFormat'
import { sourceTypeLabels } from '@/lib/enumLabels'

/**
 * Says what the job is doing *right now*, at job level.
 *
 * The per-source table can only describe per-source work, so the stages that
 * run after every source finishes — relevance filtering, dedup, top-up —
 * previously had nowhere to appear. The page just sat on "Running". This is
 * the honest place for that information, and it replaces the old workaround
 * of misreporting finished sources as still running.
 *
 * It also surfaces a stall. A job whose sources are all finished but which
 * has not moved for a long time is not healthy, and showing it as merely
 * "Running" is how one sat unnoticed for 17 hours.
 */

// Matches filtering_stall_threshold_s on the backend, which is how long the
// watchdog waits before re-driving a job in this state. Flagging it at the
// same point means the UI says "stuck" exactly when recovery is due.
const STALL_THRESHOLD_MS = 30 * 60 * 1000

function isStalled(job: JobDetailResponse): boolean {
  if (!isFilteringPhase(job.status, job.source_tasks)) return false
  const updated = Date.parse(job.updated_at)
  return Number.isFinite(updated) && Date.now() - updated > STALL_THRESHOLD_MS
}

export function JobPhaseBanner({
  job,
  filtering,
}: {
  job: JobDetailResponse
  filtering: FilteringProgress | null
}) {

  if (isStalled(job)) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm">
        <span className="font-medium">No progress since {formatRelative(job.updated_at)}.</span>{' '}
        Every source has finished but the job has not advanced. The watchdog retries automatically.
      </div>
    )
  }

  if (filtering && filtering.total > 0) {
    const pct = Math.min(100, Math.round((filtering.done / filtering.total) * 100))
    return (
      <div className="rounded-md border bg-muted/40 px-4 py-3 text-sm">
        <div className="flex items-center justify-between">
          <span className="font-medium">Filtering for relevance</span>
          <span className="tabular-nums text-muted-foreground">
            {filtering.done} / {filtering.total}
          </span>
        </div>
        <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-muted">
          <div className="h-full rounded-full bg-foreground transition-all" style={{ width: `${pct}%` }} />
        </div>
      </div>
    )
  }

  // topup_round is left on the job's config after the round that set it
  // finishes (see backend RCA), so it must not be trusted on its own —
  // without the active-status check, a finished job keeps showing "still
  // collecting" forever.
  if (job.topup_round && job.topup_round > 1 && JOB_ACTIVE_STATUSES.includes(job.status)) {
    const isTargetMet = job.target_count != null && job.accepted_count >= job.target_count
    if (!isTargetMet) {
      const remaining = job.target_count != null ? Math.max(0, job.target_count - job.accepted_count) : null
      return (
        <div className="rounded-md border bg-muted/40 px-4 py-3 text-sm">
          <span className="font-medium">Collecting more data</span> — replenishment round {job.topup_round}
          {remaining != null ? ` (${remaining} images remaining of ${job.target_count} target)` : ''}.
        </div>
      )
    }
  }

  if (isFilteringPhase(job.status, job.source_tasks)) {
    return (
      <div className="rounded-md border bg-muted/40 px-4 py-3 text-sm">
        <span className="font-medium">Processing</span> — all sources have finished downloading; filtering
        and deduplication are running.
      </div>
    )
  }

  // Strict Sequential Execution banner
  if (JOB_ACTIVE_STATUSES.includes(job.status) && job.active_source) {
    const activeLabel = sourceTypeLabels[job.active_source as keyof typeof sourceTypeLabels] ?? job.active_source
    const nextLabel = job.next_source_in_queue
      ? (sourceTypeLabels[job.next_source_in_queue as keyof typeof sourceTypeLabels] ?? job.next_source_in_queue)
      : null

    return (
      <div className="rounded-md border bg-muted/40 px-4 py-3 text-sm flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="font-medium text-foreground">
            Strict Sequential Ingestion &bull; Round {job.current_round ?? 1}
          </span>
          <span className="tabular-nums text-xs font-semibold px-2 py-0.5 rounded bg-primary/10 text-primary">
            Source {job.active_source_position ?? '1/10'}
          </span>
        </div>
        <p className="text-xs text-muted-foreground">
          Active source: <strong className="text-foreground">{activeLabel}</strong>
          {nextLabel && <span> &bull; Next in queue: <span className="font-medium text-foreground">{nextLabel}</span></span>}
          {job.sequential_waiting_reason && <span> &bull; Status: {job.sequential_waiting_reason}</span>}
        </p>
      </div>
    )
  }

  // Default "actively working" case — the job is discovering/downloading
  // and hasn't reached any of the phases above yet. This used to render
  // nothing at all beyond the raw status badges, leaving a non-technical
  // reader with no idea what's happening or what comes next.
  if (job.status === 'discovering' || job.status === 'running') {
    const overTarget = job.target_count != null && job.accepted_count > job.target_count
    return (
      <div className="rounded-md border bg-muted/40 px-4 py-3 text-sm">
        <span className="font-medium">{describeSourceActivity(job.source_tasks)}.</span>{' '}
        Next: once every source finishes, results are automatically filtered for relevance, checked for
        duplicates, and compared against your target.
        {overTarget && (
          <>
            {' '}
            Target reached ({job.accepted_count} of {job.target_count}) — candidate admission stopped; finalizing verification and transfer.
          </>
        )}
      </div>
    )
  }

  return null
}
