import { useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { jobKeys, exportKeys, assetKeys } from '@/hooks/queryKeys'
import { buildJobWsUrl, reconnectDelayMs } from '@/lib/websocket'
import { JOB_TERMINAL_STATUSES, type JobDetailResponse, type JobProgressResponse, type WsEvent } from '@/api/types'
import type { ConnectionStatus } from '@/components/layout/ConnectionStatusBadge'
import { jobCompletionReasonLabels, jobStatusLabels } from '@/lib/enumLabels'
import { randomId } from '@/lib/utils'

export interface JobLogEvent {
  id: string
  at: string
  message: string
}

const MAX_LOG_EVENTS = 50

/**
 * Keeps jobKeys.progress(jobId)/jobKeys.detail(jobId) cache entries live via
 * the job's WebSocket, imperatively patching the query cache rather than
 * returning data itself — mount once per job-detail view, alongside the
 * plain useJob/useJobProgress queries that actually own the data.
 *
 * REST-first is required, not optional: the backend's "send a snapshot on
 * connect" path is dead code today (nothing ever publishes a
 * progress_snapshot event), so a client that only opened the socket would
 * see nothing until the next delta. useJob/useJobProgress firing on mount
 * (before this hook's effect runs) is what actually bootstraps state.
 */
export interface JobProgressSocketState {
  status: ConnectionStatus
  events: JobLogEvent[]
}

export function useJobProgressSocket(jobId: string | undefined): JobProgressSocketState {
  const queryClient = useQueryClient()
  const [status, setStatus] = useState<ConnectionStatus>('connecting')
  const [events, setEvents] = useState<JobLogEvent[]>([])

  function logEvent(message: string) {
    setEvents((prev) => [{ id: randomId(), at: new Date().toISOString(), message }, ...prev].slice(0, MAX_LOG_EVENTS))
  }

  const wsRef = useRef<WebSocket | null>(null)
  const attemptRef = useRef(0)
  const reconnectTimerRef = useRef<number | null>(null)
  const intentionalCloseRef = useRef(false)

  useEffect(() => {
    if (!jobId) return
    let cancelled = false

    function isJobTerminal(): boolean {
      const job = queryClient.getQueryData<JobDetailResponse>(jobKeys.detail(jobId!))
      return !!job && JOB_TERMINAL_STATUSES.includes(job.status)
    }

    function connect() {
      if (cancelled || isJobTerminal()) return

      const ws = new WebSocket(buildJobWsUrl(jobId!))
      wsRef.current = ws
      intentionalCloseRef.current = false

      ws.onopen = () => {
        if (cancelled) return
        attemptRef.current = 0
        setStatus('connected')
        // Closes the race window between the initial REST fetch and the
        // socket's actual server-side subscription — any delta published
        // in that narrow window is simply superseded by this fresher fetch.
        queryClient.invalidateQueries({ queryKey: jobKeys.progress(jobId!) })
      }

      ws.onmessage = (event) => {
        if (cancelled) return
        try {
          handleMessage(JSON.parse(event.data) as WsEvent)
        } catch {
          // Malformed frame — ignore, don't take down the connection over it.
        }
      }

      ws.onclose = () => {
        if (cancelled || intentionalCloseRef.current) return
        scheduleReconnect()
      }

      ws.onerror = () => {
        ws.close()
      }
    }

    function scheduleReconnect() {
      attemptRef.current += 1
      const delay = reconnectDelayMs(attemptRef.current)
      if (delay === null) {
        setStatus('polling-fallback')
        return
      }
      setStatus('reconnecting')
      reconnectTimerRef.current = window.setTimeout(connect, delay)
    }

    function patchProgress(patch: Partial<JobProgressResponse>) {
      queryClient.setQueryData<JobProgressResponse>(jobKeys.progress(jobId!), (old) =>
        old ? { ...old, ...patch } : old,
      )
    }

    function patchDetail(patch: Partial<JobDetailResponse>) {
      queryClient.setQueryData<JobDetailResponse>(jobKeys.detail(jobId!), (old) =>
        old ? { ...old, ...patch } : old,
      )
    }

    function handleMessage(msg: WsEvent) {
      switch (msg.type) {
        case 'progress_delta': {
          // Additive merge — these are increments (job_repo.increment_counts
          // on the backend), not absolute values. Note the field rename:
          // the WS delta key is `errors`, the REST field is `error_count`.
          const d = msg
          queryClient.setQueryData<JobProgressResponse>(jobKeys.progress(jobId!), (old) => {
            if (!old) return old
            return {
              ...old,
              discovered_count: old.discovered_count + (d.discovered ?? 0),
              downloaded_count: old.downloaded_count + (d.downloaded ?? 0),
              accepted_count: old.accepted_count + (d.accepted ?? 0),
              rejected_count: old.rejected_count + (d.rejected ?? 0),
              duplicate_count: old.duplicate_count + (d.duplicate ?? 0),
              source_tasks: old.source_tasks.map((t) =>
                t.id === d.job_source_task_id
                  ? {
                      ...t,
                      discovered_count: t.discovered_count + (d.discovered ?? 0),
                      downloaded_count: t.downloaded_count + (d.downloaded ?? 0),
                      accepted_count: t.accepted_count + (d.accepted ?? 0),
                      rejected_count: t.rejected_count + (d.rejected ?? 0),
                      error_count: t.error_count + (d.errors ?? 0),
                    }
                  : t,
              ),
            }
          })
          queryClient.setQueryData<JobDetailResponse>(jobKeys.detail(jobId!), (old) => {
            if (!old) return old
            return {
              ...old,
              discovered_count: old.discovered_count + (d.discovered ?? 0),
              downloaded_count: old.downloaded_count + (d.downloaded ?? 0),
              accepted_count: old.accepted_count + (d.accepted ?? 0),
              rejected_count: old.rejected_count + (d.rejected ?? 0),
              duplicate_count: old.duplicate_count + (d.duplicate ?? 0),
              source_tasks: old.source_tasks.map((t) =>
                t.id === d.job_source_task_id
                  ? {
                      ...t,
                      discovered_count: t.discovered_count + (d.discovered ?? 0),
                      downloaded_count: t.downloaded_count + (d.downloaded ?? 0),
                      accepted_count: t.accepted_count + (d.accepted ?? 0),
                      rejected_count: t.rejected_count + (d.rejected ?? 0),
                      error_count: t.error_count + (d.errors ?? 0),
                    }
                  : t,
              ),
            }
          })
          break
        }

        case 'relevance_filter_complete': {
          // Never hand-merged arithmetically: the backend mutates
          // accepted_count/rejected_count directly here with no
          // corresponding progress_delta, so only a REST re-fetch is
          // authoritative.
          queryClient.invalidateQueries({ queryKey: jobKeys.progress(jobId!) })
          queryClient.invalidateQueries({ queryKey: jobKeys.detail(jobId!) })
          toast.info(`Relevance filter: ${msg.passed} passed, ${msg.rejected} rejected`)
          logEvent(`Relevance filter complete: ${msg.passed} passed, ${msg.rejected} rejected`)
          break
        }

        case 'dedup_complete': {
          queryClient.invalidateQueries({ queryKey: jobKeys.progress(jobId!) })
          queryClient.invalidateQueries({ queryKey: jobKeys.detail(jobId!) })
          if (msg.duplicate_count > 0) toast.info(`Dedup: ${msg.duplicate_count} duplicate(s) removed`)
          logEvent(`Deduplication complete: ${msg.duplicate_count} duplicate(s) found`)
          break
        }

        case 'job_status': {
          patchDetail({ status: msg.status, completion_reason: msg.completion_reason ?? undefined })
          patchProgress({ status: msg.status })
          logEvent(`Status changed to ${jobStatusLabels[msg.status]}`)

          if (JOB_TERMINAL_STATUSES.includes(msg.status)) {
            queryClient.invalidateQueries({ queryKey: assetKeys.all })
            const reasonLabel = msg.completion_reason ? ` (${jobCompletionReasonLabels[msg.completion_reason]})` : ''
            toast[msg.status === 'failed' ? 'error' : 'success'](`Job ${jobStatusLabels[msg.status]}${reasonLabel}`)
            // No more events coming — close intentionally rather than
            // reconnect-looping against a job that's already done.
            intentionalCloseRef.current = true
            wsRef.current?.close()
            setStatus('connected')
          }
          break
        }

        case 'connector_finished': {
          // No fields — just "all sources reached terminal, safe to refetch."
          queryClient.invalidateQueries({ queryKey: jobKeys.detail(jobId!) })
          logEvent('All sources finished — running relevance filtering and deduplication')
          break
        }

        case 'export_progress': {
          queryClient.setQueryData(exportKeys.detail(msg.export_id), (old: unknown) =>
            old && typeof old === 'object' ? { ...old, processed_count: msg.processed } : old,
          )
          break
        }

        case 'export_complete': {
          // Only invalidate (don't hand-patch status here): the payload
          // carries asset_count but not file_size_bytes/completed_at/etc.,
          // and a partial patch that flips status to 'completed' would
          // short-circuit useExportProgress's poll (it stops polling once
          // status is terminal) before those other fields ever get fetched
          // — same class of bug as relevance_filter_complete/dedup_complete
          // above, just for the export cache instead of the job cache.
          queryClient.invalidateQueries({ queryKey: exportKeys.detail(msg.export_id) })
          toast.success(`Export ready (${msg.asset_count} assets)`)
          break
        }

        case 'export_failed': {
          queryClient.invalidateQueries({ queryKey: exportKeys.detail(msg.export_id) })
          toast.error(`Export failed: ${msg.error}`)
          break
        }
      }
    }

    connect()

    return () => {
      cancelled = true
      intentionalCloseRef.current = true
      if (reconnectTimerRef.current) window.clearTimeout(reconnectTimerRef.current)
      wsRef.current?.close()
    }
  }, [jobId, queryClient])

  return { status, events }
}
