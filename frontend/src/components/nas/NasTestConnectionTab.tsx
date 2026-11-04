import { useEffect, useRef, useState } from 'react'
import type { UseFormReturn } from 'react-hook-form'
import {
  ActivityIcon,
  CheckCircle2Icon,
  HardDriveIcon,
  Loader2Icon,
  RefreshCwIcon,
  ServerIcon,
  ShieldAlertIcon,
  WifiIcon,
  WifiOffIcon,
  XCircleIcon,
} from 'lucide-react'

import type { NasConfig, NasTestResult } from '@/api/types'
import { buildNasConfigPayload, type NasConfigFormValues } from '@/components/nas/nasConfigSchema'
import { useTestNasConnection } from '@/hooks/useTestNasConnection'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'

interface NasTestConnectionTabProps {
  config: NasConfig
  form?: UseFormReturn<NasConfigFormValues>
  testResult?: NasTestResult | null
  onTestResultChange?: (result: NasTestResult | null) => void
  onNavigateToConfig?: () => void
  onStatusChange?: (status: 'connected' | 'disconnected' | null) => void
}

export function NasTestConnectionTab({
  config,
  form,
  testResult: parentTestResult,
  onTestResultChange,
  onNavigateToConfig,
  onStatusChange,
}: NasTestConnectionTabProps) {
  const testMutation = useTestNasConnection()
  const [localTestResult, setLocalTestResult] = useState<NasTestResult | null>(null)
  const testResult = parentTestResult ?? localTestResult

  function updateResult(result: NasTestResult | null) {
    setLocalTestResult(result)
    onTestResultChange?.(result)
    if (result) {
      onStatusChange?.(result.ok ? 'connected' : 'disconnected')
    }
  }

  const hasAutoTested = useRef(false)

  // Current values from live form or saved config
  const formValues = form?.getValues()
  const activeProtocol = formValues?.protocol ?? config.protocol ?? 'smb'
  const activeHost = formValues?.host ?? config.host ?? ''
  const activePort = formValues?.port ? Number(formValues.port) : config.port
  const activeShare = formValues?.share ?? config.share ?? ''

  function getTestPayload() {
    if (form) {
      const vals = form.getValues()
      if (vals.host?.trim() || vals.protocol === 'local') {
        try {
          return buildNasConfigPayload(vals)
        } catch {
          // If form is incomplete, fallback
        }
      }
    }
    return config.configured ? undefined : null
  }

  // Trigger exactly ONE request on mount ONLY IF we do not already have a testResult
  useEffect(() => {
    if (!hasAutoTested.current) {
      hasAutoTested.current = true
      // If we already have a test result (e.g. from Configuration tab or earlier test), skip auto-testing!
      if (testResult) {
        return
      }
      const payload = getTestPayload()
      if (payload !== null) {
        testMutation
          .mutateAsync(payload ?? undefined)
          .then((res) => {
            if (res) {
              updateResult(res)
            }
          })
          .catch(() => {
            onStatusChange?.('disconnected')
          })
      }
    }
  }, [testMutation, onStatusChange, testResult])

  // Explicit user click: sends exactly ONE request
  async function handleTest() {
    if (testMutation.isPending) return
    const payload = getTestPayload()
    if (payload === null && activeProtocol !== 'local' && !activeHost.trim()) {
      if (onNavigateToConfig) onNavigateToConfig()
      return
    }

    const res = await testMutation.mutateAsync(payload ?? undefined).catch(() => null)
    if (res) {
      updateResult(res)
    } else {
      onStatusChange?.('disconnected')
    }
  }

  const isTesting = testMutation.isPending
  const hasTarget = activeProtocol === 'local' || Boolean(activeHost.trim())

  return (
    <div className="flex max-w-3xl flex-col gap-6">
      {/* Real-time Status Card */}
      {isTesting ? (
        <div className="flex flex-col items-center justify-center gap-3 rounded-xl border border-primary/20 bg-primary/5 p-8 text-center animate-pulse">
          <Loader2Icon className="size-8 animate-spin text-primary" />
          <div className="space-y-1">
            <h3 className="text-base font-semibold text-foreground">Testing Real-Time Connection…</h3>
            <p className="text-xs text-muted-foreground">
              Sending 1 connection probe request to {activeProtocol.toUpperCase()}://{activeHost || 'server'}{activePort ? `:${activePort}` : ''}
            </p>
          </div>
        </div>
      ) : testResult ? (
        testResult.ok ? (
          <div className="flex flex-col gap-4 rounded-xl border-2 border-emerald-500/50 bg-emerald-500/10 p-6 text-emerald-900 dark:text-emerald-100">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="flex size-11 items-center justify-center rounded-full bg-emerald-500 text-white shadow-sm">
                  <CheckCircle2Icon className="size-6" />
                </div>
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-xl font-bold text-emerald-700 dark:text-emerald-300">
                      NAS connected
                    </span>
                    <Badge variant="success" className="bg-emerald-600 text-white font-medium">
                      Online
                    </Badge>
                  </div>
                  <p className="text-xs text-emerald-700/80 dark:text-emerald-300/80">
                    Real-time connection active and verified.
                  </p>
                </div>
              </div>
              <WifiIcon className="size-6 text-emerald-600 dark:text-emerald-400" />
            </div>

            <div className="grid grid-cols-2 gap-3 pt-2 border-t border-emerald-500/20">
              <div className="rounded-lg bg-background/80 p-3 shadow-xs">
                <span className="text-[11px] font-medium text-muted-foreground uppercase">Protocol</span>
                <p className="text-sm font-semibold text-foreground uppercase">{activeProtocol}</p>
              </div>
              <div className="rounded-lg bg-background/80 p-3 shadow-xs">
                <span className="text-[11px] font-medium text-muted-foreground uppercase">Response Time (Latency)</span>
                <p className="text-sm font-semibold text-foreground font-mono">
                  {testResult.latency_ms !== null ? `${Math.round(testResult.latency_ms)} ms` : 'N/A'}
                </p>
              </div>
              <div className="rounded-lg bg-background/80 p-3 shadow-xs">
                <span className="text-[11px] font-medium text-muted-foreground uppercase">Read Access</span>
                <p className="text-sm font-semibold text-emerald-600">
                  {testResult.can_read ? 'Verified (Can read)' : 'Cannot read'}
                </p>
              </div>
              <div className="rounded-lg bg-background/80 p-3 shadow-xs">
                <span className="text-[11px] font-medium text-muted-foreground uppercase">Write Access</span>
                <p className="text-sm font-semibold text-emerald-600">
                  {testResult.can_write ? 'Verified (Can write)' : 'Cannot write'}
                </p>
              </div>
            </div>

            <div className="text-xs text-emerald-800 dark:text-emerald-200">
              {testResult.message}
            </div>

            {testResult.sample_files.length > 0 && (
              <div className="rounded-lg border border-emerald-500/20 bg-background/80 p-3">
                <div className="flex items-center gap-1.5 mb-1.5 text-xs font-semibold text-foreground">
                  <HardDriveIcon className="size-3.5 text-emerald-600" />
                  <span>Sample Files Verified on Share:</span>
                </div>
                <ul className="font-mono text-xs text-muted-foreground space-y-0.5 max-h-28 overflow-y-auto">
                  {testResult.sample_files.map((file) => (
                    <li key={file} className="truncate">
                      • {file}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        ) : (
          <div className="flex flex-col gap-4 rounded-xl border-2 border-destructive/50 bg-destructive/10 p-6 text-destructive">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="flex size-11 items-center justify-center rounded-full bg-destructive text-destructive-foreground shadow-sm">
                  <XCircleIcon className="size-6" />
                </div>
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-xl font-bold text-destructive">
                      NAS disconnected
                    </span>
                    <Badge variant="destructive">
                      Offline
                    </Badge>
                  </div>
                  <p className="text-xs text-destructive/80">
                    Failed to establish connection with the NAS.
                  </p>
                </div>
              </div>
              <WifiOffIcon className="size-6 text-destructive" />
            </div>

            <div className="rounded-lg border border-destructive/20 bg-background/90 p-3.5 text-xs">
              <span className="font-semibold text-foreground">Diagnostic Message: </span>
              <p className="text-destructive font-mono mt-1 break-words">
                {testResult.message || 'Host unreachable or timed out'}
              </p>
            </div>

            <div className="rounded-lg border border-destructive/20 bg-background/50 p-3 text-xs text-muted-foreground space-y-1">
              <p className="font-semibold text-foreground flex items-center gap-1">
                <ShieldAlertIcon className="size-3.5 text-destructive" />
                Troubleshooting Checklist:
              </p>
              <ul className="list-disc pl-5 space-y-1">
                <li>Verify host address (<span className="font-mono text-foreground">{activeHost || 'none'}</span>) and port ({activePort || 'default'}).</li>
                <li>Verify user account credentials and directory permissions on the NAS.</li>
                <li>Ensure the NAS service ({activeProtocol.toUpperCase()}) is running and accepting connections.</li>
              </ul>
            </div>
          </div>
        )
      ) : (
        <Card className="flex flex-col items-center justify-center gap-3 p-8 text-center">
          <ActivityIcon className="size-8 text-muted-foreground" />
          <div className="space-y-1">
            <h3 className="text-base font-semibold">Ready to Test Connection</h3>
            <p className="text-xs text-muted-foreground max-w-sm">
              Click below to send a single real-time health probe request to the configured NAS server.
            </p>
          </div>
        </Card>
      )}

      {/* Target Details Card */}
      <Card className="p-4 gap-3">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold flex items-center gap-2">
            <ServerIcon className="size-4 text-primary" />
            Target NAS Connection
          </h3>
          <Badge variant="outline" className="font-mono text-xs uppercase">
            {activeProtocol}
          </Badge>
        </div>
        <div className={cn('grid grid-cols-1 gap-3 text-sm pt-1', activeShare ? 'sm:grid-cols-3' : 'sm:grid-cols-2')}>
          <div className="flex flex-col gap-0.5">
            <span className="text-xs text-muted-foreground">Protocol</span>
            <span className="font-medium uppercase">{activeProtocol}</span>
          </div>
          {activeProtocol !== 'local' && (
            <div className="flex flex-col gap-0.5">
              <span className="text-xs text-muted-foreground">Server Host &amp; Port</span>
              <span className="font-mono text-xs font-medium">
                {activeHost ? `${activeHost}:${activePort || 'default'}` : '(Host not set yet)'}
              </span>
            </div>
          )}
          {activeShare && (
            <div className="flex flex-col gap-0.5">
              <span className="text-xs text-muted-foreground">Share</span>
              <span className="font-mono text-xs font-medium truncate">{activeShare}</span>
            </div>
          )}
        </div>
      </Card>

      {/* Action Controls */}
      <div className="flex items-center gap-3">
        <Button
          type="button"
          onClick={handleTest}
          disabled={isTesting}
          className="gap-2"
        >
          {isTesting ? (
            <>
              <Loader2Icon className="size-4 animate-spin" />
              Testing Connection…
            </>
          ) : (
            <>
              <RefreshCwIcon className="size-4" />
              {hasTarget ? 'Test Connection Now' : 'Enter Host to Test'}
            </>
          )}
        </Button>
        <span className="text-xs text-muted-foreground">
          Sends only 1 connection request to NAS in real-time.
        </span>
      </div>
    </div>
  )
}
