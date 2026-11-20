import { Badge } from '@/components/ui/badge'
import { useNasStatus } from '@/hooks/useNasStatus'

export function NasStatusPill() {
  const { data: status, isLoading } = useNasStatus()

  if (isLoading && !status) {
    return (
      <Badge variant="outline" className="text-xs text-muted-foreground animate-pulse font-medium">
        NAS Checking...
      </Badge>
    )
  }

  if (!status || !status.configured) {
    return (
      <Badge variant="secondary" className="text-xs font-medium text-muted-foreground" title="No network attached storage configured">
        NAS Not Configured
      </Badge>
    )
  }

  if (status.connected) {
    return (
      <Badge
        variant="success"
        className="text-xs font-medium flex items-center gap-1.5 shadow-sm"
        title={status.latency_ms !== null ? `NAS connected (${status.host}:${status.port} · ${status.latency_ms}ms)` : `NAS connected (${status.host})`}
      >
        <span className="relative flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
        </span>
        NAS Connected
      </Badge>
    )
  }

  return (
    <Badge
      variant="destructive"
      className="text-xs font-medium flex items-center gap-1.5 shadow-sm"
      title={status.message || `NAS disconnected (${status.host}:${status.port})`}
    >
      <span className="h-2 w-2 rounded-full bg-red-400"></span>
      NAS Disconnected
    </Badge>
  )
}
