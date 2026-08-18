import { cn } from '@/lib/utils'

export type ConnectionStatus = 'connecting' | 'connected' | 'reconnecting' | 'polling-fallback'

const statusConfig: Record<ConnectionStatus, { label: string; dot: string; pulse?: boolean }> = {
  connecting: { label: 'Connecting…', dot: 'bg-muted-foreground', pulse: true },
  connected: { label: 'Live', dot: 'bg-success' },
  reconnecting: { label: 'Reconnecting…', dot: 'bg-warning', pulse: true },
  'polling-fallback': { label: 'Polling (every 3s)', dot: 'bg-muted-foreground' },
}

export function ConnectionStatusBadge({ status }: { status: ConnectionStatus }) {
  const config = statusConfig[status]
  return (
    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
      <span className={cn('size-2 rounded-full', config.dot, config.pulse && 'animate-pulse')} />
      {config.label}
    </div>
  )
}
