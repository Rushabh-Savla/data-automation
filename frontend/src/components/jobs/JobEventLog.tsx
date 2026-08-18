import type { JobLogEvent } from '@/hooks/useJobProgressSocket'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { formatDateTime } from '@/lib/dateFormat'

export function JobEventLog({ events }: { events: JobLogEvent[] }) {
  if (events.length === 0) return null

  return (
    <Card>
      <CardHeader>
        <CardTitle>Activity</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="flex max-h-56 flex-col gap-1.5 overflow-y-auto text-sm">
          {events.map((event) => (
            <li key={event.id} className="flex gap-2">
              <span className="shrink-0 text-muted-foreground">{formatDateTime(event.at)}</span>
              <span>{event.message}</span>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  )
}
