import { type JobSourceTaskStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { jobSourceTaskStatusLabels } from '@/lib/enumLabels'
import { jobSourceTaskStatusVariant } from '@/lib/statusMaps'

export function JobSourceTaskStatusBadge({
  status,
  label,
  explanation,
}: {
  status: JobSourceTaskStatus
  label?: string
  explanation?: string
}) {
  const displayLabel = label || jobSourceTaskStatusLabels[status] || status
  const badge = <Badge variant={jobSourceTaskStatusVariant[status] ?? 'secondary'}>{displayLabel}</Badge>

  if (!explanation) {
    return badge
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="cursor-help inline-flex">{badge}</span>
        </TooltipTrigger>
        <TooltipContent side="top">
          <p className="text-xs">{explanation}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
