import { ChevronLeftIcon, ChevronRightIcon } from 'lucide-react'

import { Button } from '@/components/ui/button'

interface PaginationProps {
  offset: number
  limit: number
  total: number
  onOffsetChange: (offset: number) => void
}

/** Offset/limit pager (matches the backend's PageResponse envelope directly —
 * no page-number translation needed) shared by the Jobs and Assets lists. */
export function Pagination({ offset, limit, total, onOffsetChange }: PaginationProps) {
  if (total <= limit && offset === 0) return null

  const page = Math.floor(offset / limit) + 1
  const pageCount = Math.max(1, Math.ceil(total / limit))
  const rangeStart = total === 0 ? 0 : offset + 1
  const rangeEnd = Math.min(offset + limit, total)

  return (
    <div className="flex items-center justify-between gap-4 px-1 py-2">
      <span className="text-sm text-muted-foreground">
        {rangeStart}–{rangeEnd} of {total}
      </span>
      <div className="flex items-center gap-2">
        <span className="text-sm text-muted-foreground">
          Page {page} of {pageCount}
        </span>
        <Button
          type="button"
          variant="outline"
          size="icon"
          disabled={offset === 0}
          onClick={() => onOffsetChange(Math.max(0, offset - limit))}
        >
          <ChevronLeftIcon />
        </Button>
        <Button
          type="button"
          variant="outline"
          size="icon"
          disabled={offset + limit >= total}
          onClick={() => onOffsetChange(offset + limit)}
        >
          <ChevronRightIcon />
        </Button>
      </div>
    </div>
  )
}
