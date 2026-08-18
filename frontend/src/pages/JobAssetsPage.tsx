import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { ArrowLeftIcon } from 'lucide-react'

import { Topbar } from '@/components/layout/Topbar'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { SearchInput } from '@/components/ui/search-input'
import { Pagination } from '@/components/ui/pagination'
import { AssetGrid } from '@/components/assets/AssetGrid'
import { AssetFilterBar } from '@/components/assets/AssetFilterBar'
import { ExportTriggerDialog } from '@/components/exports/ExportTriggerDialog'
import { ExportStatusPanel } from '@/components/exports/ExportStatusPanel'
import { useJob } from '@/hooks/useJob'
import { useAssets } from '@/hooks/useAssets'
import { useJobProgressSocket } from '@/hooks/useJobProgressSocket'
import { useDebouncedValue } from '@/hooks/useDebouncedValue'
import type { AssetStatus, SourceType } from '@/api/types'

const PAGE_SIZE = 48

export function JobAssetsPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const { data: job } = useJob(jobId)
  // Export events (export_progress/complete/failed) are published on the
  // job's own WS channel, so this page needs its own socket mount too — it's
  // a separate route from JobDetailPage, not nested under it.
  useJobProgressSocket(jobId)

  const [status, setStatus] = useState<AssetStatus | undefined>()
  const [sourceType, setSourceType] = useState<SourceType | undefined>()
  const [searchInput, setSearchInput] = useState('')
  const [offset, setOffset] = useState(0)
  const [activeExportId, setActiveExportId] = useState<string | null>(null)
  const search = useDebouncedValue(searchInput)

  useEffect(() => {
    setOffset(0)
  }, [status, sourceType, search])

  const { data, isLoading } = useAssets(jobId, {
    status,
    source_type: sourceType,
    search: search || undefined,
    limit: PAGE_SIZE,
    offset,
  })

  return (
    <>
      <Topbar
        title={job ? `Assets — ${job.name || job.query}` : 'Assets'}
        right={
          <>
            <Button asChild variant="outline" size="sm" className="gap-1.5">
              <Link to={jobId ? `/jobs/${jobId}` : '/jobs'}>
                <ArrowLeftIcon className="size-4" /> Back to job
              </Link>
            </Button>
            {jobId && <ExportTriggerDialog jobId={jobId} onExportStarted={setActiveExportId} />}
          </>
        }
      />

      <div className="flex flex-col gap-4 p-6">
        {activeExportId && jobId && <ExportStatusPanel exportId={activeExportId} jobId={jobId} />}

        {!job && jobId && <Skeleton className="h-8 w-64" />}

        <div className="flex flex-wrap gap-3">
          <SearchInput
            value={searchInput}
            onChange={setSearchInput}
            placeholder="Search by filename or dataset…"
            className="w-72"
          />
          <AssetFilterBar
            status={status}
            onStatusChange={setStatus}
            sourceType={sourceType}
            onSourceTypeChange={setSourceType}
          />
        </div>

        <Card>
          <CardContent>
            <AssetGrid assets={data?.items} isLoading={isLoading} />
            {data && (
              <Pagination offset={offset} limit={PAGE_SIZE} total={data.total} onOffsetChange={setOffset} />
            )}
          </CardContent>
        </Card>
      </div>
    </>
  )
}
