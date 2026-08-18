import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { ArrowLeftIcon } from 'lucide-react'

import { Topbar } from '@/components/layout/Topbar'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { AssetGrid } from '@/components/assets/AssetGrid'
import { AssetFilterBar } from '@/components/assets/AssetFilterBar'
import { ExportTriggerDialog } from '@/components/exports/ExportTriggerDialog'
import { ExportStatusPanel } from '@/components/exports/ExportStatusPanel'
import { useJob } from '@/hooks/useJob'
import { useAssets } from '@/hooks/useAssets'
import { useJobProgressSocket } from '@/hooks/useJobProgressSocket'
import type { AssetStatus, SourceType } from '@/api/types'

export function JobAssetsPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const { data: job } = useJob(jobId)
  // Export events (export_progress/complete/failed) are published on the
  // job's own WS channel, so this page needs its own socket mount too — it's
  // a separate route from JobDetailPage, not nested under it.
  useJobProgressSocket(jobId)

  const [status, setStatus] = useState<AssetStatus | undefined>()
  const [sourceType, setSourceType] = useState<SourceType | undefined>()
  const [activeExportId, setActiveExportId] = useState<string | null>(null)

  const { data: assets, isLoading } = useAssets(jobId, {
    status,
    source_type: sourceType,
    limit: 200,
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

        <AssetFilterBar
          status={status}
          onStatusChange={setStatus}
          sourceType={sourceType}
          onSourceTypeChange={setSourceType}
        />

        <Card>
          <CardContent>
            <AssetGrid assets={assets} isLoading={isLoading} />
          </CardContent>
        </Card>
      </div>
    </>
  )
}
