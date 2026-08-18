import { DownloadIcon } from 'lucide-react'

import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useExportProgress } from '@/hooks/useExportProgress'
import { exportJobStatusLabels } from '@/lib/enumLabels'
import { exportJobStatusVariant } from '@/lib/statusMaps'
import { downloadExport } from '@/api/exports'

export function ExportStatusPanel({ exportId, jobId }: { exportId: string; jobId: string }) {
  const { data: exportJob } = useExportProgress(exportId)

  if (!exportJob) return null

  return (
    <Card>
      <CardContent className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <Badge variant={exportJobStatusVariant[exportJob.status]}>
            {exportJobStatusLabels[exportJob.status]}
          </Badge>
          <span className="text-sm text-muted-foreground">
            {exportJob.status === 'running' && `${exportJob.processed_count} processed…`}
            {exportJob.status === 'completed' &&
              `${exportJob.asset_count} assets · ${((exportJob.file_size_bytes ?? 0) / 1024 / 1024).toFixed(1)} MB`}
            {exportJob.status === 'failed' && exportJob.error_message}
          </span>
        </div>

        {exportJob.status === 'completed' && (
          <Button
            size="sm"
            variant="outline"
            className="gap-1.5"
            onClick={() => downloadExport(exportJob.id, exportJob.format, jobId)}
          >
            <DownloadIcon className="size-4" /> Download
          </Button>
        )}
      </CardContent>
    </Card>
  )
}
