import { useState } from 'react'
import { DownloadIcon } from 'lucide-react'

import type { ExportFormat } from '@/api/types'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { exportFormatLabels } from '@/lib/enumLabels'
import { useCreateExport } from '@/hooks/useCreateExport'

export function ExportTriggerDialog({ jobId, onExportStarted }: { jobId: string; onExportStarted: (exportId: string) => void }) {
  const [open, setOpen] = useState(false)
  const [format, setFormat] = useState<ExportFormat>('raw')
  const [minScore, setMinScore] = useState('')

  const createExport = useCreateExport(jobId)

  async function handleExport() {
    const exportJob = await createExport.mutateAsync({
      format,
      filter: minScore ? { min_relevance_score: Number(minScore) } : {},
    })
    setOpen(false)
    onExportStarted(exportJob.id)
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm" className="gap-1.5">
          <DownloadIcon className="size-4" /> Export
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Export dataset</DialogTitle>
          <DialogDescription>Packages this job's accepted assets into a downloadable zip.</DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <Label>Format</Label>
            <Select value={format} onValueChange={(v) => setFormat(v as ExportFormat)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {Object.entries(exportFormatLabels).map(([value, label]) => (
                  <SelectItem key={value} value={value}>
                    {label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="flex flex-col gap-1.5">
            <Label>Minimum relevance score (optional)</Label>
            <Input
              type="number"
              min={0}
              max={1}
              step={0.05}
              placeholder="No minimum"
              value={minScore}
              onChange={(e) => setMinScore(e.target.value)}
            />
          </div>
        </div>

        <DialogFooter>
          <Button onClick={handleExport} disabled={createExport.isPending}>
            {createExport.isPending ? 'Starting…' : 'Start export'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
