import { useState } from 'react'
import { ChevronRightIcon } from 'lucide-react'
import type { Control, UseFormRegister } from 'react-hook-form'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import type { SourceType } from '@/api/types'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { cn } from '@/lib/utils'
import { RoboflowConfigFields } from '@/components/jobs/RoboflowConfigFields'
import { KaggleConfigFields } from '@/components/jobs/KaggleConfigFields'
import { YoutubeConfigFields } from '@/components/jobs/YoutubeConfigFields'
import { WebConfigFields } from '@/components/jobs/WebConfigFields'

interface JobConfigAdvancedSectionProps {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
  activeSources: SourceType[]
}

export function JobConfigAdvancedSection({ control, register, activeSources }: JobConfigAdvancedSectionProps) {
  const [open, setOpen] = useState(false)

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
        <ChevronRightIcon className={cn('size-4 transition-transform', open && 'rotate-90')} />
        Advanced options
      </CollapsibleTrigger>

      <CollapsibleContent className="flex flex-col gap-3 pt-3">
        {activeSources.includes('roboflow') && (
          <Card className="gap-2 p-4">
            <CardHeader className="p-0">
              <CardTitle className="text-sm">Roboflow</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <RoboflowConfigFields control={control} register={register} />
            </CardContent>
          </Card>
        )}

        {activeSources.includes('kaggle') && (
          <Card className="gap-2 p-4">
            <CardHeader className="p-0">
              <CardTitle className="text-sm">Kaggle</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <KaggleConfigFields control={control} register={register} />
            </CardContent>
          </Card>
        )}

        {activeSources.includes('youtube') && (
          <Card className="gap-2 p-4">
            <CardHeader className="p-0">
              <CardTitle className="text-sm">YouTube</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <YoutubeConfigFields control={control} register={register} />
            </CardContent>
          </Card>
        )}

        {activeSources.includes('web') && (
          <Card className="gap-2 p-4">
            <CardHeader className="p-0">
              <CardTitle className="text-sm">Web</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <WebConfigFields control={control} register={register} />
            </CardContent>
          </Card>
        )}

        <Card className="gap-2 p-4">
          <CardHeader className="p-0">
            <CardTitle className="text-sm">General</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 p-0">
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              <div className="flex flex-col gap-1.5">
                <Label>Min width (px)</Label>
                <Input type="number" min={1} placeholder="Default" {...register('minWidth')} />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label>Min height (px)</Label>
                <Input type="number" min={1} placeholder="Default" {...register('minHeight')} />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label>Relevance threshold</Label>
                <Input
                  type="number"
                  min={0}
                  max={1}
                  step={0.05}
                  placeholder="0.15"
                  {...register('yoloWorldConfThreshold')}
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label>Dedup sensitivity</Label>
                <Input type="number" min={0} placeholder="5" {...register('dedupHammingThreshold')} />
              </div>
            </div>
            <p className="text-xs text-muted-foreground">
              Leave width/height blank to use each source's own default — setting only one of the two
              is ignored.
            </p>
          </CardContent>
        </Card>
      </CollapsibleContent>
    </Collapsible>
  )
}
