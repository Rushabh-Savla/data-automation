import { Controller, useFieldArray, useWatch, type Control, type UseFormRegister } from 'react-hook-form'
import { PlusIcon, XIcon } from 'lucide-react'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

export function YoutubeConfigFields({
  control,
  register,
  targetCount,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
  targetCount?: number
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'youtubeUrls' })

  const maxVideosRaw = useWatch({ control, name: 'youtubeMaxVideos' })
  const framesPerVideoRaw = useWatch({ control, name: 'youtubeFramesPerVideo' })

  const maxVideos = Number(maxVideosRaw)
  const framesPerVideo = Number(framesPerVideoRaw)
  const maxPossibleFrames =
    maxVideosRaw && framesPerVideoRaw && Number.isFinite(maxVideos) && Number.isFinite(framesPerVideo)
      ? maxVideos * framesPerVideo
      : undefined
  const showLowYieldWarning =
    maxPossibleFrames !== undefined && targetCount !== undefined && maxPossibleFrames < targetCount

  return (
    <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <div className="flex flex-col gap-1.5">
          <Label>Max videos</Label>
          <Input type="number" min={1} placeholder="5" {...register('youtubeMaxVideos')} />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label>Frame mode</Label>
          <Controller
            control={control}
            name="youtubeFrameMode"
            render={({ field }) => (
              <Select value={field.value ?? 'fps'} onValueChange={field.onChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="fps">Fixed interval</SelectItem>
                </SelectContent>
              </Select>
            )}
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label>Frames per video</Label>
          <Input type="number" min={1} placeholder="100" {...register('youtubeFramesPerVideo')} />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label>Frame interval (sec)</Label>
          <Input type="number" min={0.1} step={0.1} placeholder="1" {...register('youtubeFrameInterval')} />
        </div>
        <div className="flex flex-col justify-end gap-1.5 pb-1.5">
          <Controller
            control={control}
            name="youtubeCcOnly"
            render={({ field }) => (
              <Label className="flex items-center gap-2">
                <Checkbox checked={field.value ?? true} onCheckedChange={(c) => field.onChange(c === true)} />
                Creative Commons only
              </Label>
            )}
          />
        </div>
      </div>

      {showLowYieldWarning && (
        <p className="text-xs text-amber-600 dark:text-amber-500">
          Max videos × frames per video caps this source at {maxPossibleFrames} candidate frame
          {maxPossibleFrames === 1 ? '' : 's'}, below the job's target of {targetCount} accepted —
          some frames will also be rejected by relevance/dedup filtering, so raise these values or
          lower the target to avoid the job failing with "Sources exhausted".
        </p>
      )}

      <div className="flex flex-col gap-2">
        <Label>Explicit video URLs (optional — skips auto-search)</Label>
        {fields.map((field, index) => (
          <div key={field.id} className="flex items-center gap-2">
            <Input placeholder="https://youtube.com/watch?v=..." {...register(`youtubeUrls.${index}.url`)} />
            <Button type="button" variant="ghost" size="icon" onClick={() => remove(index)}>
              <XIcon />
            </Button>
          </div>
        ))}
        <Button type="button" variant="outline" size="sm" className="w-fit gap-1" onClick={() => append({ url: '' })}>
          <PlusIcon /> Add video URL
        </Button>
      </div>
    </div>
  )
}
