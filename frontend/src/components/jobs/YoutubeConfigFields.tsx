import { Controller, useFieldArray, type Control, type UseFormRegister } from 'react-hook-form'
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
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'youtubeUrls' })

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
              <Select value={field.value ?? 'scene'} onValueChange={field.onChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="scene">Scene change</SelectItem>
                  <SelectItem value="fps">Fixed interval</SelectItem>
                  <SelectItem value="every_n">Every Nth frame</SelectItem>
                </SelectContent>
              </Select>
            )}
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label>Frames per video</Label>
          <Input type="number" min={1} placeholder="100" {...register('youtubeFramesPerVideo')} />
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
