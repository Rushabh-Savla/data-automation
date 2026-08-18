import { useFieldArray, type Control, type UseFormRegister } from 'react-hook-form'
import { PlusIcon, XIcon } from 'lucide-react'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Controller } from 'react-hook-form'

export function RoboflowConfigFields({
  control,
  register,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'roboflowProjects' })

  return (
    <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 gap-3">
        <div className="flex flex-col gap-1.5">
          <Label>Max projects to search</Label>
          <Input type="number" min={1} placeholder="5" {...register('roboflowMaxProjects')} />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label>Export format</Label>
          <Controller
            control={control}
            name="roboflowExportFormat"
            render={({ field }) => (
              <Select value={field.value ?? 'yolov8'} onValueChange={field.onChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="yolov8">YOLOv8</SelectItem>
                  <SelectItem value="coco">COCO</SelectItem>
                  <SelectItem value="voc">Pascal VOC</SelectItem>
                </SelectContent>
              </Select>
            )}
          />
        </div>
      </div>

      <div className="flex flex-col gap-2">
        <Label>Explicit project overrides (optional — skips auto-search)</Label>
        {fields.map((field, index) => (
          <div key={field.id} className="flex items-center gap-2">
            <Input placeholder="Workspace" {...register(`roboflowProjects.${index}.workspace`)} />
            <Input placeholder="Project" {...register(`roboflowProjects.${index}.project`)} />
            <Input placeholder="Version" className="w-20" {...register(`roboflowProjects.${index}.version`)} />
            <Input placeholder="License" {...register(`roboflowProjects.${index}.license`)} />
            <Button type="button" variant="ghost" size="icon" onClick={() => remove(index)}>
              <XIcon />
            </Button>
          </div>
        ))}
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="w-fit gap-1"
          onClick={() => append({ workspace: '', project: '', version: '1', license: '' })}
        >
          <PlusIcon /> Add project override
        </Button>
      </div>
    </div>
  )
}
