import { useFieldArray, type Control, type UseFormRegister } from 'react-hook-form'
import { PlusIcon, XIcon } from 'lucide-react'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'

export function KittiConfigFields({
  control,
  register,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'kittiCategories' })

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <Label>Max images to search</Label>
        <Input type="number" min={1} placeholder="200" className="max-w-40" {...register('kittiMaxImages')} />
      </div>

      <div className="flex flex-col gap-2">
        <Label>Explicit category overrides (optional — KITTI's fixed 8-class driving-scene vocabulary)</Label>
        {fields.map((field, index) => (
          <div key={field.id} className="flex items-center gap-2">
            <Input placeholder="e.g. Car, Pedestrian, Cyclist" {...register(`kittiCategories.${index}.category`)} />
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
          onClick={() => append({ category: '' })}
        >
          <PlusIcon /> Add category override
        </Button>
      </div>
    </div>
  )
}
