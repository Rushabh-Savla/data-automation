import { useFieldArray, type Control, type UseFormRegister } from 'react-hook-form'
import { PlusIcon, XIcon } from 'lucide-react'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'

export function SunRgbdConfigFields({
  control,
  register,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'sunRgbdCategories' })

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <Label>Max images to search</Label>
        <Input type="number" min={1} placeholder="200" className="max-w-40" {...register('sunRgbdMaxImages')} />
      </div>

      <div className="flex flex-col gap-2">
        <Label>Explicit category overrides (optional — SUN RGB-D's open, crowd-sourced indoor-scene vocabulary)</Label>
        {fields.map((field, index) => (
          <div key={field.id} className="flex items-center gap-2">
            <Input placeholder="e.g. chair, table, sofa" {...register(`sunRgbdCategories.${index}.category`)} />
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
