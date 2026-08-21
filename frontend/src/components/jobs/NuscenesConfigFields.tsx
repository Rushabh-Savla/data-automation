import { useFieldArray, type Control, type UseFormRegister } from 'react-hook-form'
import { PlusIcon, XIcon } from 'lucide-react'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'

export function NuscenesConfigFields({
  control,
  register,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'nuscenesCategories' })

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <Label>Max samples to search</Label>
        <Input type="number" min={1} placeholder="200" className="max-w-40" {...register('nuscenesMaxSamples')} />
      </div>

      <div className="flex flex-col gap-2">
        <Label>
          Explicit category overrides (optional — driving-scene taxonomy, e.g. &quot;vehicle.car&quot;,
          &quot;human.pedestrian.adult&quot;; uses nuImages&apos; 2D annotations, not native nuScenes 3D data)
        </Label>
        {fields.map((field, index) => (
          <div key={field.id} className="flex items-center gap-2">
            <Input placeholder="e.g. vehicle.car, human.pedestrian.adult" {...register(`nuscenesCategories.${index}.category`)} />
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
