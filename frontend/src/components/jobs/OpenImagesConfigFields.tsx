import { useFieldArray, type Control, type UseFormRegister } from 'react-hook-form'
import { PlusIcon, XIcon } from 'lucide-react'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'

export function OpenImagesConfigFields({
  control,
  register,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'openImagesClasses' })

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <Label>Max samples to search</Label>
        <Input type="number" min={1} placeholder="200" className="max-w-40" {...register('openImagesMaxSamples')} />
      </div>

      <div className="flex flex-col gap-2">
        <Label>Explicit class overrides (optional — Open Images' large class taxonomy)</Label>
        {fields.map((field, index) => (
          <div key={field.id} className="flex items-center gap-2">
            <Input placeholder="e.g. Dog, Car" {...register(`openImagesClasses.${index}.className`)} />
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
          onClick={() => append({ className: '' })}
        >
          <PlusIcon /> Add class override
        </Button>
      </div>
    </div>
  )
}
