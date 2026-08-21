import { useFieldArray, type Control, type UseFormRegister } from 'react-hook-form'
import { PlusIcon, XIcon } from 'lucide-react'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'

export function DavisConfigFields({
  control,
  register,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  const { fields, append, remove } = useFieldArray({ control, name: 'davisSequences' })

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <Label>Max frames to search</Label>
        <Input type="number" min={1} placeholder="200" className="max-w-40" {...register('davisMaxImages')} />
      </div>

      <div className="flex flex-col gap-2">
        <Label>
          Explicit sequence-name overrides (optional — DAVIS has no class vocabulary, only ~60 video
          sequence names, e.g. &quot;dog&quot;, &quot;boat&quot;, &quot;car-roundabout&quot;)
        </Label>
        {fields.map((field, index) => (
          <div key={field.id} className="flex items-center gap-2">
            <Input placeholder="e.g. dog, boat, car-roundabout" {...register(`davisSequences.${index}.sequence`)} />
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
          onClick={() => append({ sequence: '' })}
        >
          <PlusIcon /> Add sequence override
        </Button>
      </div>
    </div>
  )
}
