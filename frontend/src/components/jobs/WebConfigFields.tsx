import { Controller, type Control, type UseFormRegister } from 'react-hook-form'

import type { JobCreateFormValues } from '@/components/jobs/jobCreateSchema'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

export function WebConfigFields({
  control,
  register,
}: {
  control: Control<JobCreateFormValues>
  register: UseFormRegister<JobCreateFormValues>
}) {
  return (
    <div className="grid grid-cols-2 gap-3">
      <div className="flex flex-col gap-1.5">
        <Label>Max results</Label>
        <Input type="number" min={1} placeholder="200" {...register('webMaxResults')} />
      </div>
      <div className="flex flex-col gap-1.5">
        <Label>Safesearch</Label>
        <Controller
          control={control}
          name="webSafesearch"
          render={({ field }) => (
            <Select value={field.value ?? 'moderate'} onValueChange={field.onChange}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="strict">Strict</SelectItem>
                <SelectItem value="moderate">Moderate</SelectItem>
                <SelectItem value="off">Off</SelectItem>
              </SelectContent>
            </Select>
          )}
        />
      </div>
    </div>
  )
}
