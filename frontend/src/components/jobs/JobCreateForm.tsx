import { useNavigate } from 'react-router-dom'
import { Controller, useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'

import {
  buildJobCreatePayload,
  jobCreateDefaultValues,
  jobCreateSchema,
  type JobCreateFormValues,
} from '@/components/jobs/jobCreateSchema'
import { useCreateJob } from '@/hooks/useCreateJob'
import { Textarea } from '@/components/ui/textarea'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { SourcePicker } from '@/components/jobs/SourcePicker'
import { TargetDeadlinePicker } from '@/components/jobs/TargetDeadlinePicker'
import { JobConfigAdvancedSection } from '@/components/jobs/JobConfigAdvancedSection'

export function JobCreateForm() {
  const navigate = useNavigate()
  const createJob = useCreateJob()

  const {
    register,
    control,
    handleSubmit,
    watch,
    setValue,
    formState: { errors, isSubmitting },
  } = useForm<JobCreateFormValues>({
    resolver: zodResolver(jobCreateSchema),
    defaultValues: jobCreateDefaultValues,
  })

  const useAutoSources = watch('useAutoSources')
  const sources = watch('sources')
  const targetCountEnabled = watch('targetCountEnabled')
  const deadlineEnabled = watch('deadlineEnabled')
  const scheduleEnabled = watch('scheduleEnabled')

  const activeSources = useAutoSources ? (['roboflow', 'kaggle', 'youtube', 'web'] as const) : sources

  async function onSubmit(values: JobCreateFormValues) {
    const payload = buildJobCreatePayload(values)
    const job = await createJob.mutateAsync(payload)
    navigate(`/jobs/${job.id}`)
  }

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="flex max-w-3xl flex-col gap-8 p-6">
      <div className="flex flex-col gap-2">
        <Textarea
          {...register('query')}
          autoFocus
          rows={3}
          placeholder='What do you want to collect? e.g. "people wearing red safety vests"'
          className="text-base"
        />
        {errors.query && <p className="text-xs text-destructive">{errors.query.message}</p>}

        <Input {...register('name')} placeholder="Job name (optional)" className="max-w-sm" />
      </div>

      <section className="flex flex-col gap-2">
        <h2 className="text-sm font-medium">When should it stop?</h2>
        <Controller
          control={control}
          name="targetCountEnabled"
          render={({ field: targetField }) => (
            <Controller
              control={control}
              name="deadlineEnabled"
              render={({ field: deadlineField }) => (
                <TargetDeadlinePicker
                  targetCountEnabled={targetCountEnabled}
                  onTargetCountEnabledChange={targetField.onChange}
                  targetCount={watch('targetCount')}
                  onTargetCountChange={(v) => setValue('targetCount', v)}
                  deadlineEnabled={deadlineEnabled}
                  onDeadlineEnabledChange={deadlineField.onChange}
                  deadline={watch('deadline')}
                  onDeadlineChange={(v) => setValue('deadline', v)}
                  error={errors.targetCountEnabled?.message}
                  targetCountError={errors.targetCount?.message}
                  deadlineError={errors.deadline?.message}
                />
              )}
            />
          )}
        />
      </section>

      <section className="flex flex-col gap-2">
        <h2 className="text-sm font-medium">Sources</h2>
        <SourcePicker
          useAuto={useAutoSources}
          onUseAutoChange={(v) => setValue('useAutoSources', v)}
          selected={sources}
          onSelectedChange={(v) => setValue('sources', v)}
          error={errors.sources?.message}
        />
      </section>

      <section className="flex flex-col gap-2">
        <Card className="gap-2 p-4">
          <Label className="flex items-center gap-2">
            <Controller
              control={control}
              name="scheduleEnabled"
              render={({ field }) => (
                <Checkbox checked={field.value} onCheckedChange={(c) => field.onChange(c === true)} />
              )}
            />
            Schedule for later (instead of starting immediately)
          </Label>
          <Input
            type="datetime-local"
            disabled={!scheduleEnabled}
            className="max-w-xs"
            {...register('scheduledStart')}
          />
          {errors.scheduledStart && <p className="text-xs text-destructive">{errors.scheduledStart.message}</p>}
        </Card>
      </section>

      <JobConfigAdvancedSection control={control} register={register} activeSources={[...activeSources]} />

      <CardContent className="flex justify-end gap-2 p-0">
        <Button type="submit" disabled={isSubmitting} size="lg">
          {isSubmitting ? 'Creating…' : 'Create job'}
        </Button>
      </CardContent>
    </form>
  )
}
