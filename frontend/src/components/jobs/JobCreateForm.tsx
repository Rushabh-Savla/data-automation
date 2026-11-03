import { useMemo } from 'react'
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
import { useSources } from '@/hooks/useSources'
import { Textarea } from '@/components/ui/textarea'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { SourcePicker } from '@/components/jobs/SourcePicker'
import { TargetDeadlinePicker } from '@/components/jobs/TargetDeadlinePicker'
import { NasStorageConfigFields } from '@/components/jobs/NasStorageConfigFields'
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
  const recurringEnabled = watch('recurringEnabled')

  // Driven by the API rather than a literal. The hardcoded list here named
  // four sources while twelve connectors are implemented and the backend's
  // auto mode fans out to all of them (JobService.create), so the advanced
  // config panels for the other eight never appeared even though those
  // sources were actively being used. Reading the same registry the backend
  // does means the two cannot drift apart again.
  const { data: availableSources } = useSources()
  const autoSources = useMemo(
    () => (availableSources ?? []).filter((s) => s.connector_implemented).map((s) => s.source_type),
    [availableSources],
  )
  const activeSources = useAutoSources ? autoSources : sources

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

        <Input {...register('name')} placeholder="Job name" className="max-w-sm" />
        {errors.name && <p className="text-xs text-destructive">{errors.name.message}</p>}
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

        <Card className="gap-2 p-4">
          <Label className="flex items-center gap-2">
            <Controller
              control={control}
              name="recurringEnabled"
              render={({ field }) => (
                <Checkbox checked={field.value} onCheckedChange={(c) => field.onChange(c === true)} />
              )}
            />
            Repeat on a schedule
          </Label>
          <p className="text-xs text-muted-foreground">
            Each run collects a fresh set toward the target and is kept as its own job, so you can see what
            every run brought in.
          </p>
          <Input
            placeholder="0 2 * * *"
            disabled={!recurringEnabled}
            className="max-w-xs font-mono"
            {...register('scheduleCron')}
          />
          {errors.recurringEnabled && (
            <p className="text-xs text-destructive">{errors.recurringEnabled.message}</p>
          )}
          {errors.scheduleCron ? (
            <p className="text-xs text-destructive">{errors.scheduleCron.message}</p>
          ) : (
            <p className="text-xs text-muted-foreground">
              Cron format. <code>0 2 * * *</code> is 02:00 daily; <code>0 * * * *</code> is hourly.
            </p>
          )}
        </Card>
      </section>

      <section className="flex flex-col gap-2">
        <NasStorageConfigFields
          jobName={watch('name')}
          control={control}
          register={register}
        />
      </section>

      <JobConfigAdvancedSection
        control={control}
        register={register}
        activeSources={[...activeSources]}
        targetCount={watch('targetCountEnabled') ? watch('targetCount') : undefined}
      />

      <CardContent className="flex justify-end gap-2 p-0">
        <Button type="submit" disabled={isSubmitting} size="lg">
          {isSubmitting ? 'Creating…' : 'Create job'}
        </Button>
      </CardContent>
    </form>
  )
}
