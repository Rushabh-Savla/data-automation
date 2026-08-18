import { z } from 'zod'

import type { JobConfig, JobCreateRequest, SourceType } from '@/api/types'

// Numeric fields bound to plain <input type="number"> via RHF's register()
// are kept as strings throughout the form's lifecycle (native inputs yield
// strings) and only parsed to numbers in buildConfigPayload() at submit
// time — using zod's `coerce` here instead creates a schema input/output
// type split that RHF v7 + @hookform/resolvers can't cleanly infer through
// without threading three generic params everywhere, for no real benefit
// since these are all optional advanced fields anyway.
const optionalNumberString = z
  .string()
  .optional()
  .refine((v) => !v || !Number.isNaN(Number(v)), { message: 'Must be a number' })

export const jobCreateSchema = z
  .object({
    query: z.string().min(1, 'Tell us what you want to collect'),
    name: z.string().optional(),

    useAutoSources: z.boolean(),
    sources: z.array(z.enum(['roboflow', 'kaggle', 'youtube', 'web'])),

    targetCountEnabled: z.boolean(),
    // Controlled directly via TargetDeadlinePicker's onChange (already
    // converts to a number before calling setValue), so this one can stay
    // a real number in the schema without any register()-vs-coerce friction.
    targetCount: z.number().int().positive().optional(),

    deadlineEnabled: z.boolean(),
    deadline: z.string().optional(), // datetime-local string, converted to ISO on submit

    scheduleEnabled: z.boolean(),
    scheduledStart: z.string().optional(),

    // -- Advanced / config (all optional; unset fields are simply omitted on submit) --
    // Field-array items are objects, not raw tuples/strings: react-hook-form's
    // useFieldArray only officially supports arrays of objects (it injects a
    // hidden `id` key into each item for React keying/reconciliation) — an
    // array of plain tuples or strings breaks that reconciliation and was
    // observed to make append() insert two rows per click instead of one.
    roboflowMaxProjects: optionalNumberString,
    roboflowExportFormat: z.string().optional(),
    roboflowProjects: z
      .array(z.object({ workspace: z.string(), project: z.string(), version: z.string(), license: z.string() }))
      .optional(),

    kaggleMaxDatasets: optionalNumberString,
    kaggleSlugs: z.array(z.object({ slug: z.string(), license: z.string() })).optional(),

    youtubeMaxVideos: optionalNumberString,
    youtubeCcOnly: z.boolean().optional(),
    youtubeFrameMode: z.string().optional(),
    youtubeFramesPerVideo: optionalNumberString,
    youtubeUrls: z.array(z.object({ url: z.string() })).optional(),

    webMaxResults: optionalNumberString,
    webSafesearch: z.string().optional(),

    minWidth: optionalNumberString,
    minHeight: optionalNumberString,
    yoloWorldConfThreshold: optionalNumberString,
    dedupHammingThreshold: optionalNumberString,
  })
  .superRefine((data, ctx) => {
    if (!data.targetCountEnabled && !data.deadlineEnabled) {
      ctx.addIssue({
        code: 'custom',
        message: 'Set a target image count or a deadline (or both)',
        path: ['targetCountEnabled'],
      })
    }
    if (data.targetCountEnabled && !data.targetCount) {
      ctx.addIssue({ code: 'custom', message: 'Enter a target count', path: ['targetCount'] })
    }
    if (data.deadlineEnabled && !data.deadline) {
      ctx.addIssue({ code: 'custom', message: 'Pick a deadline', path: ['deadline'] })
    }
    if (data.scheduleEnabled && !data.scheduledStart) {
      ctx.addIssue({ code: 'custom', message: 'Pick a start time', path: ['scheduledStart'] })
    }
    if (!data.useAutoSources && data.sources.length === 0) {
      ctx.addIssue({ code: 'custom', message: 'Pick at least one source, or use Auto', path: ['sources'] })
    }
  })

export type JobCreateFormValues = z.infer<typeof jobCreateSchema>

export const jobCreateDefaultValues: JobCreateFormValues = {
  query: '',
  name: '',
  useAutoSources: true,
  sources: [],
  targetCountEnabled: true,
  targetCount: 500,
  deadlineEnabled: false,
  deadline: undefined,
  scheduleEnabled: false,
  scheduledStart: undefined,
  roboflowMaxProjects: undefined,
  roboflowExportFormat: undefined,
  roboflowProjects: [],
  kaggleMaxDatasets: undefined,
  kaggleSlugs: [],
  youtubeMaxVideos: undefined,
  // Left unset (not `true`) even though the checkbox displays checked by
  // default (YoutubeConfigFields renders `field.value ?? true`) — if this
  // defaulted to `true` here, buildConfigPayload's "only send what the user
  // touched" check (`!== undefined`) would always be true and every job
  // would silently send `youtube.cc_only: true` even with Advanced options
  // never opened, which is exactly the spurious-config problem that check
  // exists to prevent.
  youtubeCcOnly: undefined,
  youtubeFrameMode: undefined,
  youtubeFramesPerVideo: undefined,
  youtubeUrls: [],
  webMaxResults: undefined,
  webSafesearch: undefined,
  minWidth: undefined,
  minHeight: undefined,
  yoloWorldConfThreshold: undefined,
  dedupHammingThreshold: undefined,
}

function toNumber(value: string | undefined): number | undefined {
  if (!value) return undefined
  const n = Number(value)
  return Number.isNaN(n) ? undefined : n
}

/** Walks only the fields the user actually touched so an untouched Advanced
 * section submits `config: {}` exactly, never a spurious empty per-source
 * object (see plan: JobConfigAdvancedSection). */
export function buildConfigPayload(values: JobCreateFormValues): JobConfig {
  const config: JobConfig = {}

  const roboflow: JobConfig['roboflow'] = {}
  const roboflowMaxProjects = toNumber(values.roboflowMaxProjects)
  if (roboflowMaxProjects) roboflow.max_projects = roboflowMaxProjects
  if (values.roboflowExportFormat) roboflow.export_format = values.roboflowExportFormat
  const validProjects = (values.roboflowProjects ?? []).filter(
    (r) => r.workspace.trim() !== '' && r.project.trim() !== '' && r.version.trim() !== '' && r.license.trim() !== '',
  )
  if (validProjects.length) {
    roboflow.projects = validProjects.map(
      ({ workspace, project, version, license }) =>
        [workspace, project, Number(version), license] as [string, string, number, string],
    )
  }
  if (Object.keys(roboflow).length) config.roboflow = roboflow

  const kaggle: JobConfig['kaggle'] = {}
  const kaggleMaxDatasets = toNumber(values.kaggleMaxDatasets)
  if (kaggleMaxDatasets) kaggle.max_datasets = kaggleMaxDatasets
  const validSlugs = (values.kaggleSlugs ?? []).filter((r) => r.slug.trim() !== '' && r.license.trim() !== '')
  if (validSlugs.length) kaggle.slugs = validSlugs.map(({ slug, license }) => [slug, license])
  if (Object.keys(kaggle).length) config.kaggle = kaggle

  const youtube: JobConfig['youtube'] = {}
  const youtubeMaxVideos = toNumber(values.youtubeMaxVideos)
  if (youtubeMaxVideos) youtube.max_videos = youtubeMaxVideos
  if (values.youtubeCcOnly !== undefined) youtube.cc_only = values.youtubeCcOnly
  if (values.youtubeFrameMode) youtube.frame_mode = values.youtubeFrameMode
  const youtubeFramesPerVideo = toNumber(values.youtubeFramesPerVideo)
  if (youtubeFramesPerVideo) youtube.frames_per_video = youtubeFramesPerVideo
  const validUrls = (values.youtubeUrls ?? []).filter((u) => u.url.trim() !== '')
  if (validUrls.length) youtube.urls = validUrls.map((u) => u.url)
  if (Object.keys(youtube).length) config.youtube = youtube

  const web: JobConfig['web'] = {}
  const webMaxResults = toNumber(values.webMaxResults)
  if (webMaxResults) web.max_results = webMaxResults
  if (values.webSafesearch) web.safesearch = values.webSafesearch
  if (Object.keys(web).length) config.web = web

  const minWidth = toNumber(values.minWidth)
  const minHeight = toNumber(values.minHeight)
  if (minWidth && minHeight) config.min_resolution = [minWidth, minHeight]

  const yoloWorldConfThreshold = toNumber(values.yoloWorldConfThreshold)
  if (yoloWorldConfThreshold !== undefined) config.yolo_world_conf_threshold = yoloWorldConfThreshold

  const dedupHammingThreshold = toNumber(values.dedupHammingThreshold)
  if (dedupHammingThreshold !== undefined) config.dedup_hamming_threshold = dedupHammingThreshold

  return config
}

export function buildJobCreatePayload(values: JobCreateFormValues): JobCreateRequest {
  const payload: JobCreateRequest = {
    query: values.query,
    name: values.name?.trim() ? values.name.trim() : undefined,
    sources: values.useAutoSources ? undefined : (values.sources as SourceType[]),
    target_count: values.targetCountEnabled ? values.targetCount : undefined,
    deadline_at: values.deadlineEnabled && values.deadline ? new Date(values.deadline).toISOString() : undefined,
    scheduled_start_at:
      values.scheduleEnabled && values.scheduledStart ? new Date(values.scheduledStart).toISOString() : undefined,
    config: buildConfigPayload(values),
  }
  return payload
}
