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
    sources: z.array(
      z.enum([
        'roboflow',
        'kaggle',
        'youtube',
        'web',
        'coco',
        'open_images',
        'pascal_voc',
        'visual_genome',
        'davis',
        'sun_rgbd',
        'nuscenes',
        'kitti',
      ]),
    ),

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

    cocoMaxImages: optionalNumberString,
    cocoCategories: z.array(z.object({ category: z.string() })).optional(),

    openImagesMaxSamples: optionalNumberString,
    openImagesClasses: z.array(z.object({ className: z.string() })).optional(),

    pascalVocMaxImages: optionalNumberString,
    pascalVocCategories: z.array(z.object({ category: z.string() })).optional(),

    visualGenomeMaxImages: optionalNumberString,
    visualGenomeCategories: z.array(z.object({ category: z.string() })).optional(),

    davisMaxImages: optionalNumberString,
    davisSequences: z.array(z.object({ sequence: z.string() })).optional(),

    sunRgbdMaxImages: optionalNumberString,
    sunRgbdCategories: z.array(z.object({ category: z.string() })).optional(),

    nuscenesMaxSamples: optionalNumberString,
    nuscenesCategories: z.array(z.object({ category: z.string() })).optional(),

    kittiMaxImages: optionalNumberString,
    kittiCategories: z.array(z.object({ category: z.string() })).optional(),

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
  cocoMaxImages: undefined,
  cocoCategories: [],
  openImagesMaxSamples: undefined,
  openImagesClasses: [],
  pascalVocMaxImages: undefined,
  pascalVocCategories: [],
  visualGenomeMaxImages: undefined,
  visualGenomeCategories: [],
  davisMaxImages: undefined,
  davisSequences: [],
  sunRgbdMaxImages: undefined,
  sunRgbdCategories: [],
  nuscenesMaxSamples: undefined,
  nuscenesCategories: [],
  kittiMaxImages: undefined,
  kittiCategories: [],
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

  const coco: JobConfig['coco'] = {}
  const cocoMaxImages = toNumber(values.cocoMaxImages)
  if (cocoMaxImages) coco.max_images = cocoMaxImages
  const validCocoCategories = (values.cocoCategories ?? []).map((c) => c.category.trim()).filter((c) => c !== '')
  if (validCocoCategories.length) coco.categories = validCocoCategories
  if (Object.keys(coco).length) config.coco = coco

  const openImages: JobConfig['open_images'] = {}
  const openImagesMaxSamples = toNumber(values.openImagesMaxSamples)
  if (openImagesMaxSamples) openImages.max_samples = openImagesMaxSamples
  const validClasses = (values.openImagesClasses ?? []).map((c) => c.className.trim()).filter((c) => c !== '')
  if (validClasses.length) openImages.classes = validClasses
  if (Object.keys(openImages).length) config.open_images = openImages

  const pascalVoc: JobConfig['pascal_voc'] = {}
  const pascalVocMaxImages = toNumber(values.pascalVocMaxImages)
  if (pascalVocMaxImages) pascalVoc.max_images = pascalVocMaxImages
  const validVocCategories = (values.pascalVocCategories ?? []).map((c) => c.category.trim()).filter((c) => c !== '')
  if (validVocCategories.length) pascalVoc.categories = validVocCategories
  if (Object.keys(pascalVoc).length) config.pascal_voc = pascalVoc

  const visualGenome: JobConfig['visual_genome'] = {}
  const visualGenomeMaxImages = toNumber(values.visualGenomeMaxImages)
  if (visualGenomeMaxImages) visualGenome.max_images = visualGenomeMaxImages
  const validVgCategories = (values.visualGenomeCategories ?? [])
    .map((c) => c.category.trim())
    .filter((c) => c !== '')
  if (validVgCategories.length) visualGenome.categories = validVgCategories
  if (Object.keys(visualGenome).length) config.visual_genome = visualGenome

  const davis: JobConfig['davis'] = {}
  const davisMaxImages = toNumber(values.davisMaxImages)
  if (davisMaxImages) davis.max_images = davisMaxImages
  const validSequences = (values.davisSequences ?? []).map((s) => s.sequence.trim()).filter((s) => s !== '')
  if (validSequences.length) davis.sequences = validSequences
  if (Object.keys(davis).length) config.davis = davis

  const sunRgbd: JobConfig['sun_rgbd'] = {}
  const sunRgbdMaxImages = toNumber(values.sunRgbdMaxImages)
  if (sunRgbdMaxImages) sunRgbd.max_images = sunRgbdMaxImages
  const validSunCategories = (values.sunRgbdCategories ?? []).map((c) => c.category.trim()).filter((c) => c !== '')
  if (validSunCategories.length) sunRgbd.categories = validSunCategories
  if (Object.keys(sunRgbd).length) config.sun_rgbd = sunRgbd

  const nuscenes: JobConfig['nuscenes'] = {}
  const nuscenesMaxSamples = toNumber(values.nuscenesMaxSamples)
  if (nuscenesMaxSamples) nuscenes.max_samples = nuscenesMaxSamples
  const validNuCategories = (values.nuscenesCategories ?? []).map((c) => c.category.trim()).filter((c) => c !== '')
  if (validNuCategories.length) nuscenes.categories = validNuCategories
  if (Object.keys(nuscenes).length) config.nuscenes = nuscenes

  const kitti: JobConfig['kitti'] = {}
  const kittiMaxImages = toNumber(values.kittiMaxImages)
  if (kittiMaxImages) kitti.max_images = kittiMaxImages
  const validKittiCategories = (values.kittiCategories ?? []).map((c) => c.category.trim()).filter((c) => c !== '')
  if (validKittiCategories.length) kitti.categories = validKittiCategories
  if (Object.keys(kitti).length) config.kitti = kitti

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
