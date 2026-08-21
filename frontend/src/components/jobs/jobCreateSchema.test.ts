import { describe, expect, it } from 'vitest'

import {
  buildConfigPayload,
  buildJobCreatePayload,
  jobCreateDefaultValues,
  jobCreateSchema,
} from '@/components/jobs/jobCreateSchema'

describe('jobCreateSchema validation', () => {
  it('rejects an empty query', () => {
    const result = jobCreateSchema.safeParse({ ...jobCreateDefaultValues, query: '' })
    expect(result.success).toBe(false)
  })

  it('requires target count or deadline when neither is enabled', () => {
    const result = jobCreateSchema.safeParse({
      ...jobCreateDefaultValues,
      query: 'test',
      targetCountEnabled: false,
      deadlineEnabled: false,
    })
    expect(result.success).toBe(false)
    if (!result.success) {
      expect(result.error.issues.some((i) => i.path.includes('targetCountEnabled'))).toBe(true)
    }
  })

  it('accepts target count alone', () => {
    const result = jobCreateSchema.safeParse({
      ...jobCreateDefaultValues,
      query: 'test',
      targetCountEnabled: true,
      targetCount: 100,
      deadlineEnabled: false,
    })
    expect(result.success).toBe(true)
  })

  it('accepts deadline alone', () => {
    const result = jobCreateSchema.safeParse({
      ...jobCreateDefaultValues,
      query: 'test',
      targetCountEnabled: false,
      deadlineEnabled: true,
      deadline: '2026-01-01T00:00',
    })
    expect(result.success).toBe(true)
  })

  it('requires a target count value when the toggle is enabled', () => {
    const result = jobCreateSchema.safeParse({
      ...jobCreateDefaultValues,
      query: 'test',
      targetCountEnabled: true,
      targetCount: undefined,
    })
    expect(result.success).toBe(false)
  })

  it('requires at least one source when auto-sources is off', () => {
    const result = jobCreateSchema.safeParse({
      ...jobCreateDefaultValues,
      query: 'test',
      useAutoSources: false,
      sources: [],
    })
    expect(result.success).toBe(false)
  })

  it('rejects a non-numeric advanced numeric field', () => {
    const result = jobCreateSchema.safeParse({
      ...jobCreateDefaultValues,
      query: 'test',
      kaggleMaxDatasets: 'not-a-number',
    })
    expect(result.success).toBe(false)
  })
})

describe('buildConfigPayload', () => {
  it('returns an empty object when nothing was touched', () => {
    const config = buildConfigPayload(jobCreateDefaultValues)
    expect(config).toEqual({})
  })

  it('omits per-source keys the user never filled in, even if the source is active', () => {
    const config = buildConfigPayload({ ...jobCreateDefaultValues, useAutoSources: true })
    expect(config.kaggle).toBeUndefined()
    expect(config.roboflow).toBeUndefined()
  })

  it('includes only the roboflow fields that were actually set', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      roboflowMaxProjects: '10',
    })
    expect(config.roboflow).toEqual({ max_projects: 10 })
  })

  it('parses explicit kaggle slug rows, dropping incomplete ones', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      kaggleSlugs: [
        { slug: 'owner/dataset', license: 'CC0' },
        { slug: '', license: '' }, // incomplete row, must be dropped
      ],
    })
    expect(config.kaggle?.slugs).toEqual([['owner/dataset', 'CC0']])
  })

  it('only sets min_resolution when both width and height are provided', () => {
    const widthOnly = buildConfigPayload({ ...jobCreateDefaultValues, minWidth: '100' })
    expect(widthOnly.min_resolution).toBeUndefined()

    const both = buildConfigPayload({ ...jobCreateDefaultValues, minWidth: '100', minHeight: '200' })
    expect(both.min_resolution).toEqual([100, 200])
  })

  it('parses roboflow explicit project override rows with a numeric version', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      roboflowProjects: [{ workspace: 'workspace-a', project: 'project-b', version: '2', license: 'CC BY 4.0' }],
    })
    expect(config.roboflow?.projects).toEqual([['workspace-a', 'project-b', 2, 'CC BY 4.0']])
  })

  it('ignores garbage numeric strings rather than sending NaN', () => {
    const config = buildConfigPayload({ ...jobCreateDefaultValues, dedupHammingThreshold: 'abc' })
    expect(config.dedup_hamming_threshold).toBeUndefined()
  })

  it('parses explicit coco category overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      cocoCategories: [{ category: 'dog' }, { category: '  ' }, { category: 'car' }],
    })
    expect(config.coco?.categories).toEqual(['dog', 'car'])
  })

  it('parses explicit open images class overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      openImagesClasses: [{ className: 'Dog' }, { className: '' }],
    })
    expect(config.open_images?.classes).toEqual(['Dog'])
  })

  it('parses explicit pascal voc category overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      pascalVocCategories: [{ category: 'person' }, { category: '' }],
    })
    expect(config.pascal_voc?.categories).toEqual(['person'])
  })

  it('parses explicit visual genome object-name overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      visualGenomeCategories: [{ category: 'sidewalk' }, { category: '' }],
    })
    expect(config.visual_genome?.categories).toEqual(['sidewalk'])
  })

  it('parses explicit davis sequence overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      davisSequences: [{ sequence: 'dog' }, { sequence: '' }],
    })
    expect(config.davis?.sequences).toEqual(['dog'])
  })

  it('parses explicit sun rgbd category overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      sunRgbdCategories: [{ category: 'chair' }, { category: '' }],
    })
    expect(config.sun_rgbd?.categories).toEqual(['chair'])
  })

  it('parses explicit nuscenes category overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      nuscenesCategories: [{ category: 'vehicle.car' }, { category: '' }],
    })
    expect(config.nuscenes?.categories).toEqual(['vehicle.car'])
  })

  it('parses explicit kitti category overrides, dropping blank rows', () => {
    const config = buildConfigPayload({
      ...jobCreateDefaultValues,
      kittiCategories: [{ category: 'Car' }, { category: '' }],
    })
    expect(config.kitti?.categories).toEqual(['Car'])
  })
})

describe('buildJobCreatePayload', () => {
  it('omits sources entirely when Auto is selected, triggering backend auto-fan-out', () => {
    const payload = buildJobCreatePayload({ ...jobCreateDefaultValues, query: 'forklift', useAutoSources: true })
    expect(payload.sources).toBeUndefined()
  })

  it('sends the explicit source list when Auto is off', () => {
    const payload = buildJobCreatePayload({
      ...jobCreateDefaultValues,
      query: 'forklift',
      useAutoSources: false,
      sources: ['kaggle', 'web'],
    })
    expect(payload.sources).toEqual(['kaggle', 'web'])
  })

  it('omits target_count when the toggle is off, even if a stale value lingers', () => {
    const payload = buildJobCreatePayload({
      ...jobCreateDefaultValues,
      query: 'forklift',
      targetCountEnabled: false,
      targetCount: 500,
    })
    expect(payload.target_count).toBeUndefined()
  })

  it('converts the datetime-local deadline string to an ISO string', () => {
    const payload = buildJobCreatePayload({
      ...jobCreateDefaultValues,
      query: 'forklift',
      deadlineEnabled: true,
      deadline: '2026-06-01T12:00',
    })
    expect(payload.deadline_at).toBe(new Date('2026-06-01T12:00').toISOString())
  })

  it('trims an empty name to undefined rather than sending an empty string', () => {
    const payload = buildJobCreatePayload({ ...jobCreateDefaultValues, query: 'forklift', name: '   ' })
    expect(payload.name).toBeUndefined()
  })
})
