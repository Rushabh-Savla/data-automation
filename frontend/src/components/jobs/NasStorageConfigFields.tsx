import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useNasConfig } from '@/hooks/useNasConfig'

// Mirrors app/core/slugify.py::job_folder_name — this preview can't know the
// job's real UUID yet (it's assigned on submit), so it shows a placeholder
// suffix in its place.
function previewSlug(name: string | undefined): string {
  const slug = (name ?? '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60)
    .replace(/^-+|-+$/g, '')
  return slug ? `${slug}-<id>` : 'enter-a-job-name-above-<id>'
}

export function NasStorageConfigFields({ jobName }: { jobName?: string }) {
  const { data: config } = useNasConfig()

  if (!config?.use_as_storage || !config.storage_path) return null

  const folder = previewSlug(jobName)

  return (
    <Card className="gap-2 p-4">
      <CardHeader className="p-0">
        <CardTitle className="text-sm">NAS Storage</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-1.5 p-0">
        <p className="text-sm text-muted-foreground">Filtered images for this job will be saved on the NAS at:</p>
        <code className="w-fit rounded bg-muted px-2 py-1 text-xs">
          {config.storage_path}/{folder}/...
        </code>
        <p className="text-xs text-muted-foreground">
          The folder name comes from the job name above, with a 4-character id suffix appended to keep it unique.
          Configure the NAS storage base folder on the{' '}
          <a href="/nas" className="underline">
            NAS page
          </a>
          .
        </p>
      </CardContent>
    </Card>
  )
}
