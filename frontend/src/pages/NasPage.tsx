import { Topbar } from '@/components/layout/Topbar'
import { NasConfigForm } from '@/components/nas/NasConfigForm'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { useNasConfig } from '@/hooks/useNasConfig'

export function NasPage() {
  const { data: config, isLoading, isError } = useNasConfig()

  return (
    <>
      <Topbar
        title="NAS"
        right={
          config && (
            <Badge variant={config.configured ? 'success' : 'secondary'}>
              {config.configured ? 'Connected' : 'Not configured'}
            </Badge>
          )
        }
      />

      <div className="flex flex-col gap-4 p-6">
        <p className="max-w-3xl text-sm text-muted-foreground">
          Connect a network drive to collect images from it as a source and, optionally, to keep the images that
          pass filtering. Credentials are encrypted and never shown again after saving.
        </p>

        {isLoading && (
          <div className="flex max-w-3xl flex-col gap-4">
            <Skeleton className="h-64 w-full" />
            <Skeleton className="h-40 w-full" />
          </div>
        )}
        {isError && <p className="text-sm text-destructive">Failed to load the NAS configuration.</p>}
        {config && <NasConfigForm config={config} />}
      </div>
    </>
  )
}
