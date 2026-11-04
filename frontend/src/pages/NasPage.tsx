import { useState } from 'react'

import { Topbar } from '@/components/layout/Topbar'
import type { NasTestResult } from '@/api/types'
import { NasConfigForm } from '@/components/nas/NasConfigForm'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { useNasConfig } from '@/hooks/useNasConfig'

export function NasPage() {
  const { data: config, isLoading, isError } = useNasConfig()
  const [testResult, setTestResult] = useState<NasTestResult | null>(null)

  return (
    <>
      <Topbar
        title="NAS"
        right={
          config && (
            <div className="flex items-center gap-2">
              {config.configured && (
                <Badge variant="outline" className="text-xs font-mono uppercase">
                  {config.protocol}
                </Badge>
              )}
              {testResult ? (
                <Badge variant={testResult.ok ? 'success' : 'destructive'}>
                  {testResult.ok ? 'Connected' : 'Disconnected'}
                </Badge>
              ) : (
                <Badge variant={config.configured ? 'outline' : 'secondary'}>
                  {config.configured ? 'Configured' : 'Not configured'}
                </Badge>
              )}
            </div>
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
        {config && <NasConfigForm config={config} onTestResult={setTestResult} />}
      </div>
    </>
  )
}
