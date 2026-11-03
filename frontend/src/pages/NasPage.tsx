import { useEffect, useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import {
  ActivityIcon,
  Settings2Icon,
  WifiIcon,
  WifiOffIcon,
} from 'lucide-react'

import { Topbar } from '@/components/layout/Topbar'
import type { NasTestResult } from '@/api/types'
import {
  configToFormValues,
  nasConfigSchema,
  type NasConfigFormValues,
} from '@/components/nas/nasConfigSchema'
import { NasConfigForm } from '@/components/nas/NasConfigForm'
import { NasTestConnectionTab } from '@/components/nas/NasTestConnectionTab'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useNasConfig } from '@/hooks/useNasConfig'

export function NasPage() {
  const { data: config, isLoading, isError } = useNasConfig()
  const [activeTab, setActiveTab] = useState<'configuration' | 'test-connection'>('configuration')
  const [liveStatus, setLiveStatus] = useState<'connected' | 'disconnected' | null>(null)
  const [testResult, setTestResult] = useState<NasTestResult | null>(null)

  const effectiveConfig = config ?? {
    configured: false,
    protocol: 'smb' as const,
    host: '',
    port: null,
    share: '',
    base_path: '',
    storage_path: 'data_automation',
    use_as_storage: true,
    auth_mode: 'anonymous' as const,
    username: '',
    domain: '',
    has_password: false,
    use_tls: false,
    verify_tls: true,
    updated_at: null,
  }

  const form = useForm<NasConfigFormValues>({
    resolver: zodResolver(nasConfigSchema),
    defaultValues: configToFormValues(effectiveConfig),
  })

  // Synchronize form values whenever saved config is updated from server
  useEffect(() => {
    if (config) {
      form.reset(configToFormValues(config))
    }
  }, [config, form])

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
              {liveStatus === 'connected' ? (
                <Badge variant="success" className="bg-emerald-600 text-white flex items-center gap-1.5 font-medium">
                  <WifiIcon className="size-3.5" />
                  NAS connected
                </Badge>
              ) : liveStatus === 'disconnected' ? (
                <Badge variant="destructive" className="flex items-center gap-1.5 font-medium">
                  <WifiOffIcon className="size-3.5" />
                  NAS disconnected
                </Badge>
              ) : (
                <Badge variant={config.configured ? 'success' : 'secondary'}>
                  {config.configured ? 'Connected' : 'Not configured'}
                </Badge>
              )}
            </div>
          )
        }
      />

      <div className="flex flex-col gap-6 p-6 pb-16">
        <div className="max-w-3xl space-y-1">
          <h2 className="text-lg font-semibold tracking-tight">Network-Attached Storage Management</h2>
          <p className="text-sm text-muted-foreground">
            Connect network drives (SMB, SFTP, FTP, WebDAV, NFS, or local mount) to source images for dataset jobs,
            and test connectivity in real-time.
          </p>
        </div>

        {isLoading && (
          <div className="flex max-w-3xl flex-col gap-4">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-64 w-full" />
            <Skeleton className="h-40 w-full" />
          </div>
        )}

        {isError && (
          <div className="max-w-3xl rounded-lg border border-destructive/20 bg-destructive/10 p-3 text-xs text-destructive">
            Could not reach backend API server. Showing default configuration interface.
          </div>
        )}

        <Tabs
          value={activeTab}
          onValueChange={(v) => setActiveTab(v as 'configuration' | 'test-connection')}
          className="max-w-3xl flex flex-col gap-6"
        >
          <TabsList className="grid w-full grid-cols-2 h-11 p-1 bg-muted/60">
            <TabsTrigger value="configuration" className="gap-2 text-xs sm:text-sm font-medium">
              <Settings2Icon className="size-4 shrink-0" />
              <span>Configuration</span>
            </TabsTrigger>
            <TabsTrigger value="test-connection" className="gap-2 text-xs sm:text-sm font-medium">
              <ActivityIcon className="size-4 shrink-0" />
              <span>Test Connection</span>
              {liveStatus === 'connected' && (
                <span className="size-2 rounded-full bg-emerald-500 inline-block" />
              )}
              {liveStatus === 'disconnected' && (
                <span className="size-2 rounded-full bg-destructive inline-block" />
              )}
            </TabsTrigger>
          </TabsList>

          <TabsContent value="configuration" className="mt-0 outline-none">
            <NasConfigForm
              config={effectiveConfig}
              form={form}
              onSwitchToTest={() => setActiveTab('test-connection')}
              onTestResult={(result) => {
                setTestResult(result)
                setLiveStatus(result.ok ? 'connected' : 'disconnected')
              }}
            />
          </TabsContent>

          <TabsContent value="test-connection" className="mt-0 outline-none">
            <NasTestConnectionTab
              config={effectiveConfig}
              form={form}
              testResult={testResult}
              onTestResultChange={setTestResult}
              onNavigateToConfig={() => setActiveTab('configuration')}
              onStatusChange={setLiveStatus}
            />
          </TabsContent>
        </Tabs>
      </div>
    </>
  )
}
