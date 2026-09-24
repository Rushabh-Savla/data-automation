import { useEffect, useState, type ReactNode } from 'react'
import { Controller, useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { CheckCircle2Icon, XCircleIcon } from 'lucide-react'

import type { NasConfig, NasProtocol, NasTestResult } from '@/api/types'
import {
  buildNasConfigPayload,
  configToFormValues,
  defaultPortFor,
  nasConfigSchema,
  protocolSupportsAuth,
  protocolSupportsTls,
  type NasConfigFormValues,
} from '@/components/nas/nasConfigSchema'
import { RemoveNasDialog } from '@/components/nas/RemoveNasDialog'
import { useSaveNasConfig } from '@/hooks/useSaveNasConfig'
import { useTestNasConnection } from '@/hooks/useTestNasConnection'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { cn } from '@/lib/utils'

const PROTOCOL_OPTIONS: { value: NasProtocol; label: string }[] = [
  { value: 'smb', label: 'SMB / CIFS (Windows, Synology, QNAP)' },
  { value: 'sftp', label: 'SFTP (SSH)' },
  { value: 'ftp', label: 'FTP / FTPS' },
  { value: 'webdav', label: 'WebDAV' },
  { value: 'nfs', label: 'NFS (needs optional server library)' },
  { value: 'local', label: 'Mounted path (also for NFS)' },
]

const SHARE_LABELS: Partial<Record<NasProtocol, { label: string; placeholder: string; hint?: string }>> = {
  smb: { label: 'Share name', placeholder: 'e.g. datasets' },
  nfs: { label: 'Export path', placeholder: 'e.g. /volume1/datasets' },
  webdav: {
    label: 'URL path prefix (optional)',
    placeholder: 'e.g. /remote.php/dav/files/me',
  },
  local: {
    label: 'Mounted path',
    placeholder: 'e.g. /mnt/nas',
    hint: 'Path inside the container where the NAS is mounted. Use this for an NFS mount done on the host.',
  },
}

function Field({
  label,
  htmlFor,
  error,
  hint,
  className,
  children,
}: {
  label: string
  htmlFor: string
  error?: string
  hint?: ReactNode
  className?: string
  children: ReactNode
}) {
  return (
    <div className={cn('flex flex-col gap-1.5', className)}>
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
      {error ? (
        <p className="text-xs text-destructive">{error}</p>
      ) : (
        hint && <p className="text-xs text-muted-foreground">{hint}</p>
      )}
    </div>
  )
}

function TestResultPanel({ result }: { result: NasTestResult }) {
  return (
    <div
      role="status"
      className={cn(
        'flex flex-col gap-2 rounded-md border p-3 text-sm',
        result.ok
          ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400'
          : 'border-destructive/40 bg-destructive/10 text-destructive',
      )}
    >
      <div className="flex items-start gap-2">
        {result.ok ? (
          <CheckCircle2Icon className="mt-0.5 size-4 shrink-0" />
        ) : (
          <XCircleIcon className="mt-0.5 size-4 shrink-0" />
        )}
        <span>{result.message}</span>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant={result.can_read ? 'success' : 'destructive'}>
          {result.can_read ? 'Can read' : 'Cannot read'}
        </Badge>
        <Badge variant={result.can_write ? 'success' : 'destructive'}>
          {result.can_write ? 'Can write' : 'Cannot write'}
        </Badge>
        {result.latency_ms !== null && (
          <span className="text-xs text-muted-foreground">{Math.round(result.latency_ms)} ms</span>
        )}
      </div>
      {result.sample_files.length > 0 && (
        <div className="flex flex-col gap-1">
          <span className="text-xs text-muted-foreground">Sample files found</span>
          <ul className="font-mono text-xs text-foreground">
            {result.sample_files.map((file) => (
              <li key={file} className="truncate">
                {file}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}

export function NasConfigForm({ config }: { config: NasConfig }) {
  const saveNasConfig = useSaveNasConfig()
  const testConnection = useTestNasConnection()
  const [testResult, setTestResult] = useState<NasTestResult | null>(null)

  const {
    register,
    control,
    handleSubmit,
    watch,
    setValue,
    reset,
    formState: { errors, isSubmitting },
  } = useForm<NasConfigFormValues>({
    resolver: zodResolver(nasConfigSchema),
    defaultValues: configToFormValues(config),
  })

  // Re-sync after a save/remove refetches the config (also clears the
  // password field, which is never returned by the API).
  useEffect(() => {
    reset(configToFormValues(config))
    setTestResult(null)
  }, [config, reset])

  // A test result describes the values it ran with, so drop it once they change.
  useEffect(() => {
    const subscription = watch(() => setTestResult(null))
    return () => subscription.unsubscribe()
  }, [watch])

  const protocol = watch('protocol')
  const authMode = watch('authMode')
  const useTls = watch('useTls')
  const useAsStorage = watch('useAsStorage')
  const hasSavedPassword = watch('hasSavedPassword')

  const showAuth = protocolSupportsAuth(protocol)
  const showCredentials = showAuth && authMode === 'password'
  const showTls = protocolSupportsTls(protocol)
  const shareField = SHARE_LABELS[protocol]
  const defaultPort = defaultPortFor(protocol, useTls)

  function handleProtocolChange(next: NasProtocol) {
    setValue('protocol', next, { shouldDirty: true })
    // nfs/local never take credentials and sftp cannot be anonymous, so keep
    // the auth mode consistent instead of leaving an invalid combination.
    if (!protocolSupportsAuth(next)) setValue('authMode', 'anonymous')
    else if (next === 'sftp') setValue('authMode', 'password')
  }

  async function onSubmit(values: NasConfigFormValues) {
    await saveNasConfig.mutateAsync(buildNasConfigPayload(values)).catch(() => undefined)
  }

  async function onTest(values: NasConfigFormValues) {
    setTestResult(null)
    const result = await testConnection.mutateAsync(buildNasConfigPayload(values)).catch(() => null)
    if (result) setTestResult(result)
  }

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="flex max-w-3xl flex-col gap-6">
      <Card className="gap-4 p-4">
        <h2 className="text-sm font-medium">Connection</h2>

        <Field label="Protocol" htmlFor="nas-protocol">
          <Controller
            control={control}
            name="protocol"
            render={({ field }) => (
              <Select value={field.value} onValueChange={(v) => handleProtocolChange(v as NasProtocol)}>
                <SelectTrigger id="nas-protocol">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PROTOCOL_OPTIONS.map(({ value, label }) => (
                    <SelectItem key={value} value={value}>
                      {label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          />
        </Field>

        {protocol !== 'local' && (
          <div className="grid gap-4 sm:grid-cols-[1fr_10rem]">
            <Field label="Host" htmlFor="nas-host" error={errors.host?.message}>
              <Input id="nas-host" {...register('host')} placeholder="nas.local or 192.168.1.10" />
            </Field>
            <Field label="Port" htmlFor="nas-port" error={errors.port?.message}>
              <Input
                id="nas-port"
                inputMode="numeric"
                {...register('port')}
                placeholder={defaultPort === null ? '' : String(defaultPort)}
              />
            </Field>
          </div>
        )}

        {shareField && (
          <Field label={shareField.label} htmlFor="nas-share" error={errors.share?.message} hint={shareField.hint}>
            <Input id="nas-share" {...register('share')} placeholder={shareField.placeholder} />
          </Field>
        )}

        {showTls && (
          <div className="flex flex-col gap-2">
            <Label className="flex items-center gap-2">
              <Controller
                control={control}
                name="useTls"
                render={({ field }) => (
                  <Checkbox checked={field.value} onCheckedChange={(c) => field.onChange(c === true)} />
                )}
              />
              {protocol === 'ftp' ? 'Use FTPS (encrypted connection)' : 'Use HTTPS'}
            </Label>
            {useTls && (
              <Label className="flex items-center gap-2">
                <Controller
                  control={control}
                  name="verifyTls"
                  render={({ field }) => (
                    <Checkbox checked={field.value} onCheckedChange={(c) => field.onChange(c === true)} />
                  )}
                />
                Verify the server certificate
              </Label>
            )}
          </div>
        )}
      </Card>

      {showAuth && (
        <Card className="gap-4 p-4">
          <h2 className="text-sm font-medium">Sign-in</h2>

          <Field label="Authentication" htmlFor="nas-auth-mode" error={errors.authMode?.message}>
            <Controller
              control={control}
              name="authMode"
              render={({ field }) => (
                <Select value={field.value} onValueChange={field.onChange}>
                  <SelectTrigger id="nas-auth-mode">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {protocol !== 'sftp' && <SelectItem value="anonymous">Open – no password</SelectItem>}
                    <SelectItem value="password">Username &amp; password</SelectItem>
                  </SelectContent>
                </Select>
              )}
            />
          </Field>

          {showCredentials && (
            <div className="grid gap-4 sm:grid-cols-2">
              <Field label="Username" htmlFor="nas-username" error={errors.username?.message}>
                <Input id="nas-username" autoComplete="off" {...register('username')} />
              </Field>
              <Field label="Password" htmlFor="nas-password" error={errors.password?.message}>
                <Input
                  id="nas-password"
                  type="password"
                  autoComplete="new-password"
                  {...register('password')}
                  placeholder={hasSavedPassword ? 'Saved – leave blank to keep' : ''}
                />
              </Field>
              {protocol === 'smb' && (
                <Field label="Domain (optional)" htmlFor="nas-domain" className="sm:col-span-2">
                  <Input id="nas-domain" autoComplete="off" {...register('domain')} placeholder="WORKGROUP" />
                </Field>
              )}
            </div>
          )}
        </Card>
      )}

      <Card className="gap-4 p-4">
        <h2 className="text-sm font-medium">Folders</h2>

        <Field
          label="Source folder"
          htmlFor="nas-base-path"
          error={errors.basePath?.message}
          hint="Where jobs look for existing images on the NAS. Leave blank to search the whole share."
        >
          <Input id="nas-base-path" {...register('basePath')} placeholder="e.g. photos/2024" />
        </Field>

        <div className="flex flex-col gap-2">
          <Label className="flex items-center gap-2">
            <Controller
              control={control}
              name="useAsStorage"
              render={({ field }) => (
                <Checkbox checked={field.value} onCheckedChange={(c) => field.onChange(c === true)} />
              )}
            />
            Store filtered images on this NAS
          </Label>
          <p className="text-xs text-muted-foreground">
            When on, images that pass filtering are moved to the NAS and removed from local disk. When off or not
            connected, local storage is used.
          </p>
        </div>

        {useAsStorage && (
          <Field
            label="Storage folder"
            htmlFor="nas-storage-path"
            error={errors.storagePath?.message}
            hint="Filtered images are saved under this folder, in one sub-folder per job."
          >
            <Input id="nas-storage-path" {...register('storagePath')} placeholder="data_automation" />
          </Field>
        )}
      </Card>

      {testResult && <TestResultPanel result={testResult} />}

      <div className="flex flex-wrap items-center gap-2">
        <Button type="submit" disabled={isSubmitting || saveNasConfig.isPending}>
          {saveNasConfig.isPending ? 'Saving…' : 'Save'}
        </Button>
        <Button
          type="button"
          variant="outline"
          disabled={testConnection.isPending}
          onClick={handleSubmit(onTest)}
        >
          {testConnection.isPending ? 'Testing…' : 'Test connection'}
        </Button>
        {config.configured && (
          <div className="ml-auto">
            <RemoveNasDialog />
          </div>
        )}
      </div>
    </form>
  )
}
