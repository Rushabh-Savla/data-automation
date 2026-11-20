import { useEffect, useState, type ReactNode } from 'react'
import { Controller, useForm, type UseFormReturn } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import {
  ActivityIcon,
  CheckCircle2Icon,
  EyeIcon,
  EyeOffIcon,
  XCircleIcon,
} from 'lucide-react'

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
  const displayMessage = result.message?.replace(/Source folder can be read\.?/gi, '').trim()

  return (
    <div
      role="status"
      className={cn(
        'flex flex-col gap-3 rounded-xl border p-4 text-sm',
        result.ok
          ? 'border-emerald-500/50 bg-emerald-500/10 text-emerald-900 dark:text-emerald-100'
          : 'border-destructive/50 bg-destructive/10 text-destructive',
      )}
    >
      <div className="flex items-center gap-2">
        {result.ok ? (
          <CheckCircle2Icon className="size-5 text-emerald-600 dark:text-emerald-400 shrink-0" />
        ) : (
          <XCircleIcon className="size-5 text-destructive shrink-0" />
        )}
        <span className={cn('text-base font-bold', result.ok ? 'text-emerald-700 dark:text-emerald-300' : 'text-destructive')}>
          {result.ok ? 'NAS connected' : 'NAS disconnected'}
        </span>
        <Badge variant={result.ok ? 'success' : 'destructive'} className="ml-auto text-xs">
          {result.ok ? 'Online' : 'Offline'}
        </Badge>
      </div>

      {displayMessage ? (
        <p className="text-xs">{displayMessage}</p>
      ) : null}

      {result.latency_ms !== null && (
        <div className="flex items-center gap-2 pt-1 border-t border-border/40">
          <span className="text-xs text-muted-foreground font-mono">Response time: {Math.round(result.latency_ms)} ms</span>
        </div>
      )}
    </div>
  )
}

export function NasConfigForm({
  config,
  form: formProp,
  onSwitchToTest,
  onTestResult,
}: {
  config: NasConfig
  form?: UseFormReturn<NasConfigFormValues>
  onSwitchToTest?: () => void
  onTestResult?: (result: NasTestResult) => void
}) {
  const saveNasConfig = useSaveNasConfig()
  const testConnection = useTestNasConnection()
  const [testResult, setTestResult] = useState<NasTestResult | null>(null)
  const [showPassword, setShowPassword] = useState(false)

  const fallbackForm = useForm<NasConfigFormValues>({
    resolver: zodResolver(nasConfigSchema),
    defaultValues: configToFormValues(config),
  })

  const form = formProp ?? fallbackForm
  const {
    register,
    control,
    handleSubmit,
    watch,
    setValue,
    reset,
    formState: { errors, isSubmitting },
  } = form

  // Re-sync after a save/remove refetches the config
  useEffect(() => {
    if (!formProp) {
      reset(configToFormValues(config))
    }
  }, [config, reset, formProp])

  // Only clear test result when user manually modifies a form field (type === 'change'),
  // preventing programmatic reset() from immediately erasing connection results.
  useEffect(() => {
    const subscription = watch((_value, { type }) => {
      if (type === 'change') {
        setTestResult(null)
      }
    })
    return () => subscription.unsubscribe()
  }, [watch])

  const activeResult: NasTestResult | null =
    testResult ??
    (config.configured
      ? {
        ok: true,
        can_read: true,
        can_write: true,
        message: 'Share can be read and written.',
        latency_ms: null,
        sample_files: [],
      }
      : null)

  const protocol = watch('protocol')
  const authMode = watch('authMode')
  const useTls = watch('useTls')
  const hasSavedPassword = watch('hasSavedPassword')

  const showAuth = protocolSupportsAuth(protocol)
  const showCredentials = showAuth && authMode === 'password'
  const showTls = protocolSupportsTls(protocol)
  const shareField = SHARE_LABELS[protocol]
  const defaultPort = defaultPortFor(protocol, useTls)

  function handleProtocolChange(next: NasProtocol) {
    setValue('protocol', next, { shouldDirty: true })
    const nextDefaultPort = defaultPortFor(next, useTls)
    setValue('port', nextDefaultPort ? String(nextDefaultPort) : '', { shouldDirty: true })
    // nfs/local never take credentials and sftp cannot be anonymous, so keep
    // the auth mode consistent instead of leaving an invalid combination.
    if (!protocolSupportsAuth(next)) setValue('authMode', 'anonymous')
    else if (next === 'sftp') setValue('authMode', 'password')
  }

  async function onSubmit(values: NasConfigFormValues) {
    const saved = await saveNasConfig.mutateAsync(buildNasConfigPayload(values)).catch(() => null)
    if (saved) {
      const res: NasTestResult = {
        ok: true,
        can_read: true,
        can_write: true,
        message: 'NAS connection saved and active.',
        latency_ms: null,
        sample_files: [],
      }
      setTestResult(res)
      onTestResult?.(res)
    }
  }

  async function onTest(values: NasConfigFormValues) {
    setTestResult(null)
    const result = await testConnection.mutateAsync(buildNasConfigPayload(values)).catch(() => null)
    if (result) {
      setTestResult(result)
      onTestResult?.(result)
    }
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
                <div className="relative flex items-center">
                  <Input
                    id="nas-password"
                    type={showPassword ? 'text' : 'password'}
                    autoComplete="new-password"
                    className="pr-10"
                    {...register('password')}
                    placeholder={hasSavedPassword ? 'Saved – leave blank to keep' : ''}
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="absolute right-0 h-full w-9 px-2 text-muted-foreground hover:text-foreground cursor-pointer"
                    onClick={() => setShowPassword((prev) => !prev)}
                    tabIndex={-1}
                    aria-label={showPassword ? 'Hide password' : 'Show password'}
                  >
                    {showPassword ? (
                      <EyeOffIcon className="size-4 shrink-0" />
                    ) : (
                      <EyeIcon className="size-4 shrink-0" />
                    )}
                  </Button>
                </div>
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

      {activeResult && <TestResultPanel result={activeResult} />}

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
        {onSwitchToTest && (
          <Button
            type="button"
            variant="ghost"
            onClick={onSwitchToTest}
            className="gap-1.5 text-xs text-muted-foreground hover:text-foreground"
          >
            <ActivityIcon className="size-3.5" />
            Open Real-Time Test Tab
          </Button>
        )}
        {config.configured && (
          <div className="ml-auto">
            <RemoveNasDialog />
          </div>
        )}
      </div>
    </form>
  )
}
