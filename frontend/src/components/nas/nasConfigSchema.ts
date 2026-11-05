import { z } from 'zod'

import type { NasConfig, NasConfigInput, NasProtocol } from '@/api/types'

// Same convention as jobCreateSchema: the port is bound to a plain <input>
// so it stays a string in the form and is only parsed in buildNasConfigPayload().
export const nasConfigSchema = z
  .object({
    protocol: z.enum(['smb', 'sftp', 'ftp', 'webdav', 'nfs', 'local']),
    host: z.string(),
    port: z.string(),
    share: z.string(),
    basePath: z.string(),
    storagePath: z.string(),
    useAsStorage: z.boolean(),
    authMode: z.enum(['anonymous', 'password']),
    username: z.string(),
    domain: z.string(),
    password: z.string(),
    useTls: z.boolean(),
    verifyTls: z.boolean(),
    // Not a user input: whether the server already holds a password, so a
    // blank password field is valid (keep it) instead of required.
    hasSavedPassword: z.boolean(),
  })
  .superRefine((data, ctx) => {
    const fail = (path: string, message: string) => ctx.addIssue({ code: 'custom', message, path: [path] })

    const host = data.host.trim()
    if (data.protocol !== 'local' && !host) fail('host', 'Enter the server address')
    // A pasted URL ("smb://nas/photos") is the most common mistake; the server rejects it too.
    else if (host && (host.includes('://') || /[\\/\s]/.test(host))) {
      fail('host', 'Enter only the host name or IP address, without smb:// or a folder path')
    }
    if (['smb', 'nfs', 'local'].includes(data.protocol) && !data.share.trim()) {
      fail(
        'share',
        data.protocol === 'smb'
          ? 'Enter the share name'
          : data.protocol === 'nfs'
            ? 'Enter the export path'
            : 'Enter the mounted path',
      )
    }

    if (data.protocol === 'local' && data.share.trim() && !data.share.trim().startsWith('/')) {
      fail('share', 'The mounted path must start with / (for example /mnt/nas)')
    }

    const port = data.port.trim()
    if (port) {
      const n = Number(port)
      if (!Number.isInteger(n) || n < 1 || n > 65535) fail('port', 'Port must be a number from 1 to 65535')
    }

    if (data.protocol === 'sftp' && data.authMode === 'anonymous') {
      fail('authMode', 'SFTP needs a username and password')
    }
    if (data.authMode === 'password' && ['smb', 'sftp', 'ftp', 'webdav'].includes(data.protocol)) {
      if (!data.username.trim()) fail('username', 'Enter a username')
      if (!data.password && !data.hasSavedPassword) fail('password', 'Enter a password')
    }

    if (data.useAsStorage && !data.storagePath.trim()) fail('storagePath', 'Enter a storage folder')
    if (hasParentSegment(data.basePath)) fail('basePath', 'Folder cannot contain ".."')
    if (hasParentSegment(data.storagePath)) fail('storagePath', 'Folder cannot contain ".."')
  })

export type NasConfigFormValues = z.infer<typeof nasConfigSchema>

export const nasConfigDefaultValues: NasConfigFormValues = {
  protocol: 'smb',
  host: '',
  port: '',
  share: '',
  basePath: '',
  storagePath: '',
  useAsStorage: false,
  authMode: 'anonymous',
  username: '',
  domain: '',
  password: '',
  useTls: false,
  verifyTls: true,
  hasSavedPassword: false,
}

function hasParentSegment(path: string): boolean {
  return path.split(/[\\/]/).some((segment) => segment.trim() === '..')
}

/** Protocols that take a username/password (nfs and local never do). */
export function protocolSupportsAuth(protocol: NasProtocol): boolean {
  return protocol === 'smb' || protocol === 'sftp' || protocol === 'ftp' || protocol === 'webdav'
}

/** Protocols where `use_tls` means something (FTPS / https). */
export function protocolSupportsTls(protocol: NasProtocol): boolean {
  return protocol === 'ftp' || protocol === 'webdav'
}

/** The port the backend falls back to when the field is left blank. */
export function defaultPortFor(protocol: NasProtocol, useTls: boolean): number | null {
  switch (protocol) {
    case 'smb':
      return 445
    case 'sftp':
      return 22
    case 'ftp':
      return 21
    case 'webdav':
      return useTls ? 443 : 80
    case 'nfs':
      return 2049
    case 'local':
      return null
  }
}

export function configToFormValues(config: NasConfig): NasConfigFormValues {
  return {
    protocol: config.protocol,
    host: config.host,
    port: config.port === null ? '' : String(config.port),
    share: config.share,
    basePath: config.base_path,
    storagePath: config.storage_path === 'data_automation' ? '' : (config.storage_path ?? ''),
    useAsStorage: Boolean(config.storage_path && config.storage_path !== 'data_automation' && config.use_as_storage),
    authMode: config.auth_mode,
    username: config.username,
    domain: config.domain,
    password: '',
    useTls: config.use_tls,
    verifyTls: config.verify_tls,
    hasSavedPassword: config.has_password,
  }
}

/** Builds the PUT /nas/config and POST /nas/test body. Fields that do not
 * apply to the chosen protocol are normalised away so a stale value from a
 * previously selected protocol never reaches the server. */
export function buildNasConfigPayload(values: NasConfigFormValues): NasConfigInput {
  const { protocol } = values
  const usesAuth = protocolSupportsAuth(protocol)
  const authMode = usesAuth ? values.authMode : 'anonymous'
  const withPassword = authMode === 'password'
  const useTls = protocolSupportsTls(protocol) && values.useTls
  const port = values.port.trim()

  const payload: NasConfigInput = {
    protocol,
    host: protocol === 'local' ? '' : values.host.trim(),
    port: protocol === 'local' || !port ? null : Number(port),
    share: values.share.trim(),
    base_path: values.basePath.trim(),
    storage_path: values.storagePath ? values.storagePath.trim() : '',
    use_as_storage: Boolean(values.storagePath && values.storagePath.trim() && values.useAsStorage),
    auth_mode: authMode,
    username: withPassword ? values.username.trim() : '',
    domain: withPassword && protocol === 'smb' ? values.domain.trim() : '',
    use_tls: useTls,
    verify_tls: useTls ? values.verifyTls : true,
  }
  // Omitted (not empty) so the server keeps the stored password.
  if (withPassword && values.password) payload.password = values.password
  return payload
}
