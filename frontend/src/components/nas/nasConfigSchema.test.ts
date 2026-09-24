import { describe, expect, it } from 'vitest'

import type { NasConfig } from '@/api/types'
import {
  buildNasConfigPayload,
  configToFormValues,
  defaultPortFor,
  nasConfigDefaultValues,
  nasConfigSchema,
  type NasConfigFormValues,
} from '@/components/nas/nasConfigSchema'

function values(overrides: Partial<NasConfigFormValues>): NasConfigFormValues {
  return { ...nasConfigDefaultValues, host: 'nas.local', share: 'data', ...overrides }
}

function errorPaths(v: NasConfigFormValues): string[] {
  const result = nasConfigSchema.safeParse(v)
  return result.success ? [] : result.error.issues.map((i) => String(i.path[0]))
}

describe('nasConfigSchema validation', () => {
  it('accepts an anonymous SMB share', () => {
    expect(errorPaths(values({}))).toEqual([])
  })

  it('requires a host unless the protocol is local', () => {
    expect(errorPaths(values({ host: '  ' }))).toContain('host')
    expect(errorPaths(values({ protocol: 'local', host: '', share: '/mnt/nas' }))).toEqual([])
  })

  it('rejects a pasted URL or path as the host, matching the server', () => {
    for (const host of ['smb://nas/photos', 'nas/photos', 'my nas', 'http://nas']) {
      expect(errorPaths(values({ host }))).toContain('host')
    }
    expect(errorPaths(values({ host: '192.168.1.20' }))).toEqual([])
    expect(errorPaths(values({ host: 'nas.local' }))).toEqual([])
  })

  it('requires an absolute path for a mounted folder, matching the server', () => {
    expect(errorPaths(values({ protocol: 'local', host: '', share: 'mnt/nas' }))).toContain('share')
    expect(errorPaths(values({ protocol: 'local', host: '', share: '/mnt/nas' }))).toEqual([])
  })

  it('requires a share for smb, nfs and local but not sftp/ftp/webdav', () => {
    expect(errorPaths(values({ protocol: 'smb', share: '' }))).toContain('share')
    expect(errorPaths(values({ protocol: 'nfs', share: '' }))).toContain('share')
    expect(errorPaths(values({ protocol: 'local', host: '', share: '' }))).toContain('share')
    expect(errorPaths(values({ protocol: 'ftp', share: '' }))).toEqual([])
    expect(errorPaths(values({ protocol: 'webdav', share: '' }))).toEqual([])
    expect(
      errorPaths(values({ protocol: 'sftp', share: '', authMode: 'password', username: 'u', password: 'p' })),
    ).toEqual([])
  })

  it('rejects anonymous SFTP', () => {
    expect(errorPaths(values({ protocol: 'sftp', share: '', authMode: 'anonymous' }))).toContain('authMode')
  })

  it('requires a username when using a password', () => {
    expect(errorPaths(values({ authMode: 'password', password: 'p' }))).toContain('username')
    expect(errorPaths(values({ authMode: 'password', username: 'u', password: 'p' }))).toEqual([])
  })

  it('requires a password only when none is saved', () => {
    const base = { authMode: 'password', username: 'u', password: '' } as const
    expect(errorPaths(values({ ...base, hasSavedPassword: false }))).toContain('password')
    expect(errorPaths(values({ ...base, hasSavedPassword: true }))).toEqual([])
  })

  it('does not ask for credentials on nfs or local', () => {
    expect(errorPaths(values({ protocol: 'nfs', authMode: 'password' }))).toEqual([])
    expect(errorPaths(values({ protocol: 'local', host: '', share: '/mnt', authMode: 'password' }))).toEqual([])
  })

  it('requires a storage folder only when storing on the NAS', () => {
    expect(errorPaths(values({ useAsStorage: true, storagePath: ' ' }))).toContain('storagePath')
    expect(errorPaths(values({ useAsStorage: false, storagePath: '' }))).toEqual([])
  })

  it('rejects parent-directory segments in either folder', () => {
    expect(errorPaths(values({ basePath: 'photos/../secret' }))).toContain('basePath')
    expect(errorPaths(values({ storagePath: '..\\out' }))).toContain('storagePath')
    expect(errorPaths(values({ basePath: 'photos/a..b/..c' }))).toEqual([])
  })

  it('validates the port range', () => {
    expect(errorPaths(values({ port: '0' }))).toContain('port')
    expect(errorPaths(values({ port: '65536' }))).toContain('port')
    expect(errorPaths(values({ port: 'abc' }))).toContain('port')
    expect(errorPaths(values({ port: '4.5' }))).toContain('port')
    expect(errorPaths(values({ port: '445' }))).toEqual([])
    expect(errorPaths(values({ port: '' }))).toEqual([])
  })
})

describe('defaultPortFor', () => {
  it('returns the protocol defaults', () => {
    expect(defaultPortFor('smb', false)).toBe(445)
    expect(defaultPortFor('sftp', false)).toBe(22)
    expect(defaultPortFor('ftp', true)).toBe(21)
    expect(defaultPortFor('nfs', false)).toBe(2049)
    expect(defaultPortFor('local', false)).toBeNull()
  })

  it('picks the webdav port from TLS', () => {
    expect(defaultPortFor('webdav', false)).toBe(80)
    expect(defaultPortFor('webdav', true)).toBe(443)
  })
})

describe('buildNasConfigPayload', () => {
  it('trims strings and parses the port', () => {
    const payload = buildNasConfigPayload(
      values({ host: ' nas.local ', port: ' 4450 ', share: ' data ', basePath: ' a/b ', storagePath: ' out ' }),
    )
    expect(payload).toMatchObject({
      host: 'nas.local',
      port: 4450,
      share: 'data',
      base_path: 'a/b',
      storage_path: 'out',
    })
  })

  it('sends a null port when blank', () => {
    expect(buildNasConfigPayload(values({ port: '' })).port).toBeNull()
  })

  it('omits the password for anonymous access and clears credentials', () => {
    const payload = buildNasConfigPayload(
      values({ authMode: 'anonymous', username: 'leftover', domain: 'D', password: 'leftover' }),
    )
    expect(payload.auth_mode).toBe('anonymous')
    expect(payload.username).toBe('')
    expect(payload.domain).toBe('')
    expect('password' in payload).toBe(false)
  })

  it('sends username and password for password auth', () => {
    const payload = buildNasConfigPayload(
      values({ authMode: 'password', username: ' bob ', password: 'secret', domain: ' WG ' }),
    )
    expect(payload).toMatchObject({ auth_mode: 'password', username: 'bob', domain: 'WG', password: 'secret' })
  })

  it('omits a blank password so the stored one is kept', () => {
    const payload = buildNasConfigPayload(
      values({ authMode: 'password', username: 'bob', password: '', hasSavedPassword: true }),
    )
    expect('password' in payload).toBe(false)
  })

  it('keeps the domain for smb only', () => {
    const payload = buildNasConfigPayload(
      values({ protocol: 'ftp', authMode: 'password', username: 'u', password: 'p', domain: 'WG' }),
    )
    expect(payload.domain).toBe('')
  })

  it('forces anonymous and drops host for local, and ignores auth for nfs', () => {
    const local = buildNasConfigPayload(
      values({ protocol: 'local', host: 'stale', port: '22', share: '/mnt/nas', authMode: 'password' }),
    )
    expect(local).toMatchObject({ host: '', port: null, auth_mode: 'anonymous', share: '/mnt/nas' })
    const nfs = buildNasConfigPayload(values({ protocol: 'nfs', authMode: 'password', username: 'u', password: 'p' }))
    expect(nfs).toMatchObject({ auth_mode: 'anonymous', username: '' })
    expect('password' in nfs).toBe(false)
  })

  it('applies TLS options only to ftp and webdav', () => {
    expect(buildNasConfigPayload(values({ protocol: 'ftp', useTls: true, verifyTls: false }))).toMatchObject({
      use_tls: true,
      verify_tls: false,
    })
    expect(buildNasConfigPayload(values({ protocol: 'smb', useTls: true, verifyTls: false }))).toMatchObject({
      use_tls: false,
      verify_tls: true,
    })
    expect(buildNasConfigPayload(values({ protocol: 'webdav', useTls: false, verifyTls: false }))).toMatchObject({
      use_tls: false,
      verify_tls: true,
    })
  })
})

describe('configToFormValues', () => {
  it('maps a saved config and never carries a password', () => {
    const config: NasConfig = {
      configured: true,
      protocol: 'sftp',
      host: 'h',
      port: 2222,
      share: '',
      base_path: 'in',
      storage_path: 'out',
      use_as_storage: false,
      auth_mode: 'password',
      username: 'u',
      domain: '',
      has_password: true,
      use_tls: false,
      verify_tls: true,
      updated_at: null,
    }
    expect(configToFormValues(config)).toMatchObject({
      protocol: 'sftp',
      port: '2222',
      basePath: 'in',
      storagePath: 'out',
      useAsStorage: false,
      authMode: 'password',
      password: '',
      hasSavedPassword: true,
    })
    expect(configToFormValues({ ...config, port: null }).port).toBe('')
  })
})
