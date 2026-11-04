import '@testing-library/jest-dom/vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import type { NasConfig } from '@/api/types'
import { NasConfigForm } from '@/components/nas/NasConfigForm'

vi.mock('@/api/nas', () => ({
  getNasConfig: vi.fn(),
  saveNasConfig: vi.fn(),
  deleteNasConfig: vi.fn(),
  testNasConnection: vi.fn(),
}))

// Radix Checkbox measures itself with ResizeObserver, which jsdom lacks.
vi.stubGlobal(
  'ResizeObserver',
  class {
    observe() {}
    unobserve() {}
    disconnect() {}
  },
)

const baseConfig: NasConfig = {
  configured: false,
  protocol: 'smb',
  host: '',
  port: null,
  share: '',
  base_path: '',
  storage_path: 'data_automation',
  use_as_storage: true,
  auth_mode: 'anonymous',
  username: '',
  domain: '',
  has_password: false,
  use_tls: false,
  verify_tls: true,
  updated_at: null,
}

function renderForm(config: NasConfig) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={queryClient}>
      <NasConfigForm config={config} />
    </QueryClientProvider>,
  )
}

describe('NasConfigForm', () => {
  it('shows host, share and no credentials for an anonymous SMB share', () => {
    renderForm(baseConfig)
    expect(screen.getByLabelText('Host')).toBeInTheDocument()
    expect(screen.getByLabelText('Share name')).toBeInTheDocument()
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /remove connection/i })).not.toBeInTheDocument()
  })

  it('shows credentials, the domain and a keep-password hint when a password is saved', () => {
    renderForm({
      ...baseConfig,
      configured: true,
      auth_mode: 'password',
      username: 'bob',
      has_password: true,
    })
    expect(screen.getByLabelText('Username')).toHaveValue('bob')
    expect(screen.getByLabelText('Password')).toHaveAttribute('placeholder', 'Saved – leave blank to keep')
    expect(screen.getByLabelText('Password')).toHaveAttribute('type', 'password')
    expect(screen.getByLabelText('Domain (optional)')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /remove connection/i })).toBeInTheDocument()
  })

  it('hides host and port for a mounted path', () => {
    renderForm({ ...baseConfig, protocol: 'local', share: '/mnt/nas' })
    expect(screen.queryByLabelText('Host')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Port')).not.toBeInTheDocument()
    expect(screen.getByLabelText('Mounted path')).toHaveValue('/mnt/nas')
    expect(screen.queryByText('Authentication')).not.toBeInTheDocument()
  })

  it('does not render dataset storage in nas config form', () => {
    renderForm(baseConfig)
    expect(screen.queryByText('Dataset Storage')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Storage folder')).not.toBeInTheDocument()
  })

  it('offers TLS options for FTP only when TLS is on', async () => {
    renderForm({ ...baseConfig, protocol: 'ftp', host: 'ftp.local' })
    expect(screen.queryByLabelText(/verify the server certificate/i)).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('checkbox', { name: /use ftps/i }))
    expect(screen.getByRole('checkbox', { name: /verify the server certificate/i })).toBeInTheDocument()
  })

  it('blocks submit with a validation error when the host is missing', async () => {
    renderForm(baseConfig)
    await userEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(await screen.findByText('Enter the server address')).toBeInTheDocument()
  })
})
