import '@testing-library/jest-dom/vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getNasConfig } from '@/api/nas'
import type { NasConfig } from '@/api/types'
import { NasConnectionCard } from '@/components/credentials/NasConnectionCard'

vi.mock('@/api/nas', () => ({
  getNasConfig: vi.fn(),
  saveNasConfig: vi.fn(),
  deleteNasConfig: vi.fn(),
  testNasConnection: vi.fn(),
}))

const unconfigured: NasConfig = {
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

function renderCard() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <NasConnectionCard />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

describe('NasConnectionCard', () => {
  beforeEach(() => vi.mocked(getNasConfig).mockReset())

  it('invites the user to connect when no NAS is configured, linking to the NAS page', async () => {
    vi.mocked(getNasConfig).mockResolvedValue(unconfigured)
    renderCard()
    expect(await screen.findByText('Not configured')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Connect NAS' })).toHaveAttribute('href', '/nas')
  })

  it('summarises a password-protected NAS without any secret', async () => {
    vi.mocked(getNasConfig).mockResolvedValue({
      ...unconfigured,
      configured: true,
      host: '192.168.1.20',
      share: 'datasets',
      auth_mode: 'password',
      username: 'alice',
      has_password: true,
    })
    renderCard()
    expect(await screen.findByText('Configured')).toBeInTheDocument()
    expect(screen.getByText(/SMB \/ CIFS · 192\.168\.1\.20 \/ datasets · signed in as alice · filtered images are stored here/)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Edit connection' })).toHaveAttribute('href', '/nas')
  })

  it('describes an open NAS that is only used as a source', async () => {
    vi.mocked(getNasConfig).mockResolvedValue({
      ...unconfigured,
      configured: true,
      protocol: 'local',
      share: '/mnt/nas',
      use_as_storage: false,
    })
    renderCard()
    expect(await screen.findByText(/Mounted path · \/mnt\/nas · open, no password · not used to store images/)).toBeInTheDocument()
  })
})
