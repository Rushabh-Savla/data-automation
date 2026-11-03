import '@testing-library/jest-dom/vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi, beforeEach } from 'vitest'

import type { NasConfig, NasTestResult } from '@/api/types'
import { testNasConnection } from '@/api/nas'
import { NasTestConnectionTab } from '@/components/nas/NasTestConnectionTab'

vi.mock('@/api/nas', () => ({
  getNasConfig: vi.fn(),
  saveNasConfig: vi.fn(),
  deleteNasConfig: vi.fn(),
  testNasConnection: vi.fn(),
}))

const configuredConfig: NasConfig = {
  configured: true,
  protocol: 'smb',
  host: '192.168.1.50',
  port: 445,
  share: 'datasets',
  base_path: 'raw_images',
  storage_path: 'filtered_images',
  use_as_storage: true,
  auth_mode: 'password',
  username: 'admin',
  domain: 'WORKGROUP',
  has_password: true,
  use_tls: false,
  verify_tls: true,
  updated_at: '2026-11-02T00:00:00Z',
}

function renderWithClient(ui: React.ReactElement) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

describe('NasTestConnectionTab', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('displays "NAS connected" in green when real-time test succeeds', async () => {
    const successResult: NasTestResult = {
      ok: true,
      can_read: true,
      can_write: true,
      message: 'Connected. The NAS can be read and written.',
      latency_ms: 38,
      sample_files: ['sample1.jpg', 'sample2.png'],
    }
    vi.mocked(testNasConnection).mockResolvedValueOnce(successResult)

    renderWithClient(<NasTestConnectionTab config={configuredConfig} />)

    // Verify it sent only one request
    await waitFor(() => {
      expect(testNasConnection).toHaveBeenCalledTimes(1)
    })

    // Expect green NAS connected status and metrics
    expect(await screen.findByText('NAS connected')).toBeInTheDocument()
    expect(screen.getByText('Online')).toBeInTheDocument()
    expect(screen.getByText('38 ms')).toBeInTheDocument()
    expect(screen.getByText('Verified (Can read)')).toBeInTheDocument()
    expect(screen.getByText('Verified (Can write)')).toBeInTheDocument()
    expect(screen.getByText('• sample1.jpg')).toBeInTheDocument()
  })

  it('displays "NAS disconnected" in red when connection fails', async () => {
    const failResult: NasTestResult = {
      ok: false,
      can_read: false,
      can_write: false,
      message: 'Connection timed out after 15 seconds',
      latency_ms: null,
      sample_files: [],
    }
    vi.mocked(testNasConnection).mockResolvedValueOnce(failResult)

    renderWithClient(<NasTestConnectionTab config={configuredConfig} />)

    await waitFor(() => {
      expect(testNasConnection).toHaveBeenCalledTimes(1)
    })

    expect(await screen.findByText('NAS disconnected')).toBeInTheDocument()
    expect(screen.getByText('Offline')).toBeInTheDocument()
    expect(screen.getByText('Connection timed out after 15 seconds')).toBeInTheDocument()
  })
})

