import '@testing-library/jest-dom/vitest'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { SourceInfo } from '@/api/types'
import { SourcePicker } from '@/components/jobs/SourcePicker'

const sources: SourceInfo[] = [
  { source_type: 'coco', connector_implemented: true, has_credential: false },
  { source_type: 'web', connector_implemented: true, has_credential: false },
  { source_type: 'nas_share', connector_implemented: true, has_credential: true },
]

vi.mock('@/hooks/useSources', () => ({
  useSources: () => ({ data: sources, isLoading: false }),
}))

describe('SourcePicker', () => {
  it('lists the public sources but never the NAS share', () => {
    render(
      <SourcePicker useAuto={false} onUseAutoChange={() => {}} selected={[]} onSelectedChange={() => {}} />,
    )
    expect(screen.getByRole('button', { name: /COCO/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Web/i })).toBeInTheDocument()
    expect(screen.queryByText('NAS Share')).not.toBeInTheDocument()
  })
})
