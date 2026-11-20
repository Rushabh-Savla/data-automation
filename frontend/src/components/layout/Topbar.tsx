import type { ReactNode } from 'react'

import { NasStatusPill } from '@/components/nas/NasStatusPill'

export function Topbar({ title, right }: { title: string; right?: ReactNode }) {
  return (
    <header className="flex h-14 shrink-0 items-center justify-between border-b bg-card px-6">
      <h1 className="text-base font-semibold">{title}</h1>
      <div className="flex items-center gap-3">
        {right}
        <NasStatusPill />
      </div>
    </header>
  )
}
