import type { ReactNode } from 'react'

export function Topbar({ title, right }: { title: string; right?: ReactNode }) {
  return (
    <header className="sticky top-0 z-10 flex h-14 shrink-0 items-center justify-between border-b bg-card/95 px-6 backdrop-blur-sm">
      <h1 className="text-base font-semibold">{title}</h1>
      {right && <div className="flex items-center gap-3">{right}</div>}
    </header>
  )
}
