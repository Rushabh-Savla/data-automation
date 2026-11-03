import { Outlet, useLocation } from 'react-router-dom'

import { Sidebar } from '@/components/layout/Sidebar'
import { ErrorBoundary } from '@/components/layout/ErrorBoundary'

export function AppShell() {
  const location = useLocation()

  return (
    <div className="flex h-full w-full overflow-hidden">
      <Sidebar />
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <div className="flex-1 overflow-y-auto">
          {/* Keyed by path so navigating away from a broken route resets
              the boundary instead of staying stuck on the fallback UI. */}
          <ErrorBoundary key={location.pathname}>
            <Outlet />
          </ErrorBoundary>
        </div>
      </div>
    </div>
  )
}
