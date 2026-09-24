import { NavLink } from 'react-router-dom'
import { LayoutDashboardIcon, ListChecksIcon, KeyIcon, HardDriveIcon, PlusIcon } from 'lucide-react'

import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'

const navItems = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboardIcon, end: true },
  { to: '/jobs', label: 'Jobs', icon: ListChecksIcon, end: false },
  { to: '/credentials', label: 'Credentials', icon: KeyIcon, end: false },
  { to: '/nas', label: 'NAS', icon: HardDriveIcon, end: false },
]

export function Sidebar() {
  return (
    <aside className="flex h-full w-56 shrink-0 flex-col gap-4 border-r bg-card px-3 py-4">
      <div className="px-2 text-sm font-semibold text-foreground">Data Automation</div>

      <Button asChild size="sm" className="justify-start gap-2">
        <NavLink to="/jobs/new">
          <PlusIcon /> New job
        </NavLink>
      </Button>

      <nav className="flex flex-col gap-1">
        {navItems.map(({ to, label, icon: Icon, end }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            className={({ isActive }) =>
              cn(
                'flex items-center gap-2 rounded-md px-2 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground',
                isActive && 'bg-accent text-accent-foreground',
              )
            }
          >
            <Icon className="size-4" />
            {label}
          </NavLink>
        ))}
      </nav>
    </aside>
  )
}
