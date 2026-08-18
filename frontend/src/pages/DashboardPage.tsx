import { Link } from 'react-router-dom'

import { Topbar } from '@/components/layout/Topbar'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { JobList } from '@/components/jobs/JobList'
import { useStats } from '@/hooks/useStats'
import { useJobs } from '@/hooks/useJobs'

export function DashboardPage() {
  const { data: stats, isLoading: statsLoading } = useStats()
  const { data: jobsPage, isLoading: jobsLoading } = useJobs({ limit: 8 })

  return (
    <>
      <Topbar
        title="Dashboard"
        right={
          <Button asChild size="sm">
            <Link to="/jobs/new">New job</Link>
          </Button>
        }
      />

      <div className="flex flex-col gap-6 p-6">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          <StatCard label="Active jobs" value={stats?.active_jobs} loading={statsLoading} />
          <StatCard label="Total jobs" value={stats?.total_jobs} loading={statsLoading} />
          <StatCard label="Total assets" value={stats?.total_assets} loading={statsLoading} />
        </div>

        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle>Recent jobs</CardTitle>
            <Button asChild variant="link" size="sm">
              <Link to="/jobs">View all</Link>
            </Button>
          </CardHeader>
          <CardContent className="px-0">
            <JobList jobs={jobsPage?.items} isLoading={jobsLoading} />
          </CardContent>
        </Card>
      </div>
    </>
  )
}

function StatCard({ label, value, loading }: { label: string; value: number | undefined; loading: boolean }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium text-muted-foreground">{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <span className="text-2xl font-semibold">{loading ? '—' : (value ?? 0)}</span>
      </CardContent>
    </Card>
  )
}
