import { Link } from 'react-router-dom'
import { AlertCircle, RotateCcw, ShieldAlert, WifiOff } from 'lucide-react'

import { Topbar } from '@/components/layout/Topbar'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { JobList } from '@/components/jobs/JobList'
import { useStats } from '@/hooks/useStats'
import { useJobs } from '@/hooks/useJobs'
import { ApiError } from '@/api/client'
import { apiErrorMessage } from '@/lib/apiErrorMessage'

export function DashboardPage() {
  const { data: stats, isLoading: statsLoading, error: statsError, refetch: refetchStats } = useStats()
  const { data: jobsPage, isLoading: jobsLoading, error: jobsError, refetch: refetchJobs } = useJobs({ limit: 8 })

  const error = statsError || jobsError
  const isAuthError = error instanceof ApiError && error.status === 401
  const isNetworkError =
    error instanceof ApiError
      ? error.status >= 502 && error.status <= 504
      : error instanceof TypeError && error.message.includes('fetch')

  const handleRetryAll = () => {
    refetchStats()
    refetchJobs()
  }

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
        {/* Error notification banner for auth, network, or server failures */}
        {error && (
          <div className="flex flex-col gap-2 rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-destructive sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-3">
              {isAuthError ? (
                <ShieldAlert className="h-5 w-5 shrink-0" />
              ) : isNetworkError ? (
                <WifiOff className="h-5 w-5 shrink-0" />
              ) : (
                <AlertCircle className="h-5 w-5 shrink-0" />
              )}
              <div className="text-sm">
                <span className="font-semibold">
                  {isAuthError
                    ? 'Authentication Required'
                    : isNetworkError
                    ? 'Backend Service Unavailable'
                    : 'API Error'}
                  :{' '}
                </span>
                <span>
                  {isAuthError
                    ? 'API key is missing or invalid. Check credentials or authentication settings.'
                    : isNetworkError
                    ? 'Could not connect to the Data Automation backend. Ensure the API container is running.'
                    : apiErrorMessage(error, 'An unexpected error occurred while fetching dashboard data.')}
                </span>
              </div>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={handleRetryAll}
              className="mt-2 shrink-0 border-destructive/30 hover:bg-destructive/20 sm:mt-0"
            >
              <RotateCcw className="mr-2 h-4 w-4" />
              Retry
            </Button>
          </div>
        )}

        {/* 3 Metric Cards */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          <StatCard
            label="Active jobs"
            value={stats?.active_jobs}
            loading={statsLoading}
            error={statsError}
          />
          <StatCard
            label="Total jobs"
            value={stats?.total_jobs}
            loading={statsLoading}
            error={statsError}
          />
          <StatCard
            label="Total assets"
            value={stats?.total_assets}
            loading={statsLoading}
            error={statsError}
          />
        </div>

        {/* Recent Jobs Card */}
        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle>Recent jobs</CardTitle>
            <Button asChild variant="link" size="sm">
              <Link to="/jobs">View all</Link>
            </Button>
          </CardHeader>
          <CardContent className="px-0">
            <JobList
              jobs={jobsPage?.items}
              isLoading={jobsLoading}
              error={jobsError}
              onRetry={() => refetchJobs()}
            />
          </CardContent>
        </Card>
      </div>
    </>
  )
}

function StatCard({
  label,
  value,
  loading,
  error,
}: {
  label: string
  value: number | undefined
  loading: boolean
  error?: unknown
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium text-muted-foreground">{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <span className="text-2xl font-semibold">
          {loading ? (
            <span className="text-muted-foreground animate-pulse text-lg">Loading...</span>
          ) : error ? (
            <span className="text-destructive text-lg font-normal">Unavailable</span>
          ) : (
            value ?? 0
          )}
        </span>
      </CardContent>
    </Card>
  )
}
