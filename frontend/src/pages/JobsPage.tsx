import { useState } from 'react'
import { Link } from 'react-router-dom'

import { Topbar } from '@/components/layout/Topbar'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { JobList } from '@/components/jobs/JobList'
import { useJobs } from '@/hooks/useJobs'
import { jobStatusLabels, sourceTypeLabels } from '@/lib/enumLabels'
import type { JobStatus, SourceType } from '@/api/types'

const ALL = '__all__'

export function JobsPage() {
  const [status, setStatus] = useState<JobStatus | typeof ALL>(ALL)
  const [source, setSource] = useState<SourceType | typeof ALL>(ALL)

  const { data: jobs, isLoading } = useJobs({
    status: status === ALL ? undefined : status,
    source: source === ALL ? undefined : source,
    limit: 100,
  })

  return (
    <>
      <Topbar
        title="Jobs"
        right={
          <Button asChild size="sm">
            <Link to="/jobs/new">New job</Link>
          </Button>
        }
      />

      <div className="flex flex-col gap-4 p-6">
        <div className="flex gap-3">
          <Select value={status} onValueChange={(v) => setStatus(v as JobStatus | typeof ALL)}>
            <SelectTrigger className="w-48">
              <SelectValue placeholder="All statuses" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>All statuses</SelectItem>
              {Object.entries(jobStatusLabels).map(([value, label]) => (
                <SelectItem key={value} value={value}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={source} onValueChange={(v) => setSource(v as SourceType | typeof ALL)}>
            <SelectTrigger className="w-48">
              <SelectValue placeholder="All sources" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>All sources</SelectItem>
              {Object.entries(sourceTypeLabels).map(([value, label]) => (
                <SelectItem key={value} value={value}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <Card>
          <CardContent className="px-0">
            <JobList jobs={jobs} isLoading={isLoading} />
          </CardContent>
        </Card>
      </div>
    </>
  )
}
