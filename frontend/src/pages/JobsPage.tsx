import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'

import { Topbar } from '@/components/layout/Topbar'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { SearchInput } from '@/components/ui/search-input'
import { Pagination } from '@/components/ui/pagination'
import { JobList } from '@/components/jobs/JobList'
import { useJobs } from '@/hooks/useJobs'
import { useDebouncedValue } from '@/hooks/useDebouncedValue'
import { jobStatusLabels, sourceTypeLabels } from '@/lib/enumLabels'
import type { JobStatus, SourceType } from '@/api/types'

const ALL = '__all__'
const PAGE_SIZE = 20

export function JobsPage() {
  const [status, setStatus] = useState<JobStatus | typeof ALL>(ALL)
  const [source, setSource] = useState<SourceType | typeof ALL>(ALL)
  const [searchInput, setSearchInput] = useState('')
  const [offset, setOffset] = useState(0)
  const search = useDebouncedValue(searchInput)

  // Any filter/search change invalidates the current page — jump back to the
  // first page rather than showing an empty "page 3 of 1" result set.
  useEffect(() => {
    setOffset(0)
  }, [status, source, search])

  const { data, isLoading, error, refetch } = useJobs({
    status: status === ALL ? undefined : status,
    source: source === ALL ? undefined : source,
    search: search || undefined,
    limit: PAGE_SIZE,
    offset,
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
        <div className="flex flex-wrap gap-3">
          <SearchInput
            value={searchInput}
            onChange={setSearchInput}
            placeholder="Search by name or query…"
            className="w-72"
          />

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
            <JobList jobs={data?.items} isLoading={isLoading} error={error} onRetry={() => refetch()} />
            {data && (
              <Pagination offset={offset} limit={PAGE_SIZE} total={data.total} onOffsetChange={setOffset} />
            )}
          </CardContent>
        </Card>
      </div>
    </>
  )
}
