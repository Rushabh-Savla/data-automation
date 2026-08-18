import { Topbar } from '@/components/layout/Topbar'
import { JobCreateForm } from '@/components/jobs/JobCreateForm'

export function JobCreatePage() {
  return (
    <>
      <Topbar title="New job" />
      <JobCreateForm />
    </>
  )
}
