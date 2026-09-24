import { createBrowserRouter } from 'react-router-dom'

import { AppShell } from '@/components/layout/AppShell'
import { DashboardPage } from '@/pages/DashboardPage'
import { JobsPage } from '@/pages/JobsPage'
import { JobCreatePage } from '@/pages/JobCreatePage'
import { JobDetailPage } from '@/pages/JobDetailPage'
import { JobAssetsPage } from '@/pages/JobAssetsPage'
import { CredentialsPage } from '@/pages/CredentialsPage'
import { NasPage } from '@/pages/NasPage'
import { NotFoundPage } from '@/pages/NotFoundPage'

export const router = createBrowserRouter([
  {
    path: '/',
    element: <AppShell />,
    children: [
      { index: true, element: <DashboardPage /> },
      { path: 'jobs', element: <JobsPage /> },
      { path: 'jobs/new', element: <JobCreatePage /> },
      { path: 'jobs/:jobId', element: <JobDetailPage /> },
      { path: 'jobs/:jobId/assets', element: <JobAssetsPage /> },
      { path: 'credentials', element: <CredentialsPage /> },
      { path: 'nas', element: <NasPage /> },
      { path: '*', element: <NotFoundPage /> },
    ],
  },
])
