import type { CredentialResponse } from '@/api/types'
import { Table, TableBody, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Skeleton } from '@/components/ui/skeleton'
import { CredentialListItem } from '@/components/credentials/CredentialListItem'

export function CredentialList({
  credentials,
  isLoading,
}: {
  credentials: CredentialResponse[] | undefined
  isLoading: boolean
}) {
  if (isLoading) {
    return (
      <div className="flex flex-col gap-2 p-4">
        {Array.from({ length: 3 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    )
  }

  if (!credentials || credentials.length === 0) {
    return (
      <div className="flex flex-col items-center gap-1 p-12 text-center text-muted-foreground">
        <p className="font-medium">No credentials configured</p>
        <p className="text-sm">Sources without a credential still run — Roboflow's public search and web
          scraping need none, and others simply return fewer results.</p>
      </div>
    )
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Source</TableHead>
          <TableHead>Label</TableHead>
          <TableHead>Key name</TableHead>
          <TableHead>Value</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Last used</TableHead>
          <TableHead />
        </TableRow>
      </TableHeader>
      <TableBody>
        {credentials.map((credential) => (
          <CredentialListItem key={credential.id} credential={credential} />
        ))}
      </TableBody>
    </Table>
  )
}
